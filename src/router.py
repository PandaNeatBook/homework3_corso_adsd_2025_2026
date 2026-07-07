"""
Router: unico gateway del sistema per client e amministratori.

Combina due contratti distinti:

1. Lo SHARDING/REBALANCING:
   calcolo dello shard responsabile di una chiave, generazione di un Sequence
   Number globale per ogni scrittura, fallback in lettura sulla topologia
   vecchia durante il rebalance, blocco delle CAS durante il rebalance,
   conversione del DELETE in scrittura di un tombstone.

2. L'IDEMPOTENZA dei retry via request_id: tabella delle
   richieste per (client_id, seq), sliding window con eviction O(1),
   locking sul client per l'intera transazione, qui riusata per decidere SE eseguire l'effetto,
   che pero' ora consiste nello spedire comandi agli ShardNode invece che
   mutare un dizionario locale.

Scostamenti dichiarati rispetto alle due specifiche originali, necessari per
farle combaciare (vedi anche README.md):

* Le operazioni mutative pubbliche sono SET_REQ / CAS_REQ / DELETE_REQ (con
  request_id), non i semplici SET / CAS / DELETE della specifica di
  rebalancing: e' l'estensione richiesta dall'Homework 3.
* I nomi degli errori applicativi seguono la convenzione ERR_XXX della
  specifica di rebalancing (es. ERR_NOT_FOUND, ERR_CAS_CONFLICT) anche per
  gli errori di request_id, che nell'Homework 3 originale erano minuscoli
  (es. request_id_expired -> qui ERR_REQUEST_ID_EXPIRED), per uniformita'
  dell'intero protocollo del Router.
* GET/GETV seguono il formato "<valore> <versione>" / "<versione>" della
  specifica di rebalancing (senza prefisso "OK"), diverso dal formato
  "OK version=<n> <value>" usato nell'Homework 3 originale.
* Una lettura che incontra un tombstone e' trattata come ERR_NOT_FOUND, e in
  quel caso NON si fa fallback sulla topologia vecchia: il tombstone e' gia'
  l'informazione piu' aggiornata. Fare altrimenti resusciterebbe un dato
  cancellato leggendolo dalla topologia N invece che dalla N+1 - esattamente
  il problema di "dati zombie" che il contratto vuole evitare.
* Le risposte ERR_REBALANCING per le CAS_REQ NON vengono salvate nella
  request table: sono un esito transitorio legato allo stato del sistema, non
  all'operazione in se'. Se le salvassimo, un client che ritenta lo stesso
  request_id dopo la fine del rebalance riceverebbe per sempre
  ERR_REBALANCING anche a rebalance concluso da tempo, violando la liveness
  ("un client corretto deve poter completare").
* Per compatibilita' con la semantica ERR_NOT_FOUND dell'Homework 3, la
  DELETE_REQ controlla prima (rispettando il fallback di lettura) se la
  chiave esiste davvero, e solo in quel caso scrive il tombstone.
"""
from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass

from protocol_common import (
    LineTCPServer,
    ShardRef,
    TOMBSTONE,
    encode_topology,
    send_line,
    shard_for_key,
)


@dataclass
class RequestRecord:
    payload: str
    response: str


class Router:
    def __init__(
        self,
        coordinator_host: str,
        coordinator_port: int,
        request_window_size: int = 100,
    ) -> None:
        # --- Topologia degli shard ---
        # _topology_new != None  <=>  e' in corso (o e' stato accettato) un rebalance:
        #   usato per bloccare nuovi ADD_SHARD/REMOVE_SHARD/REBALANCE.
        # _rebalancing = True    <=>  il Coordinator ha confermato ACK_REBALANCE_START:
        #   usato per decidere fallback di lettura / instradamento delle scritture / blocco CAS.
        # Le due condizioni non coincidono temporalmente: c'e' una breve finestra tra
        # l'accettazione della REBALANCE e la conferma del Coordinator.
        self._topology_lock = threading.Lock()
        self._active_topology: list[ShardRef] = []
        self._pending_adds: dict[str, ShardRef] = {}
        self._pending_removes: set[str] = set()
        self._rebalancing = False
        self._topology_new: list[ShardRef] | None = None

        # --- Watchdog Rebalance ---
        self._watchdog_active = False
        self._watchdog_interval = 2.0  # Secondi tra un PING e l'altro
        self._watchdog_max_failures = 3  # Quanti PING falliti prima di dichiarare la morte

        # --- Sequence number globale (versioni delle scritture) ---
        self._version_lock = threading.Lock()
        self._global_version = -1  # la prima scrittura in assoluto produce version=0

        # --- Idempotenza (stessa struttura logica di server.py / Homework 3) ---
        self._meta_lock = threading.Lock()
        self._client_locks: dict[str, threading.Lock] = {}
        self._requests: dict[str, dict[int, RequestRecord]] = {}
        self._evicted_until: dict[str, int] = {}
        self._window = request_window_size

        # --- Coordinator ---
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port

    # ------------------------------------------------------------------
    # Lock helper
    # ------------------------------------------------------------------
    def _get_client_lock(self, client_id: str) -> threading.Lock:
        with self._meta_lock:
            if client_id not in self._client_locks:
                self._client_locks[client_id] = threading.Lock()
            return self._client_locks[client_id]

    def _next_version(self) -> int:
        with self._version_lock:
            self._global_version += 1
            return self._global_version

    def _routing_snapshot(self) -> tuple[bool, list[ShardRef], list[ShardRef] | None]:
        """Fotografia coerente e breve dello stato di routing, senza tenere il lock durante l'I/O di rete."""
        with self._topology_lock:
            return (
                self._rebalancing,
                list(self._active_topology),
                list(self._topology_new) if self._topology_new is not None else None,
            )

    # ------------------------------------------------------------------
    # Data plane helpers (Router -> ShardNode)
    # ------------------------------------------------------------------
    @staticmethod
    def _shard_get(shard: ShardRef, key: str) -> tuple[bool, str, int]:
        """Ritorna (trovato, valore, versione). trovato=False se lo shard non ha la chiave."""
        response = send_line(shard.host, shard.port, f"SHARD_GET {key}")
        if response == "ERR_NOT_FOUND":
            return False, "", -1
        if not response.startswith("OK "):
            raise RuntimeError(f"risposta inattesa da SHARD_GET: {response}")
        rest = response[len("OK "):]
        version_str, _, value = rest.partition(" ")
        return True, value, int(version_str)

    @staticmethod
    def _shard_set(shard: ShardRef, key: str, version: int, value: str) -> None:
        send_line(shard.host, shard.port, f"SHARD_SET {key} {version} {value}")

    @staticmethod
    def _shard_get_all(shard: ShardRef) -> dict[str, tuple[str, int]]:
        response = send_line(shard.host, shard.port, "SHARD_GET_ALL")
        raw_json = response[len("OK "):]
        data = json.loads(raw_json)
        return {k: (v[0], v[1]) for k, v in data.items()}

    def _read_with_fallback(self, key: str) -> tuple[bool, str, int]:
        """
        Legge una chiave rispettando le regole di fallback durante il rebalance.
        Ritorna (trovato_e_non_tombstone, valore, versione).
        """
        rebalancing, active, new_topology = self._routing_snapshot()

        if rebalancing and new_topology:
            target_new = shard_for_key(key, new_topology)
            found, value, version = self._shard_get(target_new, key)
            if found:
                # Presente sulla topologia nuova: e' l'informazione autoritativa,
                # NON si fa fallback (evita di resuscitare un dato cancellato).
                if value == TOMBSTONE:
                    return False, "", -1
                return True, value, version
            target_old = shard_for_key(key, active)
            found_old, value_old, version_old = self._shard_get(target_old, key)
            if found_old and value_old != TOMBSTONE:
                return True, value_old, version_old
            return False, "", -1

        target = shard_for_key(key, active)
        found, value, version = self._shard_get(target, key)
        if found and value != TOMBSTONE:
            return True, value, version
        return False, "", -1

    def _target_for_write(self, key: str) -> ShardRef:
        """Durante il rebalance le scritture vanno SOLO sulla topologia nuova (N+1)."""
        rebalancing, active, new_topology = self._routing_snapshot()
        topology = new_topology if (rebalancing and new_topology) else active
        return shard_for_key(key, topology)

    # ------------------------------------------------------------------
    # Dispatcher pubblico (client)
    # ------------------------------------------------------------------
    def handle_line(self, line: str) -> str:
        stripped = line.strip()
        if not stripped:
            return "ERR empty_command"

        parts = stripped.split(maxsplit=1)
        cmd = parts[0].upper()
        args_str = parts[1] if len(parts) > 1 else ""

        if cmd == "PING":
            return "ERR usage: PING" if args_str else "OK PONG"

        if cmd == "GET":
            return self._handle_get(args_str)

        if cmd == "GETV":
            return self._handle_getv(args_str)

        if cmd == "KEYS":
            return "ERR usage: KEYS" if args_str else self._handle_keys()

        if cmd == "STATS":
            return self._handle_stats(args_str)

        if cmd == "QUIT":
            return "OK BYE"

        if cmd == "ADD_SHARD":
            return self._handle_add_shard(args_str)

        if cmd == "REMOVE_SHARD":
            return self._handle_remove_shard(args_str)

        if cmd == "REBALANCE":
            return self._handle_rebalance(args_str)

        if cmd in ("SET_REQ", "CAS_REQ", "DELETE_REQ"):
            return self._handle_mutation(cmd, args_str)

        return "ERR unknown_command"

    # ------------------------------------------------------------------
    # Letture
    # ------------------------------------------------------------------
    def _handle_get(self, args_str: str) -> str:
        key = args_str.strip()
        if not key or len(key.split()) != 1:
            return "ERR usage: GET <key>"
        try:
            found, value, version = self._read_with_fallback(key)
        except OSError as exc:
            return f"ERR shard_unreachable: {exc}"
        if not found:
            return "ERR_NOT_FOUND"
        return f"{value} {version}"

    def _handle_getv(self, args_str: str) -> str:
        key = args_str.strip()
        if not key or len(key.split()) != 1:
            return "ERR usage: GETV <key>"
        try:
            found, _value, version = self._read_with_fallback(key)
        except OSError as exc:
            return f"ERR shard_unreachable: {exc}"
        if not found:
            return "ERR_NOT_FOUND"
        return f"{version}"

    def _handle_keys(self) -> str:
        rebalancing, active, new_topology = self._routing_snapshot()
        try:
            combined: dict[str, tuple[str, int]] = {}
            for shard in active:
                combined.update(self._shard_get_all(shard))
            if rebalancing and new_topology:
                for shard in new_topology:
                    # I dati sulla topologia nuova sono piu' aggiornati: sovrascrivono.
                    combined.update(self._shard_get_all(shard))
        except OSError as exc:
            return f"ERR shard_unreachable: {exc}"

        keys = sorted(k for k, (value, _v) in combined.items() if value != TOMBSTONE)
        return "OK " + " ".join(keys) if keys else "OK"

    def _handle_stats(self, args_str: str) -> str:
        if args_str:
            return "ERR usage: STATS"
        rebalancing, active, new_topology = self._routing_snapshot()
        with self._meta_lock:
            clients_count = len(self._requests)
            cached_requests_count = sum(len(v) for v in self._requests.values())
        return (
            f"OK shards={len(active)} rebalancing={int(rebalancing)} "
            f"new_shards={len(new_topology) if new_topology else 0} "
            f"clients={clients_count} cached_requests={cached_requests_count} "
            f"window_size={self._window}"
        )

    # ------------------------------------------------------------------
    # Amministrazione
    # ------------------------------------------------------------------
    def _rebalance_watchdog(self) -> None:
        """Monitora la salute del Coordinator durante un rebalance."""
        failed_attempts = 0

        while self._watchdog_active:
            time.sleep(self._watchdog_interval)

            # Controllo se il rebalance si è concluso in modo pulito mentre dormivamo
            if not self._watchdog_active:
                break

            try:
                # Il control plane del Coordinator risponde a PING
                response = send_line(
                    self.coordinator_host,
                    self.coordinator_port,
                    "PING",
                    timeout=1.0
                )
                if response == "OK PONG":
                    failed_attempts = 0  # Reset del contatore
                else:
                    failed_attempts += 1
            except OSError:
                failed_attempts += 1

            if failed_attempts >= self._watchdog_max_failures:
                print(
                    f"[router] ATTENZIONE: Coordinator irraggiungibile ({failed_attempts} fallimenti). Abortisco il rebalance.")
                self._abort_rebalance()
                break

    def _abort_rebalance(self) -> None:
        """Annulla lo stato di rebalancing in corso a causa di un guasto."""
        with self._topology_lock:
            self._rebalancing = False
            self._topology_new = None
            self._watchdog_active = False
            # Nota: self._pending_adds e self._pending_removes vengono mantenuti.
            # L'amministratore (o un nuovo Coordinator) potrà ritentare il REBALANCE.

    def _handle_add_shard(self, args_str: str) -> str:
        tokens = args_str.split()
        if len(tokens) != 2:
            return "ERR usage: ADD_SHARD <id> <host:port>"
        shard_id, addr = tokens
        try:
            host, port_str = addr.rsplit(":", 1)
            port = int(port_str)
        except ValueError:
            return "ERR usage: ADD_SHARD <id> <host:port>"

        with self._topology_lock:
            if self._topology_new is not None:
                return "ERR rebalance_in_progress"
            already_active = any(s.shard_id == shard_id for s in self._active_topology)
            if already_active or shard_id in self._pending_adds:
                return "ERR shard_already_present"
            self._pending_adds[shard_id] = ShardRef(shard_id, host, port)
            self._pending_removes.discard(shard_id)
        return "OK"

    def _handle_remove_shard(self, args_str: str) -> str:
        tokens = args_str.split()
        if len(tokens) != 2:
            return "ERR usage: REMOVE_SHARD <id> <host:port>"
        shard_id, _addr = tokens

        with self._topology_lock:
            if self._topology_new is not None:
                return "ERR rebalance_in_progress"
            already_active = any(s.shard_id == shard_id for s in self._active_topology)
            if not already_active and shard_id not in self._pending_adds:
                return "ERR shard_not_found"
            self._pending_removes.add(shard_id)
            self._pending_adds.pop(shard_id, None)
        return "OK"

    def _handle_rebalance(self, args_str: str) -> str:
        if args_str:
            return "ERR usage: REBALANCE"

        with self._topology_lock:
            if self._topology_new is not None:
                return "ERR rebalance_in_progress"
            if not self._pending_adds and not self._pending_removes:
                return "ERR nothing_to_rebalance"

            new_topology = [
                s for s in self._active_topology if s.shard_id not in self._pending_removes
            ] + list(self._pending_adds.values())
            if not new_topology:
                return "ERR empty_topology"

            old_topology = list(self._active_topology)
            self._topology_new = new_topology

        try:
            response = send_line(
                self.coordinator_host,
                self.coordinator_port,
                f"START_REBALANCE {encode_topology(old_topology)} {encode_topology(new_topology)}",
            )
        except OSError as exc:
            with self._topology_lock:
                self._topology_new = None
            return f"ERR coordinator_unreachable: {exc}"

        if not response.startswith("OK"):
            with self._topology_lock:
                self._topology_new = None
            return f"ERR coordinator_rejected: {response}"

        return "OK rebalance_scheduled"

    # ------------------------------------------------------------------
    # Dispatcher control-plane (Coordinator -> Router)
    # ------------------------------------------------------------------
    def handle_control_line(self, line: str) -> str:
        stripped = line.strip()

        if stripped == "PING":
            return "OK PONG"

        if stripped == "ACK_REBALANCE_START":
            with self._topology_lock:
                if self._topology_new is None:
                    return "ERR no_rebalance_pending"
                self._rebalancing = True
                self._watchdog_active = True

            print("[router] rebalance confermato dal Coordinator: fallback in lettura abilitato e watchdog avviato")
            # Avvio il thread separato
            threading.Thread(target=self._rebalance_watchdog, daemon=True).start()
            return "OK"

        if stripped == "ACK_REBALANCE_END":
            with self._topology_lock:
                if self._topology_new is None:
                    return "ERR no_rebalance_pending"
                self._active_topology = self._topology_new
                self._topology_new = None
                self._rebalancing = False
                self._watchdog_active = False  # Termina gentilmente il thread di watchdog
                self._pending_adds = {}
                self._pending_removes = set()
                topology_snapshot = list(self._active_topology)

            print("[router] rebalance completato: nuova topologia attiva, avvio cleanup tombstone")
            self._cleanup_tombstones_async(topology_snapshot)
            return "OK"

        return "ERR unknown_control_command"

    def _cleanup_tombstones_async(self, topology: list[ShardRef]) -> None:
        def _run() -> None:
            for shard in topology:
                try:
                    send_line(shard.host, shard.port, "SHARD_CLEANUP_TOMBSTONES")
                except OSError as exc:
                    print(f"[router] cleanup tombstone fallito su {shard.shard_id}: {exc}")

        threading.Thread(target=_run, daemon=True).start()

    # ------------------------------------------------------------------
    # Operazioni mutative idempotenti (SET_REQ / CAS_REQ / DELETE_REQ)
    # ------------------------------------------------------------------
    def _handle_mutation(self, cmd: str, args_str: str) -> str:
        # ---- 1. Parsing sintattico (fuori da qualunque lock) ----
        if cmd == "SET_REQ":
            tokens = args_str.split(maxsplit=2)
            if len(tokens) < 3:
                return "ERR usage: SET_REQ <request_id> <key> <value...>"
            req_id, key, value = tokens[0], tokens[1], tokens[2]
            payload_canonico = f"SET_REQ {key} {value}"
            expected_version = None

        elif cmd == "CAS_REQ":
            tokens = args_str.split(maxsplit=3)
            if len(tokens) < 4:
                return "ERR usage: CAS_REQ <request_id> <key> <expected_version> <value...>"
            req_id, key, expected_version_str, value = tokens
            try:
                expected_version = int(expected_version_str)
                if expected_version < 0:
                    raise ValueError()
            except ValueError:
                return "ERR bad_version"
            payload_canonico = f"CAS_REQ {key} {expected_version} {value}"

        elif cmd == "DELETE_REQ":
            tokens = args_str.split()
            if len(tokens) != 2:
                return "ERR usage: DELETE_REQ <request_id> <key>"
            req_id, key = tokens
            value = None
            expected_version = None
            payload_canonico = f"DELETE_REQ {key}"

        else:
            return "ERR unknown_command"

        # ---- 2. Validazione del request_id ----
        if ":" not in req_id:
            return "ERR_INVALID_REQUEST_ID"
        client_id, seq_str = req_id.split(":", 1)
        if not client_id:
            return "ERR_INVALID_REQUEST_ID"
        try:
            seq = int(seq_str)
            if seq < 0:
                raise ValueError()
        except ValueError:
            return "ERR_INVALID_REQUEST_ID"

        # ---- 3. Transazione idempotente ----
        client_lock = self._get_client_lock(client_id)
        with client_lock:
            eviction_boundary = self._evicted_until.get(client_id, -1)
            if seq <= eviction_boundary:
                return "ERR_REQUEST_ID_EXPIRED"

            client_requests = self._requests.setdefault(client_id, {})
            if seq in client_requests:
                record = client_requests[seq]
                if record.payload != payload_canonico:
                    return "ERR_REQUEST_ID_CONFLICT"
                return record.response

            # ---- 4. Prima esecuzione: instradamento + effetto ----
            if cmd == "CAS_REQ":
                rebalancing_now, _active, _new = self._routing_snapshot()
                if rebalancing_now:
                    # Esito transitorio: NON viene salvato nella request table
                    # (vedi nota di design in testa al file).
                    return "ERR_REBALANCING"
                response = self._execute_cas(key, expected_version, value)
            elif cmd == "SET_REQ":
                response = self._execute_set(key, value)
            else:  # DELETE_REQ
                response = self._execute_delete(key)

            client_requests[seq] = RequestRecord(payload=payload_canonico, response=response)

            if len(client_requests) > self._window:
                min_seq = min(client_requests.keys())
                del client_requests[min_seq]
                self._evicted_until[client_id] = min_seq

            return response

    def _execute_set(self, key: str, value: str) -> str:
        target = self._target_for_write(key)
        version = self._next_version()
        try:
            self._shard_set(target, key, version, value)
        except OSError as exc:
            return f"ERR shard_unreachable: {exc}"
        return f"OK version={version}"

    def _execute_cas(self, key: str, expected_version: int, value: str) -> str:
        # La CAS e' bloccata durante il rebalance dal chiamante (_handle_mutation),
        # quindi qui la topologia attiva e' stabile e unica.
        _rebalancing, active, _new = self._routing_snapshot()
        target = shard_for_key(key, active)
        try:
            found, _current_value, current_version = self._shard_get(target, key)
        except OSError as exc:
            return f"ERR shard_unreachable: {exc}"

        if not found:
            return "ERR_NOT_FOUND"
        if current_version != expected_version:
            return f"ERR_CAS_CONFLICT current={current_version}"

        new_version = self._next_version()
        try:
            self._shard_set(target, key, new_version, value)
        except OSError as exc:
            return f"ERR shard_unreachable: {exc}"
        return f"OK version={new_version}"

    def _execute_delete(self, key: str) -> str:
        # Il DELETE e' realizzato come SET del valore sentinella TOMBSTONE con
        # un nuovo Sequence Number, come richiesto dal contratto di
        # rebalancing (evita la "resurrezione" di dati durante il fallback in
        # lettura). Per restare compatibili con la semantica NOT_FOUND
        # dell'Homework 3, controlliamo prima (rispettando il fallback) se la
        # chiave e' effettivamente presente, e solo in quel caso scriviamo il
        # tombstone.
        try:
            found, _value, _version = self._read_with_fallback(key)
        except OSError as exc:
            return f"ERR shard_unreachable: {exc}"

        if not found:
            return "ERR_NOT_FOUND"

        target = self._target_for_write(key)
        version = self._next_version()
        try:
            self._shard_set(target, key, version, TOMBSTONE)
        except OSError as exc:
            return f"ERR shard_unreachable: {exc}"
        return "OK"


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Avvia il Router standalone")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--public-port", type=int, required=True, help="porta per i client")
    parser.add_argument("--control-port", type=int, required=True, help="porta per il Coordinator")
    parser.add_argument("--coordinator-host", default="127.0.0.1")
    parser.add_argument("--coordinator-port", type=int, required=True)
    parser.add_argument("--window-size", type=int, default=100)
    args = parser.parse_args()

    router = Router(args.coordinator_host, args.coordinator_port, args.window_size)

    control_server = LineTCPServer(args.host, args.control_port, router.handle_control_line)
    threading.Thread(target=control_server.serve_forever, daemon=True).start()

    public_server = LineTCPServer(args.host, args.public_port, router.handle_line)
    print(f"Router: client su {args.host}:{args.public_port}, control-plane su {args.host}:{args.control_port}")
    public_server.serve_forever()


if __name__ == "__main__":
    main()