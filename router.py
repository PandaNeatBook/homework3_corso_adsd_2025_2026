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
    """Rappresenta una richiesta cachata per garantire l'idempotenza."""
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
        # _topology_lock protegge tutte le mutazioni alle strutture dati topologiche.
        self._topology_lock = threading.Lock()

        # _active_topology: La topologia stabile attuale su cui avvengono le operazioni.
        self._active_topology: list[ShardRef] = []

        # Strutture per la costruzione incrementale del prossimo rebalance:
        self._pending_adds: dict[str, ShardRef] = {}
        self._pending_removes: set[str] = set()

        # _rebalancing: Flag booleano. Se True, il Coordinator ha confermato l'avvio
        # del rebalance. Attiva il fallback in lettura e l'instradamento in scrittura.
        self._rebalancing = False

        # _topology_new: Lista degli shard target del rebalance. Se non None, significa
        # che un rebalance è stato richiesto (impedisce nuove richieste di rebalance sovrapposte).
        self._topology_new: list[ShardRef] | None = None

        # --- Watchdog Rebalance ---
        # Sistema di fault-tolerance per rilevare la caduta del Coordinator durante un rebalance.
        self._watchdog_active = False
        self._watchdog_interval = 2.0  # Frequenza dei PING (in secondi).
        self._watchdog_max_failures = 3  # Tolleranza ai PING falliti prima dell'abort.

        # --- Sequence number globale ---
        # Utilizzato per assegnare una versione monotonamente crescente a ogni scrittura.
        self._version_lock = threading.Lock()
        self._global_version = -1  # Inizializzato a -1 così la prima scrittura (+=1) avrà versione 0.

        # --- Idempotenza (Sliding Window) ---
        # _meta_lock protegge l'accesso al dizionario dei lock dei client e alle code delle richieste.
        self._meta_lock = threading.Lock()

        # Mappa {client_id: Lock}. Permette di serializzare le richieste di un SINGOLO client
        # senza bloccare globalmente l'intero router. Inizializzata lazy (su richiesta).
        self._client_locks: dict[str, threading.Lock] = {}

        # Tabella di caching: {client_id: {sequence_number: RequestRecord}}.
        # Memorizza le risposte già calcolate per evitare di ripetere gli effetti collaterali.
        self._requests: dict[str, dict[int, RequestRecord]] = {}

        # Track dell'ultimo sequence number espulso per ogni client: {client_id: last_evicted_seq}.
        # Se un client richiede un seq <= a questo valore, la richiesta è considerata scaduta.
        self._evicted_until: dict[str, int] = {}

        # Dimensione massima della finestra di idempotenza per client.
        self._window = request_window_size

        # --- Coordinator ---
        self.coordinator_host = coordinator_host
        self.coordinator_port = coordinator_port

    # ------------------------------------------------------------------
    # Lock helper
    # ------------------------------------------------------------------
    def _get_client_lock(self, client_id: str) -> threading.Lock:
        """
        Ritorna il lock specifico per un dato client, creandolo se non esiste.
        Questo garantisce che richieste multiple concorrenti dallo STESSO client
        siano eseguite in mutua esclusione, proteggendo la logica di idempotenza
        da race condition.
        """
        with self._meta_lock:
            if client_id not in self._client_locks:
                self._client_locks[client_id] = threading.Lock()
            return self._client_locks[client_id]

    def _next_version(self) -> int:
        """
        Genera e ritorna il prossimo sequence number globale in modo thread-safe.
        Usato per assegnare versioni alle scritture (SET/DELETE).
        """
        with self._version_lock:
            self._global_version += 1
            return self._global_version

    def _routing_snapshot(self) -> tuple[bool, list[ShardRef], list[ShardRef] | None]:
        """
        Restituisce una fotografia coerente dello stato di routing attuale.
        Questo metodo è cruciale: prelevando i dati in una volta sola all'interno del lock,
        permette alle operazioni successive di effettuare chiamate di rete bloccanti
        (I/O) SENZA tenere occupato il _topology_lock, migliorando drasticamente la
        concorrenza del Router.
        """
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
        """
        Esegue una richiesta SHARD_GET direttamente al nodo specificato.
        Ritorna una tupla: (Trovato?, Valore, Versione).
        Se lo shard risponde "ERR_NOT_FOUND", imposta Trovato=False.
        """
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
        """Esegue una scrittura (SHARD_SET) forzata della chiave e versione sul nodo target."""
        send_line(shard.host, shard.port, f"SHARD_SET {key} {version} {value}")

    @staticmethod
    def _shard_get_all(shard: ShardRef) -> dict[str, tuple[str, int]]:
        """Recupera l'intero dump di un singolo shard tramite SHARD_GET_ALL e parsa il JSON."""
        response = send_line(shard.host, shard.port, "SHARD_GET_ALL")
        raw_json = response[len("OK "):]
        data = json.loads(raw_json)
        return {k: (v[0], v[1]) for k, v in data.items()}

    def _read_with_fallback(self, key: str) -> tuple[bool, str, int]:
        """
        Implementa il meccanismo di lettura autoritativa durante un rebalance.

        Logica di fallback:
        1. Se c'è un rebalance in corso:
           - Cerca prima nella NUOVA topologia. Se la chiave viene trovata lì, questa
             è l'informazione più aggiornata.
           - Se il valore trovato nella nuova topologia è un TOMBSTONE, la chiave è
             stata logicamente cancellata di recente. Ritorna False IMMEDIATAMENTE
             per evitare di leggere dati obsoleti (zombie) dalla vecchia topologia.
           - Se non viene trovata nella nuova topologia, fa "fallback" cercando
             nella VECCHIA topologia attiva.
        2. Se non c'è rebalance, cerca semplicemente nella topologia attiva.

        Ritorna (trovato_e_non_tombstone, valore, versione).
        """
        rebalancing, active, new_topology = self._routing_snapshot()

        # Condizione 1: Rebalance in corso
        if rebalancing and new_topology:
            target_new = shard_for_key(key, new_topology)
            found, value, version = self._shard_get(target_new, key)

            if found:
                # Dato trovato nella nuova topologia (autoritativo).
                if value == TOMBSTONE:
                    return False, "", -1 # Trovato tombstone, chiave logicamente assente.
                return True, value, version

            # Non trovato nella nuova topologia, fallback sulla vecchia.
            target_old = shard_for_key(key, active)
            found_old, value_old, version_old = self._shard_get(target_old, key)
            if found_old and value_old != TOMBSTONE:
                return True, value_old, version_old
            return False, "", -1

        # Condizione 2: Stato stabile (Nessun rebalance)
        target = shard_for_key(key, active)
        found, value, version = self._shard_get(target, key)
        if found and value != TOMBSTONE:
            return True, value, version
        return False, "", -1

    def _target_for_write(self, key: str) -> ShardRef:
        """
        Determina lo shard di destinazione per una scrittura (SET/DELETE).
        Durante un rebalance confermato, TUTTE le scritture vengono indirizzate
        esclusivamente verso la NUOVA topologia per iniziare a popolarla in vista
        dello switch-over finale. Altrimenti, usa la topologia attiva.
        """
        rebalancing, active, new_topology = self._routing_snapshot()
        topology = new_topology if (rebalancing and new_topology) else active
        return shard_for_key(key, topology)

    # ------------------------------------------------------------------
    # Dispatcher pubblico (client)
    # ------------------------------------------------------------------
    def handle_line(self, line: str) -> str:
        """
        Punto d'ingresso principale per le connessioni client (Data Plane).
        Esegue il parsing base del comando (es. "GET <key>") e delega l'esecuzione
        all'handler specifico. Nessuna logica di business dovrebbe trovarsi qui.
        """
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
        """
        Gestisce la richiesta GET di un client, incapsulando il comando
        all'interno della logica di fallback di _read_with_fallback.
        Restituisce valore e versione se trovati, altrimenti ERR_NOT_FOUND.
        """
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
        """Come _handle_get, ma formatta la risposta per restituire solo la versione."""
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
        """
        Raccoglie le chiavi da tutti gli shard. Durante un rebalance, fa il merge
        della topologia attiva e di quella nuova. Poiché un dizionario aggiorna
        le chiavi duplicate, iterando prima la vecchia topologia e POI la nuova,
        i dati più aggiornati (quelli sulla nuova) andranno a sovrascrivere
        correttamente i valori vecchi, rispettando il principio di autorità.
        Infine filtra i tombstone per non esporre chiavi cancellate.
        """
        rebalancing, active, new_topology = self._routing_snapshot()
        try:
            combined: dict[str, tuple[str, int]] = {}
            for shard in active:
                combined.update(self._shard_get_all(shard))

            if rebalancing and new_topology:
                for shard in new_topology:
                    # Merge semantico: i dati nuovi sovrascrivono i vecchi.
                    combined.update(self._shard_get_all(shard))
        except OSError as exc:
            return f"ERR shard_unreachable: {exc}"

        keys = sorted(k for k, (value, _v) in combined.items() if value != TOMBSTONE)
        return "OK " + " ".join(keys) if keys else "OK"

    def _handle_stats(self, args_str: str) -> str:
        """Stampa statistiche diagnostiche interne del Router (shards, size delle cache)."""
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
        """
        Thread in background avviato durante un rebalance. Esegue un polling (PING)
        periodico verso il Coordinator. Se il Coordinator risulta irraggiungibile per
        più di _watchdog_max_failures, deduce un fallimento del Coordinator e chiama
        _abort_rebalance() per sbloccare il sistema e riportarlo a uno stato stabile.
        """
        failed_attempts = 0

        while self._watchdog_active:
            time.sleep(self._watchdog_interval)

            if not self._watchdog_active: # Il rebalance potrebbe essere finito nel frattempo
                break

            try:
                response = send_line(
                    self.coordinator_host,
                    self.coordinator_port,
                    "PING",
                    timeout=1.0
                )
                if response == "OK PONG":
                    failed_attempts = 0
                else:
                    failed_attempts += 1
            except OSError:
                failed_attempts += 1

            if failed_attempts >= self._watchdog_max_failures:
                print(f"[router] ATTENZIONE: Coordinator irraggiungibile ({failed_attempts} fallimenti). Abortisco il rebalance.")
                self._abort_rebalance()
                break

    def _abort_rebalance(self) -> None:
        """
        Annulla lo stato di rebalancing disabilitando i flag e uccidendo il watchdog.
        Mantiene però intatti i pool di shard in aggiunta (_pending_adds) e
        rimozione (_pending_removes) affinché un nuovo Coordinator o un SysAdmin
        possano riprovare la medesima transizione senza ricominciare da zero.
        """
        with self._topology_lock:
            self._rebalancing = False
            self._topology_new = None
            self._watchdog_active = False

    def _handle_add_shard(self, args_str: str) -> str:
        """Aggiunge uno shard alla lista di quelli in attesa per il prossimo rebalance."""
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
                return "ERR rebalance_in_progress" # Previene mutazioni se un rebalance è bloccato in attesa di ACK
            already_active = any(s.shard_id == shard_id for s in self._active_topology)
            if already_active or shard_id in self._pending_adds:
                return "ERR shard_already_present"
            self._pending_adds[shard_id] = ShardRef(shard_id, host, port)
            self._pending_removes.discard(shard_id) # Annulla un'eventuale rimozione pendente
        return "OK"

    def _handle_remove_shard(self, args_str: str) -> str:
        """Aggiunge uno shard attivo (o pendente) alla blacklist per il prossimo rebalance."""
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
            self._pending_adds.pop(shard_id, None) # Annulla un'eventuale aggiunta pendente
        return "OK"

    def _handle_rebalance(self, args_str: str) -> str:
        """
        Innesca il calcolo della nuova topologia basandosi su _active_topology,
        _pending_adds e _pending_removes. Contatta in modo sincrono il Coordinator
        sulla porta di controllo per schedulare formalmente (START_REBALANCE)
        l'inizio del travaso dei dati. Se il Coordinator rifiuta o cade, roll-backa
        la _topology_new.
        """
        if args_str:
            return "ERR usage: REBALANCE"

        with self._topology_lock:
            if self._topology_new is not None:
                return "ERR rebalance_in_progress"
            if not self._pending_adds and not self._pending_removes:
                return "ERR nothing_to_rebalance"

            # Crea la nuova topologia logica: rimuove i mark-for-delete e unisce i nuovi shard.
            new_topology = [
                s for s in self._active_topology if s.shard_id not in self._pending_removes
            ] + list(self._pending_adds.values())

            if not new_topology:
                return "ERR empty_topology"

            old_topology = list(self._active_topology)
            self._topology_new = new_topology

        try:
            # I/O di rete esterno al blocco with, previene stalli.
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
                self._topology_new = None # Rollback in caso di fallimento
            return f"ERR coordinator_rejected: {response}"

        return "OK rebalance_scheduled"

    # ------------------------------------------------------------------
    # Dispatcher control-plane (Coordinator -> Router)
    # ------------------------------------------------------------------
    def handle_control_line(self, line: str) -> str:
        """
        Gateway per i comandi provenienti ESCLUSIVAMENTE dal Coordinator (Control Plane).
        Gestisce le conferme di avvio (ACK_REBALANCE_START) e fine (ACK_REBALANCE_END)
        del ciclo di vita del rebalance.
        """
        stripped = line.strip()

        if stripped == "PING":
            return "OK PONG"

        if stripped == "ACK_REBALANCE_START":
            # Il Coordinator ha confermato di aver iniziato la copia in background.
            # Il Router "apre i cancelli": abilita il fallback in lettura, inizia a deviare
            # le scritture verso la nuova topologia, inibisce le CAS e avvia il Watchdog.
            with self._topology_lock:
                if self._topology_new is None:
                    return "ERR no_rebalance_pending"
                self._rebalancing = True
                self._watchdog_active = True

            print("[router] rebalance confermato dal Coordinator: fallback in lettura abilitato e watchdog avviato")
            threading.Thread(target=self._rebalance_watchdog, daemon=True).start()
            return "OK"

        if stripped == "ACK_REBALANCE_END":
            # Il Coordinator ha terminato la sincronizzazione dei dati.
            # Il Router effettua lo switch-over (atomic commit della nuova topologia).
            with self._topology_lock:
                if self._topology_new is None:
                    return "ERR no_rebalance_pending"

                self._active_topology = self._topology_new
                self._topology_new = None
                self._rebalancing = False
                self._watchdog_active = False # Spegnimento pulito del watchdog

                # Svuotamento code pendenti, pronte per un futuro rebalance
                self._pending_adds = {}
                self._pending_removes = set()

                topology_snapshot = list(self._active_topology)

            print("[router] rebalance completato: nuova topologia attiva, avvio cleanup tombstone")
            # Invia un broadcast in asincrono per purgare i Tombstone ormai irrilevanti
            self._cleanup_tombstones_async(topology_snapshot)
            return "OK"

        return "ERR unknown_control_command"

    def _cleanup_tombstones_async(self, topology: list[ShardRef]) -> None:
        """Avvia un thread daemon per contattare ogni shard e richiedere l'eviction fisica dei tombstone."""
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
        """
        Il cuore del sistema di mutazione. Garantisce la "Exactly-Once Semantics" (tramite idempotenza)
        per operazioni complesse proteggendole da disconnessioni di rete e retry del client.

        Flusso logico:
        1. Parsing del comando (fuori dal lock, per non rallentare l'accesso concorrente).
        2. Validazione sintattica dell'ID richiesta `client_id:sequence_number`.
        3. Acquisizione del lock DI CLIENT. Isola le richieste di questo client da altre.
        4. Controllo Eviction Window: Il client sta chiedendo una seq troppo vecchia (già scartata)?
        5. Controllo Cache: Il client ha già eseguito questo seq_number? Se sì, ritorna la cache,
           verificando che il payload canonico combaci (protezione contro riuso fraudolento di ID).
        6. ESECUZIONE (solo se è la prima volta che vediamo questo seq_number per questo client).
        7. Cache della risposta: Salvataggio dell'esito nella sliding window.
        8. Sliding Window Eviction: Espulsione O(1) in coda se le richieste cachate superano `_window`.
        """
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
            # Controllo 4: Eviction Boundary (Finestra Temporale)
            eviction_boundary = self._evicted_until.get(client_id, -1)
            if seq <= eviction_boundary:
                return "ERR_REQUEST_ID_EXPIRED"

            # Controllo 5: Lookup in Cache
            client_requests = self._requests.setdefault(client_id, {})
            if seq in client_requests:
                record = client_requests[seq]
                # Se il payload non matcha, il client sta usando un seq number valido
                # per un'operazione che non centra nulla (potenziale attacco o bug logico del client).
                if record.payload != payload_canonico:
                    return "ERR_REQUEST_ID_CONFLICT"
                return record.response

            # ---- 6. Prima esecuzione: instradamento + effetto ----
            if cmd == "CAS_REQ":
                rebalancing_now, _active, _new = self._routing_snapshot()
                if rebalancing_now:
                    # Le CAS (Compare-And-Swap) necessitano che i dati siano stabili per
                    # valutare la expected_version. Durante un rebalance transitorio, si
                    # restituisce ERR_REBALANCING per costringere il client al back-off.
                    # Questa risposta NON viene inserita in cache per evitare il blocco perenne.
                    return "ERR_REBALANCING"
                response = self._execute_cas(key, expected_version, value)
            elif cmd == "SET_REQ":
                response = self._execute_set(key, value)
            else:  # DELETE_REQ
                response = self._execute_delete(key)

            # ---- 7. Salvataggio della risposta in Cache ----
            client_requests[seq] = RequestRecord(payload=payload_canonico, response=response)

            # ---- 8. Eviction O(1) con Sliding Window ----
            if len(client_requests) > self._window:
                min_seq = min(client_requests.keys())
                del client_requests[min_seq]
                self._evicted_until[client_id] = min_seq

            return response

    def _execute_set(self, key: str, value: str) -> str:
        """Esegue fisicamente la SET inviando i dati allo shard target corretto (nuovo se in rebalance)."""
        target = self._target_for_write(key)
        version = self._next_version()
        try:
            self._shard_set(target, key, version, value)
        except OSError as exc:
            return f"ERR shard_unreachable: {exc}"
        return f"OK version={version}"

    def _execute_cas(self, key: str, expected_version: int, value: str) -> str:
        """
        Esegue la logica del Compare-And-Swap. Poiché `_handle_mutation` ha bloccato
        questa operazione durante i rebalance, siamo garantiti che `active_topology`
        sia stabile e contenga lo shard autoritativo.
        Esegue il fetch della versione attuale, valida la mutazione e la committa.
        """
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
        """
        Effettua un DELETE logico scrivendo un TOMBSTONE.
        Questo pattern è essenziale per un rebalancing sicuro: se il DELETE
        facesse una vera `del` da dizionario, una lettura durante il rebalance
        effettuerebbe il "fallback" sulla vecchia topologia e resusciterebbe il
        vecchio valore della chiave ("zombie data"). Scrivendo il tombstone sulla
        nuova topologia, intercettiamo il fallback in `_read_with_fallback`.
        """
        # Step 1: Valida che la chiave esista per restare conforme alla Spec
        try:
            found, _value, _version = self._read_with_fallback(key)
        except OSError as exc:
            return f"ERR shard_unreachable: {exc}"

        if not found:
            return "ERR_NOT_FOUND"

        # Step 2: Scrittura autoritativa del Tombstone.
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

    # Control Plane in un thread separato
    control_server = LineTCPServer(args.host, args.control_port, router.handle_control_line)
    threading.Thread(target=control_server.serve_forever, daemon=True).start()

    # Data Plane (pubblico) nel thread principale
    public_server = LineTCPServer(args.host, args.public_port, router.handle_line)
    print(f"Router: client su {args.host}:{args.public_port}, control-plane su {args.host}:{args.control_port}")
    public_server.serve_forever()


if __name__ == "__main__":
    main()