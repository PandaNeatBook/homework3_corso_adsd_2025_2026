"""
Coordinator: motore di migrazione del sistema di rebalancing.

Riceve dal Router, sul proprio control plane, il segnale di avvio rebalance
(topologia vecchia N e nuova N+1: START_REBALANCE), e si occupa di:

  1. notificare al Router, aprendo una NUOVA connessione verso il suo control
     plane, ACK_REBALANCE_START (da questo momento il Router abilita il
     fallback in lettura e dirotta le scritture sulla sola topologia nuova);
  2. per ogni shard della topologia vecchia, leggere l'intero dizionario
     (SHARD_GET_ALL) e, per ogni chiave il cui shard di destinazione cambia,
     scriverla sul nuovo shard (SHARD_SET, con la stessa versione) e
     rimuoverla fisicamente da quello vecchio (SHARD_REMOVE_PHYSICAL);
  3. una volta terminata la migrazione, notificare al Router ACK_REBALANCE_END.

Il Coordinator e' l'unico componente che sposta i dati fisicamente: gli
ShardNode restano "stupidi" (si limitano ad accettare o rifiutare scritture in
base alla versione) e il Router si limita a instradare le richieste.
"""
from __future__ import annotations

import json
import threading

from protocol_common import (
    LineTCPServer,
    ShardRef,
    decode_topology,
    send_line,
    shard_for_key,
)


class Coordinator:
    def __init__(self, router_control_host: str, router_control_port: int) -> None:
        self.router_control_host = router_control_host
        self.router_control_port = router_control_port

    # ------------------------------------------------------------------
    # Control plane: riceve i comandi dal Router
    # ------------------------------------------------------------------
    def handle_line(self, line: str) -> str:
        stripped = line.strip()
        parts = stripped.split(maxsplit=2)
        if not parts:
            return "ERR empty_command"
        cmd = parts[0].upper()

        if cmd == "PING":
            return "OK PONG"

        if cmd == "START_REBALANCE":
            if len(parts) != 3:
                return "ERR usage: START_REBALANCE <old_topology_json> <new_topology_json>"
            try:
                old_topology = decode_topology(parts[1])
                new_topology = decode_topology(parts[2])
            except (ValueError, json.JSONDecodeError):
                return "ERR bad_topology"
            # Rispondiamo subito per confermare la ricezione: l'ACK "di
            # business" (ACK_REBALANCE_START) arriva al Router come
            # connessione separata, aperta da questo stesso Coordinator dopo
            # aver preso in carico la migrazione (vedi _run_rebalance_blocking).
            self._start_rebalance_async(old_topology, new_topology)
            return "OK rebalance_scheduled"

        return "ERR unknown_command"

    # ------------------------------------------------------------------
    # Comunicazione verso il Router (control plane)
    # ------------------------------------------------------------------
    def _notify_router(self, message: str) -> None:
        try:
            send_line(self.router_control_host, self.router_control_port, message)
        except OSError as exc:
            print(f"[coordinator] impossibile notificare il Router ({message}): {exc}")

    # ------------------------------------------------------------------
    # Comunicazione verso gli ShardNode (data plane)
    # ------------------------------------------------------------------
    @staticmethod
    def _shard_get_all(shard: ShardRef) -> dict[str, tuple[str, int]]:
        response = send_line(shard.host, shard.port, "SHARD_GET_ALL")
        if not response.startswith("OK "):
            raise RuntimeError(f"SHARD_GET_ALL fallita su {shard.shard_id}: {response}")
        raw_json = response[len("OK "):]
        data = json.loads(raw_json)
        return {k: (v[0], v[1]) for k, v in data.items()}

    @staticmethod
    def _shard_set(shard: ShardRef, key: str, version: int, value: str) -> str:
        return send_line(shard.host, shard.port, f"SHARD_SET {key} {version} {value}")

    @staticmethod
    def _shard_remove_physical(shard: ShardRef, key: str) -> str:
        return send_line(shard.host, shard.port, f"SHARD_REMOVE_PHYSICAL {key}")

    # ------------------------------------------------------------------
    # Migrazione
    # ------------------------------------------------------------------
    def _start_rebalance_async(self, old_topology: list[ShardRef], new_topology: list[ShardRef]) -> None:
        threading.Thread(
            target=self._run_rebalance_blocking,
            args=(old_topology, new_topology),
            daemon=True,
        ).start()

    def _run_rebalance_blocking(self, old_topology: list[ShardRef], new_topology: list[ShardRef]) -> None:
        print(f"[coordinator] avvio rebalance (Phase 1: Copy): {len(old_topology)} -> {len(new_topology)} shard")
        self._notify_router("ACK_REBALANCE_START")

        migrated = 0
        keys_to_delete: list[tuple[ShardRef, str]] = []

        # --- FASE 1: PREPARE (Copia dei dati) ---
        for shard in old_topology:
            try:
                snapshot = self._shard_get_all(shard)
            except (OSError, RuntimeError) as exc:
                print(f"[coordinator] impossibile leggere lo shard {shard.shard_id}: {exc}")
                continue

            for key, (value, version) in snapshot.items():
                target = shard_for_key(key, new_topology)
                if target.shard_id == shard.shard_id:
                    # La chiave resta fisicamente sullo stesso shard: nulla da fare.
                    continue

                try:
                    # Copiamo il dato sul nuovo shard
                    self._shard_set(target, key, version, value)

                    # Salviamo l'intento di cancellazione per la Fase 3,
                    # ma NON tocchiamo ancora il dato sul vecchio shard.
                    keys_to_delete.append((shard, key))
                    migrated += 1
                except OSError as exc:
                    print(f"[coordinator] copia fallita per chiave '{key}': {exc}")

        # --- FASE 2: COMMIT (Cambio topologia sul Router) ---
        # A questo punto i dati sono duplicati in modo sicuro.
        print(f"[coordinator] copia completata ({migrated} chiavi). Avvio Phase 2: Commit della topologia")
        self._notify_router("ACK_REBALANCE_END")

        # --- FASE 3: CLEANUP (Rimozione fisica) ---
        # Il Router ha abbandonato la vecchia topologia. Possiamo eliminare le vecchie copie.
        print(f"[coordinator] topologia aggiornata. Avvio Phase 3: Cleanup vecchi dati")
        cleaned = 0
        for old_shard, key in keys_to_delete:
            try:
                self._shard_remove_physical(old_shard, key)
                cleaned += 1
            except OSError as exc:
                print(f"[coordinator] cleanup fallito per chiave '{key}' su {old_shard.shard_id}: {exc}")

        print(
            f"[coordinator] rebalance e cleanup completati in sicurezza. Chiavi rimosse: {cleaned}/{len(keys_to_delete)}")


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Avvia il Coordinator standalone")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True, help="porta del control plane del Coordinator")
    parser.add_argument("--router-host", default="127.0.0.1")
    parser.add_argument("--router-control-port", type=int, required=True)
    args = parser.parse_args()

    coordinator = Coordinator(args.router_host, args.router_control_port)
    server = LineTCPServer(args.host, args.port, coordinator.handle_line)
    print(f"Coordinator in ascolto su {args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()