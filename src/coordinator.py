"""
Coordinator: motore di migrazione del sistema di rebalancing.

Riceve dal Router, sul proprio control plane, il segnale di avvio rebalance
(topologia vecchia N e nuova N+1: START_REBALANCE). Il suo compito è spostare
fisicamente i dati tra gli ShardNode in modo trasparente per i client.

Il flusso di lavoro si articola in 3 fasi distinte per garantire l'assenza di
disservizi (zero-downtime) e la consistenza:
  1. PREPARE (Copy): Notifica al Router ACK_REBALANCE_START (che attiva il
     fallback in lettura). Legge l'intero dump dagli shard vecchi e lo copia
     sui nuovi shard presidiando la versione originale.
  2. COMMIT (Switch-over): Notifica al Router ACK_REBALANCE_END. Il Router
     abbandona la vecchia topologia e rende la nuova autoritativa al 100%.
  3. CLEANUP (Garbage Collection): Rimuove fisicamente i dati dagli shard
     vecchi, ormai irraggiungibili dai client.

Il Coordinator è l'unico componente che sposta i dati fisicamente: gli
ShardNode restano "stupidi" (rifiutano scritture vecchie) e il Router si
limita a instradare le richieste logiche.
"""
from __future__ import annotations

import json
import threading

from src.protocol_common import (
    LineTCPServer,
    ShardRef,
    decode_topology,
    send_line,
    shard_for_key,
)


class Coordinator:
    """
    Gestore del Control Plane per le transizioni di topologia.
    A differenza del Router e degli ShardNode, il Coordinator non intercetta
    mai il traffico "pubblico" dei client (Data Plane).
    """

    def __init__(self, router_control_host: str, router_control_port: int) -> None:
        # Memorizza le coordinate del Control Plane del Router.
        # Il Coordinator agisce come "Server" per ricevere i comandi dall'admin/Router,
        # ma agisce come "Client" quando deve notificare al Router l'avanzamento dei lavori.
        self.router_control_host = router_control_host
        self.router_control_port = router_control_port

    # ------------------------------------------------------------------
    # Control plane: riceve i comandi dal Router
    # ------------------------------------------------------------------
    def handle_line(self, line: str) -> str:
        """
        Entry-point per il server TCP del Coordinator.
        Intercetta le richieste di START_REBALANCE. Poiché la migrazione può
        richiedere tempo (I/O di rete massivo), questo metodo risponde *immediatamente*
        con OK e delega il lavoro pesante a un thread in background.
        """
        stripped = line.strip()
        parts = stripped.split(maxsplit=2)
        if not parts:
            return "ERR empty_command"
        cmd = parts[0].upper()

        if cmd == "PING":
            # Usato dal Watchdog del Router per verificare che il Coordinator sia vivo
            return "OK PONG"

        if cmd == "START_REBALANCE":
            if len(parts) != 3:
                return "ERR usage: START_REBALANCE <old_topology_json> <new_topology_json>"
            try:
                old_topology = decode_topology(parts[1])
                new_topology = decode_topology(parts[2])
            except (ValueError, json.JSONDecodeError):
                return "ERR bad_topology"

            # Rispondiamo subito al Router in modo sincrono per confermare
            # la validità del comando. Il vero inizio dei lavori verrà
            # notificato in modo asincrono tramite ACK_REBALANCE_START.
            self._start_rebalance_async(old_topology, new_topology)
            return "OK rebalance_scheduled"

        return "ERR unknown_command"

    # ------------------------------------------------------------------
    # Comunicazione verso il Router (control plane)
    # ------------------------------------------------------------------
    def _notify_router(self, message: str) -> None:
        """
        Apre una connessione on-the-fly verso la porta di controllo del Router
        per notificargli i cambi di stato (ACK_START, ACK_END).
        Questo design a chiamate indipendenti evita connessioni TCP tenute
        aperte e bloccate per ore durante migrazioni di grandi dimensioni.
        """
        try:
            send_line(self.router_control_host, self.router_control_port, message)
        except OSError as exc:
            print(f"[coordinator] impossibile notificare il Router ({message}): {exc}")

    # ------------------------------------------------------------------
    # Comunicazione verso gli ShardNode (data plane)
    # ------------------------------------------------------------------
    @staticmethod
    def _shard_get_all(shard: ShardRef) -> dict[str, tuple[str, int]]:
        """Scarica l'intero dizionario (valori e versioni) da un singolo shard."""
        response = send_line(shard.host, shard.port, "SHARD_GET_ALL")
        if not response.startswith("OK "):
            raise RuntimeError(f"SHARD_GET_ALL fallita su {shard.shard_id}: {response}")
        raw_json = response[len("OK "):]
        data = json.loads(raw_json)
        return {k: (v[0], v[1]) for k, v in data.items()}

    @staticmethod
    def _shard_set(shard: ShardRef, key: str, version: int, value: str) -> str:
        """Forza la scrittura di una chiave, valore e versione sul nuovo shard."""
        return send_line(shard.host, shard.port, f"SHARD_SET {key} {version} {value}")

    @staticmethod
    def _shard_remove_physical(shard: ShardRef, key: str) -> str:
        """Elimina definitivamente la chiave dalla RAM del vecchio shard."""
        return send_line(shard.host, shard.port, f"SHARD_REMOVE_PHYSICAL {key}")

    # ------------------------------------------------------------------
    # Migrazione
    # ------------------------------------------------------------------
    def _start_rebalance_async(self, old_topology: list[ShardRef], new_topology: list[ShardRef]) -> None:
        """Sgancia il processo di copia in un thread daemon separato."""
        threading.Thread(
            target=self._run_rebalance_blocking,
            args=(old_topology, new_topology),
            daemon=True,
        ).start()

    def _run_rebalance_blocking(self, old_topology: list[ShardRef], new_topology: list[ShardRef]) -> None:
        """
        Cuore del processo di migrazione, eseguito in background.
        Implementa la logica per migrare i dati mantenendo l'integrità
        grazie al fallback di lettura del Router e alle versioni dello Shard.
        """
        print(f"[coordinator] avvio rebalance (Phase 1: Copy): {len(old_topology)} -> {len(new_topology)} shard")

        # Avvisa il Router di "aprire i cancelli": il Router inizierà a
        # deviare le nuove scritture sulla nuova topologia e ad abilitare
        # il fallback in lettura.
        self._notify_router("ACK_REBALANCE_START")

        migrated = 0
        keys_to_delete: list[tuple[ShardRef, str]] = []

        # --- FASE 1: PREPARE (Copia dei dati) ---
        # Si itera sulla VECCHIA topologia per trovare dove sono i dati attualmente.
        for shard in old_topology:
            try:
                # Prende un'istantanea di tutto ciò che è contenuto nello shard
                snapshot = self._shard_get_all(shard)
            except (OSError, RuntimeError) as exc:
                print(f"[coordinator] impossibile leggere lo shard {shard.shard_id}: {exc}")
                continue

            # Per ogni chiave presente nel vecchio shard, si ricalcola a chi appartiene
            # in base al nuovo hash ring (N+1).
            for key, (value, version) in snapshot.items():
                target = shard_for_key(key, new_topology)

                if target.shard_id == shard.shard_id:
                    # Ottimizzazione: se l'hashing assegna la chiave allo stesso
                    # nodo di prima (es. aggiungiamo uno shard, ma questa chiave
                    # non cambia proprietario), saltiamo la copia.
                    continue

                try:
                    # Copiamo il dato sul nuovo shard.
                    # NOTA SULLA CONCORRENZA: Se un client fa una SET tramite il
                    # Router (che ora indirizza sulla topologia nuova) e la sua
                    # versione è > di questa, lo ShardNode rifiuterà questa
                    # copia, preservando il dato nuovo. È esattamente ciò che vogliamo.
                    self._shard_set(target, key, version, value)

                    # Salviamo l'intento di cancellazione.
                    # ATTENZIONE: NON possiamo eliminare il dato dal vecchio shard ORA!
                    # Se lo facessimo e il client provasse a leggere prima che la fase 2
                    # sia conclusa (durante il read-fallback), il Router non troverebbe
                    # il dato né sul nuovo né sul vecchio.
                    keys_to_delete.append((shard, key))
                    migrated += 1
                except OSError as exc:
                    print(f"[coordinator] copia fallita per chiave '{key}': {exc}")

        # --- FASE 2: COMMIT (Cambio topologia sul Router) ---
        # A questo punto i dati sono duplicati in modo sicuro su entrambe le topologie.
        print(f"[coordinator] copia completata ({migrated} chiavi). Avvio Phase 2: Commit della topologia")

        # Notificando END, il Router smetterà di usare la vecchia topologia.
        # Da questo esatto istante, i vecchi shard sono "invisibili" per i client.
        self._notify_router("ACK_REBALANCE_END")

        # --- FASE 3: CLEANUP (Rimozione fisica) ---
        # Ora che il Router indirizza tutto sul nuovo cluster e non fa più fallback,
        # i dati originali parcheggiati sui vecchi shard sono spazzatura che occupa RAM.
        print(f"[coordinator] topologia aggiornata. Avvio Phase 3: Cleanup vecchi dati")
        cleaned = 0

        for old_shard, key in keys_to_delete:
            try:
                # Cancellazione fisica definitiva
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