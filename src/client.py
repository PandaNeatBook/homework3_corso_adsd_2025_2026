#!/usr/bin/env python3
"""
Client interattivo per il Router.

Questo script funge da interfaccia CLI (Command Line Interface) per testare
il sistema distribuito. Implementa un pattern REPL (Read-Eval-Print Loop) che
semplifica l'interazione umana con il protocollo del Router.

La sua responsabilità principale, oltre a gestire l'I/O di rete, è l'astrazione
del meccanismo di IDEMPOTENZA:
1. Genera automaticamente e in modo trasparente i `request_id` (formato
   `<client_id>:<sequence_number>`) per le operazioni mutative.
2. Traduce i comandi human-friendly (es. `set key val`) nei comandi di
   protocollo corretti (`SET_REQ clientA:0 key val`).
3. Mantiene in memoria l'ultima mutazione eseguita per permettere di testare
   i retry (e verificare che il Router non ripeta gli effetti collaterali).
"""
from __future__ import annotations

import argparse
import socket
import sys


class KVClient:
    """Gestisce la sessione TCP persistente e lo stato locale del client."""

    def __init__(self, host: str, port: int, client_id: str) -> None:
        self.host = host
        self.port = port

        # Identificativo univoco del client (usato come partizione nella cache del Router)
        self.client_id = client_id

        # Contatore monotono crescente per garantire l'univocità locale delle richieste
        self.seq = 0

        # Memorizza l'esatto payload dell'ultima operazione mutativa per testare
        # i replay tramite il comando 'retry'.
        self.last_mutation: str | None = None

        # Stato della rete
        self.sock: socket.socket | None = None
        self.reader = None

    def connect(self) -> None:
        """Instaura la connessione TCP verso il Router (Data Plane)."""
        self.sock = socket.create_connection((self.host, self.port))
        # Creiamo un wrapper file-like per poter leggere riga per riga (readline)
        self.reader = self.sock.makefile("r", encoding="utf-8", newline="\n")

    def close(self) -> None:
        """Chiude garbatamente la connessione inviando QUIT prima di chiudere il socket."""
        if self.sock is not None:
            try:
                self.send("QUIT")
            except OSError:
                pass
            self.sock.close()

    def send(self, line: str) -> str:
        """
        Invia una riga di comando al Router e attende in modo bloccante la risposta.
        Rappresenta la primitiva di rete sincrona del client.
        """
        if self.sock is None or self.reader is None:
            raise RuntimeError("not connected")

        # Assicura che ogni comando termini con \n come da specifica di protocollo
        self.sock.sendall((line + "\n").encode("utf-8"))
        response = self.reader.readline()

        if response == "":
            raise ConnectionError("server closed connection")

        return response.rstrip("\r\n")

    def next_id(self) -> str:
        """
        Costruisce il request_id autoritativo e incrementa il contatore.
        È essenziale che la sequenza non torni mai indietro per questo client_id,
        altrimenti il Router restituirebbe ERR_REQUEST_ID_EXPIRED.
        """
        request_id = f"{self.client_id}:{self.seq}"
        self.seq += 1
        return request_id

    def loop(self) -> None:
        """Il cuore del REPL: legge l'input utente all'infinito e lo esegue."""
        print(f"Connected to {self.host}:{self.port} as {self.client_id}")
        print("Type 'help' for commands.")

        while True:
            try:
                user_line = input("> ").strip()
            except EOFError:
                # Gestisce correttamente l'uscita tramite Ctrl+D
                print()
                return

            if not user_line:
                continue

            if not self.execute(user_line):
                # Se execute() ritorna False, è tempo di terminare il client
                return

    def execute(self, user_line: str) -> bool:
        """
        Interpreta il comando digitato dall'utente e orchestra l'invio.
        Ritorna True se il loop deve continuare, False se deve terminare.
        """
        cmd = user_line.split(maxsplit=1)[0].lower()

        if cmd in {"quit", "exit"}:
            print(f"<- {self.send('QUIT')}")
            return False

        if cmd == "help":
            self.help()
            return True

        if cmd == "retry":
            # Test didattico dell'idempotenza: reinvia l'ultimo comando mutativo
            # ESATTAMENTE come è stato inviato in precedenza (stesso client_id e stesso seq).
            # Se la cache del Router funziona, l'esito dovrebbe essere identico
            # ma senza che gli ShardNode vengano effettivamente contattati.
            if self.last_mutation is None:
                print("client error: no mutating request to retry")
                return True
            return self._send_and_print(self.last_mutation)

        if cmd == "raw":
            # Consente all'utente di bypassare la traduzione automatica (e il next_id())
            # per testare edge-case malevoli (es. ID contraffatti, versioni errate, ecc.)
            parts = user_line.split(maxsplit=1)
            if len(parts) != 2:
                print("client error: usage: raw <protocol command>")
                return True
            return self._send_and_print(parts[1])

        # Traduzione standard
        try:
            protocol_line = self.translate(user_line)
        except ValueError as exc:
            # Cattura errori di validazione argomenti locali prima di usare la rete
            print(f"client error: {exc}")
            return True

        if protocol_line is None:
            print("client error: unknown command. Type 'help'.")
            return True

        return self._send_and_print(protocol_line)

    def _send_and_print(self, protocol_line: str) -> bool:
        """Invia il comando raw, stampa a video i log I/O e aggiorna lo stato mutativo."""
        print(f"-> {protocol_line}")

        try:
            response = self.send(protocol_line)
        except OSError as exc:
            print(f"connection error: {exc}")
            return False

        print(f"<- {response}")

        # Se il comando era una mutazione, ne salvo il testo tradotto per un eventuale 'retry'
        cmd = protocol_line.split(maxsplit=1)[0].upper()
        if cmd in {"SET_REQ", "CAS_REQ", "DELETE_REQ"}:
            self.last_mutation = protocol_line

        return True

    def translate(self, user_line: str) -> str | None:
        """
        Mappatura tra l'intento dell'utente e il protocollo di rete ufficiale.
        Qui avviene l'iniezione automatica del request_id generato da next_id().
        """
        parts = user_line.split()
        cmd = parts[0].lower()

        # --- Comandi non mutativi (Nessun ID necessario) ---
        if cmd == "ping":
            self._arity(parts, 1, "ping")
            return "PING"
        if cmd == "keys":
            self._arity(parts, 1, "keys")
            return "KEYS"
        if cmd == "stats":
            self._arity(parts, 1, "stats")
            return "STATS"
        if cmd in {"get", "getv"}:
            self._arity(parts, 2, f"{cmd} <key>")
            return f"{cmd.upper()} {parts[1]}"

        # --- Comandi mutativi (Iniezione IDempotenza necessaria) ---
        if cmd == "set":
            parts = user_line.split(maxsplit=2)  # Splitta in 3: cmd, key, [value...]
            if len(parts) != 3:
                raise ValueError("usage: set <key> <value...>")
            return f"SET_REQ {self.next_id()} {parts[1]} {parts[2]}"

        if cmd == "cas":
            parts = user_line.split(maxsplit=3)
            if len(parts) != 4:
                raise ValueError("usage: cas <key> <expected_version> <value...>")
            return f"CAS_REQ {self.next_id()} {parts[1]} {parts[2]} {parts[3]}"

        if cmd == "delete":
            self._arity(parts, 2, "delete <key>")
            return f"DELETE_REQ {self.next_id()} {parts[1]}"

        # --- Comandi di Amministrazione topologia ---
        if cmd == "add_shard":
            self._arity(parts, 3, "add_shard <id> <host:port>")
            return f"ADD_SHARD {parts[1]} {parts[2]}"
        if cmd == "remove_shard":
            self._arity(parts, 3, "remove_shard <id> <host:port>")
            return f"REMOVE_SHARD {parts[1]} {parts[2]}"
        if cmd == "rebalance":
            self._arity(parts, 1, "rebalance")
            return "REBALANCE"

        return None

    @staticmethod
    def _arity(parts: list[str], n: int, usage: str) -> None:
        """Helper per validare rapidamente il numero di argomenti del comando."""
        if len(parts) != n:
            raise ValueError(f"usage: {usage}")

    @staticmethod
    def help() -> None:
        print(
            """
Available commands:
  ping
  get <key>
  getv <key>
  keys
  stats
  set <key> <value...>
  cas <key> <expected_version> <value...>
  delete <key>
  retry
  add_shard <id> <host:port>
  remove_shard <id> <host:port>
  rebalance
  raw <protocol command>
  help
  quit
""".strip()
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7000)
    parser.add_argument("--client-id", default="clientA")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = KVClient(args.host, args.port, args.client_id)

    try:
        client.connect()
        client.loop()
    except OSError as exc:
        print(f"connection error: {exc}", file=sys.stderr)
        sys.exit(1)
    finally:
        client.close()


if __name__ == "__main__":
    main()