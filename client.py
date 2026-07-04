#!/usr/bin/env python3
from __future__ import annotations

import argparse
import socket
import sys


class KVClient:
    def __init__(self, host: str, port: int, client_id: str) -> None:
        self.host = host
        self.port = port
        self.client_id = client_id
        self.seq = 0
        self.last_mutation: str | None = None
        self.sock: socket.socket | None = None
        self.reader = None

    def connect(self) -> None:
        self.sock = socket.create_connection((self.host, self.port))
        self.reader = self.sock.makefile("r", encoding="utf-8", newline="\n")

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.send("QUIT")
            except OSError:
                pass
            self.sock.close()

    def send(self, line: str) -> str:
        if self.sock is None or self.reader is None:
            raise RuntimeError("not connected")

        self.sock.sendall((line + "\n").encode("utf-8"))
        response = self.reader.readline()

        if response == "":
            raise ConnectionError("server closed connection")

        return response.rstrip("\r\n")

    def next_id(self) -> str:
        request_id = f"{self.client_id}:{self.seq}"
        self.seq += 1
        return request_id

    def loop(self) -> None:
        print(f"Connected to {self.host}:{self.port} as {self.client_id}")
        print("Type 'help' for commands.")

        while True:
            try:
                user_line = input("> ").strip()
            except EOFError:
                print()
                return

            if not user_line:
                continue

            if not self.execute(user_line):
                return

    def execute(self, user_line: str) -> bool:
        cmd = user_line.split(maxsplit=1)[0].lower()

        if cmd in {"quit", "exit"}:
            print(f"<- {self.send('QUIT')}")
            return False

        if cmd == "help":
            self.help()
            return True

        if cmd == "retry":
            if self.last_mutation is None:
                print("client error: no mutating request to retry")
                return True
            return self._send_and_print(self.last_mutation)

        if cmd == "raw":
            parts = user_line.split(maxsplit=1)
            if len(parts) != 2:
                print("client error: usage: raw <protocol command>")
                return True
            return self._send_and_print(parts[1])

        try:
            protocol_line = self.translate(user_line)
        except ValueError as exc:
            print(f"client error: {exc}")
            return True

        if protocol_line is None:
            print("client error: unknown command. Type 'help'.")
            return True

        return self._send_and_print(protocol_line)

    def _send_and_print(self, protocol_line: str) -> bool:
        print(f"-> {protocol_line}")

        try:
            response = self.send(protocol_line)
        except OSError as exc:
            print(f"connection error: {exc}")
            return False

        print(f"<- {response}")

        cmd = protocol_line.split(maxsplit=1)[0].upper()
        if cmd in {"SET_REQ", "CAS_REQ", "DELETE_REQ"}:
            self.last_mutation = protocol_line

        return True

    def translate(self, user_line: str) -> str | None:
        parts = user_line.split()
        cmd = parts[0].lower()

        if cmd == "ping":
            self._arity(parts, 1, "ping")
            return "PING"

        if cmd == "keys":
            self._arity(parts, 1, "keys")
            return "KEYS"

        if cmd == "stats":
            self._arity(parts, 1, "stats")
            return "STATS"

        if cmd in {"get", "getv", "exists"}:
            self._arity(parts, 2, f"{cmd} <key>")
            return f"{cmd.upper()} {parts[1]}"

        if cmd == "set":
            parts = user_line.split(maxsplit=2)
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

        return None

    @staticmethod
    def _arity(parts: list[str], n: int, usage: str) -> None:
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
  exists <key>
  keys
  stats
  set <key> <value...>
  cas <key> <expected_version> <value...>
  delete <key>
  retry
  raw <protocol command>
  help
  quit
""".strip()
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
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