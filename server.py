#!/usr/bin/env python3
from __future__ import annotations

import argparse
import socket
import threading
from dataclasses import dataclass
from typing import Callable


@dataclass
class ValueRecord:
    value: str
    version: int


@dataclass
class RequestRecord:
    payload: str
    response: str


class KVStore:
    def __init__(self, request_window_size: int = 100) -> None:
        if request_window_size < 1:
            raise ValueError("request_window_size must be >= 1")
        self._data: dict[str, ValueRecord] = {}
        self._requests: dict[str, dict[int, RequestRecord]] = {}
        self._evicted_until: dict[str, int] = {}
        self._window = request_window_size
        self._lock = threading.Lock()

    def handle_line(self, raw_line: str) -> str:
        line = raw_line.rstrip("\r\n")
        if not line.strip():
            return "ERR empty_command"

        cmd = line.split(maxsplit=1)[0].upper()
        try:
            if cmd == "PING":
                return self._zero_arg(line, "PONG")
            if cmd == "QUIT":
                return self._zero_arg(line, "BYE")
            if cmd == "GET":
                return self._get(line, versioned=False)
            if cmd == "GETV":
                return self._get(line, versioned=True)
            if cmd == "EXISTS":
                return self._exists(line)
            if cmd == "KEYS":
                return self._keys(line)
            if cmd == "STATS":
                return self._stats(line)
            if cmd == "SET_REQ":
                return self._set_req(line)
            if cmd == "CAS_REQ":
                return self._cas_req(line)
            if cmd == "DELETE_REQ":
                return self._delete_req(line)
            if cmd == "SET":
                return self._set(line)
            if cmd == "CAS":
                return self._cas(line)
            if cmd == "DELETE":
                return self._delete(line)
            return "ERR unknown_command"
        except ValueError as exc:
            return f"ERR {exc}"

    @staticmethod
    def _zero_arg(line: str, response: str) -> str:
        return response if len(line.split()) == 1 else "ERR malformed"

    @staticmethod
    def _check_key(key: str) -> None:
        if not key or any(ch.isspace() for ch in key):
            raise ValueError("bad_key")

    @staticmethod
    def _parse_int(text: str, error: str) -> int:
        try:
            return int(text)
        except ValueError as exc:
            raise ValueError(error) from exc

    @classmethod
    def _parse_request_id(cls, request_id: str) -> tuple[str, int]:
        if ":" not in request_id:
            raise ValueError("bad_request_id")
        client_id, seq_text = request_id.rsplit(":", 1)
        if not client_id or ":" in client_id or any(ch.isspace() for ch in client_id):
            raise ValueError("bad_request_id")
        seq = cls._parse_int(seq_text, "bad_request_id")
        if seq < 0:
            raise ValueError("bad_request_id")
        return client_id, seq

    def _get(self, line: str, versioned: bool) -> str:
        parts = line.split()
        if len(parts) != 2:
            return "ERR malformed"
        key = parts[1]
        self._check_key(key)
        with self._lock:
            rec = self._data.get(key)
        if rec is None:
            return "NOT_FOUND"
        return f"OK version={rec.version} {rec.value}" if versioned else f"OK {rec.value}"

    def _exists(self, line: str) -> str:
        parts = line.split()
        if len(parts) != 2:
            return "ERR malformed"
        key = parts[1]
        self._check_key(key)
        with self._lock:
            exists = key in self._data
        return f"OK {str(exists).lower()}"

    def _keys(self, line: str) -> str:
        if len(line.split()) != 1:
            return "ERR malformed"
        with self._lock:
            keys = sorted(self._data)
        return "OK" if not keys else "OK " + " ".join(keys)

    def _stats(self, line: str) -> str:
        if len(line.split()) != 1:
            return "ERR malformed"
        with self._lock:
            nreq = sum(len(t) for t in self._requests.values())
            return (
                f"OK keys={len(self._data)} clients={len(self._requests)} "
                f"cached_requests={nreq} window_size={self._window}"
            )

    def _set_req(self, line: str) -> str:
        parts = line.split(maxsplit=3)
        if len(parts) != 4:
            return "ERR malformed"
        _, rid, key, value = parts
        self._check_key(key)
        client_id, seq = self._parse_request_id(rid)
        payload = f"SET_REQ\n{client_id}\n{seq}\n{key}\n{value}"
        return self._idempotent(
            client_id,
            seq,
            payload,
            lambda: self._apply_set(key, value),
        )

    def _cas_req(self, line: str) -> str:
        parts = line.split(maxsplit=4)
        if len(parts) != 5:
            return "ERR malformed"
        _, rid, key, expected_text, value = parts
        self._check_key(key)
        expected = self._parse_int(expected_text, "bad_version")
        client_id, seq = self._parse_request_id(rid)
        payload = f"CAS_REQ\n{client_id}\n{seq}\n{key}\n{expected}\n{value}"
        return self._idempotent(
            client_id,
            seq,
            payload,
            lambda: self._apply_cas(key, expected, value),
        )

    def _delete_req(self, line: str) -> str:
        parts = line.split()
        if len(parts) != 3:
            return "ERR malformed"
        _, rid, key = parts
        self._check_key(key)
        client_id, seq = self._parse_request_id(rid)
        payload = f"DELETE_REQ\n{client_id}\n{seq}\n{key}"
        return self._idempotent(
            client_id,
            seq,
            payload,
            lambda: self._apply_delete(key),
        )

    def _idempotent(
        self,
        client_id: str,
        seq: int,
        payload: str,
        apply: Callable[[], str],
    ) -> str:
        with self._lock:
            table = self._requests.setdefault(client_id, {})
            old = table.get(seq)

            if old is not None:
                if old.payload == payload:
                    return old.response
                return "ERR request_id_conflict"

            if seq <= self._evicted_until.get(client_id, -1):
                return "ERR request_id_expired"

            response = apply()
            table[seq] = RequestRecord(payload, response)

            while len(table) > self._window:
                dropped = min(table)
                del table[dropped]
                self._evicted_until[client_id] = max(
                    self._evicted_until.get(client_id, -1),
                    dropped,
                )

            return response

    def _set(self, line: str) -> str:
        parts = line.split(maxsplit=2)
        if len(parts) != 3:
            return "ERR malformed"
        _, key, value = parts
        self._check_key(key)
        with self._lock:
            return self._apply_set(key, value)

    def _cas(self, line: str) -> str:
        parts = line.split(maxsplit=3)
        if len(parts) != 4:
            return "ERR malformed"
        _, key, expected_text, value = parts
        self._check_key(key)
        expected = self._parse_int(expected_text, "bad_version")
        with self._lock:
            return self._apply_cas(key, expected, value)

    def _delete(self, line: str) -> str:
        parts = line.split()
        if len(parts) != 2:
            return "ERR malformed"
        key = parts[1]
        self._check_key(key)
        with self._lock:
            return self._apply_delete(key)

    def _apply_set(self, key: str, value: str) -> str:
        old = self._data.get(key)
        version = 0 if old is None else old.version + 1
        self._data[key] = ValueRecord(value, version)
        return f"OK version={version}"

    def _apply_cas(self, key: str, expected: int, value: str) -> str:
        old = self._data.get(key)
        if old is None:
            return "ERR not_found"
        if old.version != expected:
            return f"ERR version_mismatch current={old.version}"
        version = old.version + 1
        self._data[key] = ValueRecord(value, version)
        return f"OK version={version}"

    def _apply_delete(self, key: str) -> str:
        if key not in self._data:
            return "NOT_FOUND"
        del self._data[key]
        return "OK deleted=true"


class TCPKVServer:
    def __init__(self, host: str, port: int, store: KVStore) -> None:
        self.host = host
        self.port = port
        self.store = store

    def serve_forever(self) -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((self.host, self.port))
            sock.listen()
            print(f"KV server listening on {self.host}:{self.port}")

            while True:
                conn, addr = sock.accept()
                threading.Thread(
                    target=self._handle_client,
                    args=(conn, addr),
                    daemon=True,
                ).start()

    def _handle_client(self, conn: socket.socket, addr: tuple[str, int]) -> None:
        with conn:
            try:
                for line in conn.makefile("r", encoding="utf-8", newline="\n"):
                    response = self.store.handle_line(line)
                    conn.sendall((response + "\n").encode("utf-8"))
                    if response == "BYE":
                        break
            except OSError:
                pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--request-window-size", type=int, default=100)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    store = KVStore(args.request_window_size)

    try:
        TCPKVServer(args.host, args.port, store).serve_forever()
    except KeyboardInterrupt:
        print("\nKV server stopped")


if __name__ == "__main__":
    main()