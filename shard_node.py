"""
ShardNode: unita' di memorizzazione "stupida" del sistema distribuito.

Mantiene in RAM un dizionario chiave -> (valore, versione). Non conosce nulla
della topologia, del rebalancing o dell'idempotenza lato client: la sua unica
responsabilita' e' il contratto fondamentale del Data Plane descritto nella
specifica di rebalancing:

    Accetta e memorizza un valore in ingresso SOLO SE la versione fornita e'
    strettamente maggiore della versione attualmente presente per quella
    chiave. Altrimenti ignora l'operazione (scarta scritture di migrazione
    arrivate in ritardo rispetto a scritture piu' recenti).

Protocollo (una riga per comando, una riga per risposta):

    SHARD_SET <key> <version> <value...>   -> OK stored | OK stale | ERR usage: ...
    SHARD_GET <key>                        -> OK <version> <value...> | ERR_NOT_FOUND
    SHARD_GET_ALL                          -> OK <json>
    SHARD_REMOVE_PHYSICAL <key>            -> OK removed | OK absent
    SHARD_CLEANUP_TOMBSTONES               -> OK removed=<n>
    PING                                   -> OK PONG

NOTA sull'ordine degli argomenti di SHARD_SET: il contratto originale elenca
"<key> <value> <versione>", ma poiche' i valori possono contenere spazi, la
versione deve essere un token non ambiguo per poter fare parsing senza
escaping. La mettiamo quindi subito dopo la chiave (SHARD_SET <key> <version>
<value...>), cosi' il resto della riga e' preso per intero come valore -
esattamente come gia' avviene per SET_REQ nel protocollo dell'Homework 3. La
semantica del contratto e' identica: cambia solo l'ordine dei token sul wire,
non il comportamento.
"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass

from protocol_common import TOMBSTONE, LineTCPServer


@dataclass
class StoredValue:
    value: str
    version: int


class ShardStore:
    """Store in-memory thread-safe di un singolo shard."""

    def __init__(self) -> None:
        self._data: dict[str, StoredValue] = {}
        self._meta_lock = threading.Lock()
        self._key_locks: dict[str, threading.Lock] = {}
        # Protegge le operazioni che iterano l'intero dizionario (SHARD_GET_ALL,
        # SHARD_CLEANUP_TOMBSTONES) da mutazioni concorrenti sulla struttura.
        self._structure_lock = threading.Lock()

    def _get_key_lock(self, key: str) -> threading.Lock:
        with self._meta_lock:
            if key not in self._key_locks:
                self._key_locks[key] = threading.Lock()
            return self._key_locks[key]

    def handle_line(self, line: str) -> str:
        stripped = line.strip()
        if not stripped:
            return "ERR empty_command"

        parts = stripped.split(maxsplit=1)
        cmd = parts[0].upper()
        args_str = parts[1] if len(parts) > 1 else ""

        if cmd == "PING":
            return "OK PONG"

        if cmd == "SHARD_SET":
            return self._handle_set(args_str)

        if cmd == "SHARD_GET":
            return self._handle_get(args_str)

        if cmd == "SHARD_GET_ALL":
            return self._handle_get_all(args_str)

        if cmd == "SHARD_REMOVE_PHYSICAL":
            return self._handle_remove_physical(args_str)

        if cmd == "SHARD_CLEANUP_TOMBSTONES":
            return self._handle_cleanup_tombstones(args_str)

        return "ERR unknown_command"

    def _handle_set(self, args_str: str) -> str:
        tokens = args_str.split(maxsplit=2)
        if len(tokens) < 3:
            return "ERR usage: SHARD_SET <key> <version> <value...>"
        key, version_str, value = tokens[0], tokens[1], tokens[2]
        try:
            version = int(version_str)
        except ValueError:
            return "ERR bad_version"

        key_lock = self._get_key_lock(key)
        with key_lock:
            with self._structure_lock:
                existing = self._data.get(key)
                if existing is not None and version <= existing.version:
                    # Scrittura obsoleta (es. migrazione in ritardo rispetto a
                    # una scrittura piu' recente gia' arrivata sul nuovo shard):
                    # viene scartata silenziosamente, come da contratto.
                    return "OK stale"
                self._data[key] = StoredValue(value=value, version=version)
            return "OK stored"

    def _handle_get(self, args_str: str) -> str:
        key = args_str.strip()
        if not key or len(key.split()) != 1:
            return "ERR usage: SHARD_GET <key>"
        key_lock = self._get_key_lock(key)
        with key_lock:
            existing = self._data.get(key)
            if existing is None:
                return "ERR_NOT_FOUND"
            return f"OK {existing.version} {existing.value}"

    def _handle_get_all(self, args_str: str) -> str:
        if args_str:
            return "ERR usage: SHARD_GET_ALL"
        with self._structure_lock:
            snapshot = {k: [v.value, v.version] for k, v in self._data.items()}
        return "OK " + json.dumps(snapshot)

    def _handle_remove_physical(self, args_str: str) -> str:
        key = args_str.strip()
        if not key or len(key.split()) != 1:
            return "ERR usage: SHARD_REMOVE_PHYSICAL <key>"
        key_lock = self._get_key_lock(key)
        with key_lock:
            with self._structure_lock:
                if key in self._data:
                    del self._data[key]
                    return "OK removed"
                return "OK absent"

    def _handle_cleanup_tombstones(self, args_str: str) -> str:
        if args_str:
            return "ERR usage: SHARD_CLEANUP_TOMBSTONES"
        removed = 0
        with self._structure_lock:
            tombstoned_keys = [k for k, v in self._data.items() if v.value == TOMBSTONE]
            for k in tombstoned_keys:
                del self._data[k]
                removed += 1
        return f"OK removed={removed}"


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Avvia uno ShardNode standalone")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    store = ShardStore()
    server = LineTCPServer(args.host, args.port, store.handle_line)
    print(f"ShardNode in ascolto su {args.host}:{args.port}")
    server.serve_forever()


if __name__ == "__main__":
    main()