"""
Utilita' condivise dal sistema distribuito Router / Coordinator / ShardNode.

Contiene:
- il valore sentinella di tombstone;
- la funzione di hashing deterministico usata per calcolare lo shard di una chiave;
- una piccola utility per aprire una connessione TCP, inviare una riga e leggere la risposta;
- un server TCP generico "una riga -> una risposta", riusato da tutti i componenti;
- la rappresentazione di uno ShardRef (id, host, porta) e la sua (de)serializzazione,
  usata per scambiarsi le topologie tra Router e Coordinator.
"""
from __future__ import annotations

import json
import socket
import threading
import zlib
from dataclasses import dataclass

TOMBSTONE = "<TOMBSTONE>"


@dataclass(frozen=True)
class ShardRef:
    shard_id: str
    host: str
    port: int

    def to_tuple(self) -> list:
        return [self.shard_id, self.host, self.port]

    @staticmethod
    def from_tuple(t) -> "ShardRef":
        shard_id, host, port = t
        return ShardRef(shard_id=shard_id, host=host, port=int(port))


def encode_topology(topology: list[ShardRef]) -> str:
    """Serializza una topologia (ordinata per shard_id) in una singola riga JSON compatta (senza spazi)."""
    ordered = sorted(topology, key=lambda s: s.shard_id)
    # Aggiungiamo separators=(',', ':') per evitare che json.dumps inserisca
    # spazi dopo le virgole, che romperebbero il parsing con split() nel Coordinator.
    return json.dumps([s.to_tuple() for s in ordered], separators=(',', ':'))


def decode_topology(raw: str) -> list[ShardRef]:
    data = json.loads(raw)
    return [ShardRef.from_tuple(t) for t in data]


def shard_index_for_key(key: str, topology_size: int) -> int:
    """
    Calcola l'indice di shard per una chiave, data la dimensione della topologia.

    NOTA DI DESIGN: usiamo un semplice hash(key) % N invece di un consistent
    hashing "a minima ridistribuzione". Questo significa che un cambio di
    topologia puo' rimescolare una frazione consistente delle chiavi tra gli
    shard, non solo quelle strettamente necessarie. E' una semplificazione
    accettabile per questo esercizio: la correttezza della migrazione (nessun
    dato perso, nessuna resurrezione di dati cancellati) non dipende dalla
    percentuale di chiavi che si spostano, solo dal fatto che il Coordinator
    migri effettivamente TUTTE le chiavi il cui shard di destinazione cambia.
    """
    if topology_size <= 0:
        raise ValueError("empty topology")
    digest = zlib.crc32(key.encode("utf-8"))
    return digest % topology_size


def shard_for_key(key: str, topology: list[ShardRef]) -> ShardRef:
    """Restituisce lo ShardRef responsabile di key, data una topologia (ordinata per id)."""
    ordered = sorted(topology, key=lambda s: s.shard_id)
    idx = shard_index_for_key(key, len(ordered))
    return ordered[idx]


class ProtocolError(Exception):
    pass


def send_line(host: str, port: int, line: str, timeout: float = 5.0) -> str:
    """
    Apre una connessione TCP, invia una singola riga e legge una singola riga
    di risposta. Usato da Router e Coordinator per parlare con gli ShardNode e
    tra loro (control plane). E' deliberatamente "una connessione per comando":
    semplice e senza stato, coerente con la natura "stupida" dei componenti a
    valle (ShardNode) descritta nel contratto di rebalancing.
    """
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.sendall((line + "\n").encode("utf-8"))
        reader = sock.makefile("r", encoding="utf-8", newline="\n")
        response = reader.readline()
        if response == "":
            raise ProtocolError(f"connessione chiusa da {host}:{port} senza risposta")
        return response.rstrip("\r\n")


class LineTCPServer:
    """
    Server TCP generico "una riga in ingresso -> una riga di risposta", con un
    thread dedicato per ogni connessione. Riusato da ShardNode, Coordinator
    (control plane) e Router (sia lato client pubblico che lato controllo).
    """

    def __init__(self, host: str, port: int, handler) -> None:
        self.host = host
        self.port = port
        self._handler = handler
        self._server_socket: socket.socket | None = None
        self._is_running = False

    def serve_forever(self) -> None:
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(128)
        self._is_running = True
        try:
            while self._is_running:
                try:
                    client_sock, _ = self._server_socket.accept()
                except OSError:
                    break
                threading.Thread(target=self._handle_client, args=(client_sock,), daemon=True).start()
        finally:
            if self._server_socket:
                try:
                    self._server_socket.close()
                except OSError:
                    pass

    def _handle_client(self, client_sock: socket.socket) -> None:
        try:
            reader = client_sock.makefile("r", encoding="utf-8")
            writer = client_sock.makefile("w", encoding="utf-8")
            for line in reader:
                try:
                    response = self._handler(line)
                except Exception as exc:  # noqa: BLE001 - non vogliamo mai crashare il server
                    response = f"ERR internal: {exc}"
                writer.write(response + "\n")
                writer.flush()
                if response == "OK BYE":
                    break
        except Exception:
            pass
        finally:
            try:
                client_sock.close()
            except OSError:
                pass

    def shutdown(self) -> None:
        self._is_running = False
        if self._server_socket:
            try:
                self._server_socket.close()
            except OSError:
                pass