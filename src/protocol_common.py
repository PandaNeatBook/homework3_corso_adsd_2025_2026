"""
Utilita' condivise dal sistema distribuito Router / Coordinator / ShardNode.

Questo modulo centralizza le logiche "core" che devono essere assolutamente
identiche in tutti i nodi del sistema per evitare inconsistenze:
1. Il valore di Tombstone (essenziale per la correttezza del Rebalancing).
2. L'algoritmo di Hashing (Router e Coordinator devono concordare sempre
   su dove si trovi una determinata chiave).
3. Le primitive di Rete (Server e Client TCP) standardizzando il protocollo
   "Line-based" (una riga di testo per comando, una riga per risposta).
"""
from __future__ import annotations

import json
import socket
import threading
import zlib
from dataclasses import dataclass

# Valore sentinella usato per indicare che una chiave è stata cancellata logicamente.
# Perché serve? Se eliminassimo fisicamente la chiave durante un rebalance, una
# lettura farebbe "fallback" sulla vecchia topologia, trovando il vecchio valore e
# resuscitando un "dato zombie". Scrivendo il TOMBSTONE, il Router capisce
# esplicitamente che la chiave è morta.
TOMBSTONE = "<TOMBSTONE>"


@dataclass(frozen=True)
class ShardRef:
    """
    Rappresenta le coordinate di rete di un singolo ShardNode.
    Essendo frozen=True, è immutabile e può essere usato in contesti sicuri
    o come chiave/valore da confrontare facilmente.
    """
    shard_id: str
    host: str
    port: int

    def to_tuple(self) -> list:
        """Converte l'oggetto in una lista nativa per facilitare la serializzazione JSON."""
        return [self.shard_id, self.host, self.port]

    @staticmethod
    def from_tuple(t) -> "ShardRef":
        """Ricostruisce l'oggetto dalla lista deserializzata."""
        shard_id, host, port = t
        return ShardRef(shard_id=shard_id, host=host, port=int(port))


def encode_topology(topology: list[ShardRef]) -> str:
    """
    Serializza un'intera topologia in una singola stringa JSON.

    DETTAGLIO CRITICO SUL PROTOCOLLO:
    Il nostro protocollo TCP usa lo spazio (' ') per separare il comando dagli argomenti.
    Se json.dumps introducesse degli spazi dopo le virgole (come fa di default), il
    parsing sul nodo ricevente si romperebbe (lo split() taglierebbe il JSON a metà).
    Usiamo `separators=(',', ':')` per produrre una stringa compatta e sicura.
    Inoltre, ordiniamo sempre per shard_id per garantire che la stringa generata
    sia deterministica.
    """
    ordered = sorted(topology, key=lambda s: s.shard_id)
    return json.dumps([s.to_tuple() for s in ordered], separators=(',', ':'))


def decode_topology(raw: str) -> list[ShardRef]:
    """Deserializza una stringa compatta in una lista di ShardRef pronti all'uso."""
    data = json.loads(raw)
    return [ShardRef.from_tuple(t) for t in data]


def shard_index_for_key(key: str, topology_size: int) -> int:
    """
    Calcola l'indice matematico dello shard responsabile di una determinata chiave.

    NOTA ARCHITETTURALE (Hashing Modulare vs Consistent Hashing):
    Qui usiamo un semplice hashing modulare: hash(key) % N.
    In produzione, l'aggiunta di uno shard cambierebbe il modulo (da % N a % N+1),
    spostando quasi tutte le chiavi (effetto valanga). Si userebbe un Consistent
    Hashing (Hash Ring) per limitare gli spostamenti solo a K/N chiavi.
    Tuttavia, per gli scopi didattici di questo sistema e per verificare la
    correttezza del Rebalancing, il modulo è perfetto: garantisce che il Coordinator
    migri le chiavi in modo deterministico senza far perdere alcun dato, testando a fondo
    la robustezza del sistema sotto forte stress migratorio.
    """
    if topology_size <= 0:
        raise ValueError("empty topology")
    # Usiamo CRC32 perché produce lo STESSO valore su qualsiasi architettura/sistema
    # operativo, a differenza della funzione hash() nativa di Python che è randomizzata
    # a ogni avvio dell'interprete per motivi di sicurezza (anti-DDoS).
    digest = zlib.crc32(key.encode("utf-8"))
    return digest % topology_size


def shard_for_key(key: str, topology: list[ShardRef]) -> ShardRef:
    """
    Restituisce il nodo fisico responsabile per la chiave data.

    L'ordinamento esplicito `sorted(..., key=lambda s: s.shard_id)` è FONDAMENTALE.
    Garantisce che, anche se i nodi vengono passati in ordine diverso, l'indice
    calcolato da `shard_index_for_key` punti sempre allo stesso identico nodo,
    sia che il calcolo venga fatto dal Router, sia che venga fatto dal Coordinator.
    """
    ordered = sorted(topology, key=lambda s: s.shard_id)
    idx = shard_index_for_key(key, len(ordered))
    return ordered[idx]


class ProtocolError(Exception):
    """Eccezione custom sollevata in caso di risposte vuote o malformate dal socket."""
    pass


def send_line(host: str, port: int, line: str, timeout: float = 5.0) -> str:
    """
    Primitive di Rete (Client).
    Apre una connessione effimera (short-lived), invia il payload, legge
    esattamente una riga di risposta e chiude il socket.

    Questo modello "una connessione per comando" introduce un leggero overhead
    di handshake TCP, ma semplifica immensamente l'architettura: elimina il bisogno
    di gestire pool di connessioni, ripristini da timeout o state-machine complesse.
    Coerente con la natura "stateless" delle richieste di base.
    """
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.sendall((line + "\n").encode("utf-8"))
        # Rende il socket un file-like object per facilitare la lettura per riga
        reader = sock.makefile("r", encoding="utf-8", newline="\n")
        response = reader.readline()
        if response == "":
            raise ProtocolError(f"connessione chiusa da {host}:{port} senza risposta")
        return response.rstrip("\r\n")


class LineTCPServer:
    """
    Primitive di Rete (Server).
    Server TCP generico basato sul pattern "Thread-per-Connection".
    Riusato per istanziare i socket in ascolto del Router (sia verso client
    che verso Coordinator), del Coordinator stesso e degli ShardNode.
    """

    def __init__(self, host: str, port: int, handler) -> None:
        self.host = host
        self.port = port
        self._handler = handler # La callback contenente la logica di business (es. router.handle_line)
        self._server_socket: socket.socket | None = None
        self._is_running = False

    def serve_forever(self) -> None:
        """
        Avvia il loop di accettazione. Per ogni client che si connette,
        stacca un Thread in background (daemon) per non bloccare l'accettazione
        di nuove connessioni.
        """
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # SO_REUSEADDR permette di riavviare il server velocemente senza l'errore "Address already in use"
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))
        self._server_socket.listen(128)
        self._is_running = True

        try:
            while self._is_running:
                try:
                    client_sock, _ = self._server_socket.accept()
                except OSError:
                    # Accade tipicamente se il server viene spento o il socket chiuso
                    break

                # Delega la gestione della conversazione TCP a un nuovo thread
                threading.Thread(target=self._handle_client, args=(client_sock,), daemon=True).start()
        finally:
            if self._server_socket:
                try:
                    self._server_socket.close()
                except OSError:
                    pass

    def _handle_client(self, client_sock: socket.socket) -> None:
        """
        Ciclo di vita della singola connessione. Continua a leggere righe e a
        rispondere finché il client non chiude la connessione o invia QUIT (OK BYE).
        """
        try:
            reader = client_sock.makefile("r", encoding="utf-8")
            writer = client_sock.makefile("w", encoding="utf-8")

            for line in reader:
                try:
                    # Invocazione della logica di business passata nel costruttore
                    response = self._handler(line)
                except Exception as exc:  # noqa: BLE001
                    # Cattura qualsiasi eccezione imprevista per evitare il crash del thread
                    response = f"ERR internal: {exc}"

                # Invia la risposta e forza il flush del buffer di rete
                writer.write(response + "\n")
                writer.flush()

                if response == "OK BYE":
                    break # Uscita volontaria del client (comando QUIT)

        except Exception:
            pass # Disconnessione improvvisa del client (Broken Pipe)
        finally:
            try:
                client_sock.close() # Rilascio pulito delle risorse
            except OSError:
                pass

    def shutdown(self) -> None:
        """Spegne il server interrompendo il ciclo di accept() in modo pulito."""
        self._is_running = False
        if self._server_socket:
            try:
                self._server_socket.close()
            except OSError:
                pass