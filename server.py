import threading
import socket
from dataclasses import dataclass


@dataclass
class ValueRecord:
    """Rappresenta il valore memorizzato nel database e la sua versione corrente."""
    value: str
    version: int


@dataclass
class RequestRecord:
    """Memorizza lo stato di una richiesta passata per garantire l'idempotenza in caso di retry."""
    payload: str
    response: str


class KVStore:
    """
    Core del Key-Value Store in-memory, thread-safe.
    Supporta operazioni concorrenti e garantisce l'idempotenza delle operazioni mutative
    tramite un sistema di Request ID basato su (client_id, sequence_number).
    """

    def __init__(self, request_window_size: int = 100) -> None:
        if request_window_size < 1:
            raise ValueError("request_window_size must be >= 1")

        # Stato dell'applicazione
        self._data: dict[str, ValueRecord] = {}

        # Cache per l'idempotenza: mappa client_id -> (sequence_number -> RequestRecord)
        self._requests: dict[str, dict[int, RequestRecord]] = {}

        # Tracking per l'eviction: l'ultimo sequence_number eliminato per ogni client
        self._evicted_until: dict[str, int] = {}
        self._window = request_window_size

        # --- GESTIONE DEI LOCK (Concurrency Control) ---
        # Usiamo una strategia di locking a due livelli per massimizzare la liveness (parallelismo).

        # 1. Lock Globale (Meta-lock): Usato solo per proteggere i dizionari che contengono i lock stessi.
        self._meta_lock = threading.Lock()

        # 2. Lock a Grana Fine: Un lock per ogni specifico client e uno per ogni specifica chiave.
        # Questo permette a thread che operano su chiavi/client diversi di non bloccarsi a vicenda.
        self._client_locks: dict[str, threading.Lock] = {}
        self._key_locks: dict[str, threading.Lock] = {}

        # 3. Lock Strutturale: Usato per operazioni che alterano la dimensione/struttura di _data (es. DELETE, KEYS, STATS)
        self._store_structure_lock = threading.Lock()

    def _get_client_lock(self, client_id: str) -> threading.Lock:
        """Restituisce (o crea lazy) il lock associato a uno specifico client."""
        with self._meta_lock:
            if client_id not in self._client_locks:
                self._client_locks[client_id] = threading.Lock()
            return self._client_locks[client_id]

    def _get_key_lock(self, key: str) -> threading.Lock:
        """Restituisce (o crea lazy) il lock associato a una specifica chiave."""
        with self._meta_lock:
            if key not in self._key_locks:
                self._key_locks[key] = threading.Lock()
            return self._key_locks[key]

    def handle_line(self, line: str) -> str:
        """
        Processa un singolo comando stringa e restituisce la risposta.
        Tutta la logica di business transazionale si trova qui.
        """
        # Pre-validazione veloce fuori dai lock per massimizzare le prestazioni.
        stripped = line.strip()
        if not stripped:
            return "ERR empty_command"

        parts = stripped.split(maxsplit=1)
        cmd = parts[0].upper()
        args_str = parts[1] if len(parts) > 1 else ""

        # ==========================================
        # COMANDI DI LETTURA (Non mutativi)
        # ==========================================
        if cmd == "PING":
            if args_str:
                return "ERR usage: PING"
            return "OK PONG"

        elif cmd == "GET":
            if not args_str or len(args_str.split()) != 1:
                return "ERR usage: GET <key>"
            key = args_str.strip()
            key_lock = self._get_key_lock(key)

            # Acquisiamo il lock specifico della chiave in lettura per evitare dati parziali
            with key_lock:
                record = self._data.get(key)
                if record is None:
                    return "NOT_FOUND"
                return f"OK {record.value}"

        elif cmd == "GETV":
            if not args_str or len(args_str.split()) != 1:
                return "ERR usage: GETV <key>"
            key = args_str.strip()
            key_lock = self._get_key_lock(key)

            with key_lock:
                record = self._data.get(key)
                if record is None:
                    return "NOT_FOUND"
                return f"OK version={record.version} {record.value}"

        elif cmd == "EXISTS":
            if not args_str or len(args_str.split()) != 1:
                return "ERR usage: EXISTS <key>"
            key = args_str.strip()
            key_lock = self._get_key_lock(key)

            with key_lock:
                exists = key in self._data
                return f"OK {1 if exists else 0}"

        elif cmd == "KEYS":
            if args_str:
                return "ERR usage: KEYS"
            # KEYS richiede una iterazione su tutto il dizionario, blocchiamo la struttura
            with self._store_structure_lock:
                if not self._data:
                    return "OK"
                sorted_keys = sorted(self._data.keys())
                return f"OK {' '.join(sorted_keys)}"

        elif cmd == "STATS":
            if args_str:
                return "ERR usage: STATS"
            with self._store_structure_lock:
                keys_count = len(self._data)
            with self._meta_lock:
                clients_count = len(self._requests)
                cached_requests_count = sum(len(self._requests[c]) for c in self._requests)
            return f"OK keys={keys_count} clients={clients_count} cached_requests={cached_requests_count} window_size={self._window}"

        elif cmd == "QUIT":
            return "OK BYE"

        # ==========================================
        # COMANDI MUTATIVI (Richiedono Idempotenza)
        # ==========================================
        elif cmd in ("SET_REQ", "CAS_REQ", "DELETE_REQ"):

            # 1. Parsing Sintattico specifico per comando
            if cmd == "SET_REQ":
                tokens = args_str.split(maxsplit=2)
                if len(tokens) < 3:
                    return "ERR usage: SET_REQ <request_id> <key> <value...>"
                req_id, key, value = tokens[0], tokens[1], tokens[2]
                payload_canonico = f"SET_REQ {key} {value}"

            elif cmd == "CAS_REQ":
                tokens = args_str.split(maxsplit=3)
                if len(tokens) < 4:
                    return "ERR usage: CAS_REQ <request_id> <key> <expected_version> <value...>"
                req_id, key, expected_version_str, value = tokens[0], tokens[1], tokens[2], tokens[3]
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
                req_id, key = tokens[0], tokens[1]
                payload_canonico = f"DELETE_REQ {key}"

            # 2. Validazione del Request ID
            if ":" not in req_id:
                return "ERR invalid_request_id"
            client_id, seq_str = req_id.split(":", 1)
            if not client_id:
                return "ERR invalid_request_id"
            try:
                seq = int(seq_str)
                if seq < 0:
                    raise ValueError()
            except ValueError:
                return "ERR invalid_request_id"

            # 3. Transazione Idempotente - Acquisizione lock del client
            # NOTA: Per evitare deadlock, acquisiamo SEMPRE prima il lock del client, poi quello della chiave.
            client_lock = self._get_client_lock(client_id)
            with client_lock:

                # FASE A: Verifica della finestra di Eviction (Garbage Collection check)
                # Controlliamo se la richiesta è troppo vecchia e la sua cache è già stata svuotata
                eviction_boundary = self._evicted_until.get(client_id, -1)
                if seq <= eviction_boundary:
                    return "ERR request_id_expired"

                # FASE B: Controllo dell'Idempotenza
                # Se abbiamo già visto questo (client_id, sequence_number), restituiamo la risposta salvata.
                client_requests = self._requests.setdefault(client_id, {})
                if seq in client_requests:
                    record = client_requests[seq]
                    # Prevenzione di abusi: se lo stesso ID viene riusato per un comando diverso, segnaliamo errore.
                    if record.payload != payload_canonico:
                        return "ERR request_id_conflict"
                    return record.response

                # FASE C: Esecuzione Nuova Richiesta - Acquisizione lock della chiave
                key_lock = self._get_key_lock(key)
                with key_lock:
                    if cmd == "SET_REQ":
                        with self._store_structure_lock:  # Protegge da conflitti con iterazioni strutturali
                            existing = self._data.get(key)
                            new_version = 0 if existing is None else existing.version + 1
                            self._data[key] = ValueRecord(value=value, version=new_version)
                        response = f"OK version={new_version}"

                    elif cmd == "CAS_REQ":
                        with self._store_structure_lock:
                            existing = self._data.get(key)
                            if existing is None:
                                response = "ERR not_found"
                            elif existing.version != expected_version:
                                # Fallimento del Compare-And-Swap
                                response = f"ERR version_mismatch current={existing.version}"
                            else:
                                # Successo del Compare-And-Swap
                                new_version = existing.version + 1
                                self._data[key] = ValueRecord(value=value, version=new_version)
                                response = f"OK version={new_version}"

                    elif cmd == "DELETE_REQ":
                        with self._store_structure_lock:
                            existing = self._data.get(key)
                            if existing is None:
                                response = "NOT_FOUND"
                            else:
                                del self._data[key]
                                response = "OK"

                # FASE D: Salvataggio in cache della risposta
                # Manteniamo la risposta in modo che futuri retry ricevano l'esito originale senza mutare nuovamente il dato
                client_requests[seq] = RequestRecord(payload=payload_canonico, response=response)

                # FASE E: Garbage Collection (Sliding Window)
                # Se la cache di questo client supera il limite consentito, eliminiamo la richiesta più vecchia.
                if len(client_requests) > self._window:
                    min_seq = min(client_requests.keys())
                    del client_requests[min_seq]
                    self._evicted_until[client_id] = min_seq

                return response

        # Se il loop arriva fin qui, il comando è sintatticamente inatteso
        return "ERR unknown_command"


class TCPKVServer:
    """
    Server TCP concorrente per il KVStore.
    Utilizza un thread dedicato (Thread-per-connection) per ogni client connesso.
    """

    def __init__(self, host: str, port: int, store: KVStore) -> None:
        self.host = host
        self.port = port
        self.store = store
        self._server_socket = None
        self._is_running = False

    def serve_forever(self) -> None:
        """Avvia il server per ascoltare le connessioni in ingresso in modo permanente."""
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # SO_REUSEADDR permette il riutilizzo immediato della porta dopo il riavvio del processo
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind((self.host, self.port))

        # Dimensione della coda dei client in attesa
        self._server_socket.listen(128)
        self._is_running = True

        try:
            while self._is_running:
                try:
                    # Bloccante finché non arriva un client, o finché shutdown() non chiude il socket
                    client_sock, _ = self._server_socket.accept()
                except OSError:
                    # In caso di shutdown() invocato esternamente, usciamo fluidamente dal ciclo while.
                    break

                # Deleghiamo la gestione di questo specifico client a un thread in background (daemon).
                t = threading.Thread(target=self._handle_client, args=(client_sock,), daemon=True)
                t.start()
        finally:
            if self._server_socket:
                try:
                    self._server_socket.close()
                except OSError:
                    pass

    def _handle_client(self, client_sock: socket.socket) -> None:
        """Loop di lettura delle richieste e invio delle risposte per un singolo client."""
        try:
            # .makefile trasforma il raw socket TCP in un file-like object per facilitare la lettura per riga
            reader = client_sock.makefile('r', encoding='utf-8')
            writer = client_sock.makefile('w', encoding='utf-8')

            for line in reader:
                # Passiamo l'esecuzione della logica al nostro Store
                response = self.store.handle_line(line)

                # Scrittura e flushing istantaneo per reattività
                writer.write(response + "\n")
                writer.flush()

                # Gestione disconnessione graziosa
                if response == "OK BYE":
                    break
        except Exception:
            # Ignoriamo le ConnectionResetError dovute a chiusure improvvise dei client (broken pipe)
            pass
        finally:
            try:
                client_sock.close()
            except OSError:
                pass

    def shutdown(self) -> None:
        """
        Interrompe il server fluidamente.
        Chiudendo il socket server si sblocca l'operazione .accept() in attesa nel ciclo principale.
        """
        self._is_running = False
        if self._server_socket:
            try:
                self._server_socket.close()
            except OSError:
                pass