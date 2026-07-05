import sys
import time
import socket
import threading
from server import KVStore, TCPKVServer


def run_client_command(host: str, port: int, command: str) -> str:
    try:
        with socket.create_connection((host, port), timeout=2.0) as sock:
            rfile = sock.makefile('r', encoding='utf-8')
            wfile = sock.makefile('w', encoding='utf-8')
            wfile.write(command + "\n")
            wfile.flush()
            response = rfile.readline().strip()
            return response
    except Exception as e:
        return f"ERR_CONNECTION_FAILED: {e}"


def main():
    host = "127.0.0.1"
    port = 6380  # use a distinct port to avoid conflicts

    print("============================================================")
    print("AVVIO CONCURRENCY & STRESS TEST (TCP REAL SOCKETS)")
    print("============================================================")

    # 1. Start the server in a background thread
    store = KVStore()
    server = TCPKVServer(host, port, store)

    # Starting server in daemon thread so it doesn't block the script
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    # Give the socket a moment to bind and listen
    time.sleep(0.1)

    # Verify the server is responding
    ping_resp = run_client_command(host, port, "PING")
    if ping_resp != "OK PONG":
        print(f"Errore: Il server non risponde al PING. Risposta: {ping_resp}")
        sys.exit(1)

    print("\n[Stress Test 1] Idempotenza Concorrente sullo stesso request_id...")
    # Send 10 concurrent requests with the same request_id
    threads = []
    results = [None] * 10

    def worker(index):
        results[index] = run_client_command(host, port, "SET_REQ clientStress:1 key_stress valore_stress")

    for i in range(10):
        t = threading.Thread(target=worker, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("Risposte ricevute dai 10 thread concorrenti:")
    for i, res in enumerate(results):
        print(f"  Thread {i}: {res}")

    # Check final state of the key
    final_val = run_client_command(host, port, "GETV key_stress")
    print(f"Stato finale della chiave 'key_stress': {final_val}")

    # S1 assertion: version should be 0 because all requests had same request_id
    if final_val == "OK version=0 valore_stress" and all(r == "OK version=0" for r in results):
        print("-> [TEST 1 PASSED]: Idempotenza perfetta garantita sotto race condition reali.")
    else:
        print("-> [TEST 1 FAILED]: Violazione dell'idempotenza concorrente!")
        sys.exit(1)

    print("\n[Stress Test 2] Parallelismo di 10 client differenti su chiavi differenti...")
    # Send requests to different keys
    threads = []
    results_2 = [None] * 10

    def worker_2(index):
        results_2[index] = run_client_command(host, port, f"SET_REQ client_{index}:1 key_{index} val_{index}")

    for i in range(10):
        t = threading.Thread(target=worker_2, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("Risposte ricevute da client diversi:")
    for i, res in enumerate(results_2):
        print(f"  client_{i}: {res}")

    if all(r == "OK version=0" for r in results_2):
        print("-> [TEST 2 PASSED]: Nessuna contesa bloccante tra client diversi grazie al lock a due livelli.")
    else:
        print("-> [TEST 2 FAILED]: Errore nel parallelismo!")
        sys.exit(1)

    print("\n[Stress Test 3] 15 client concorrenti sulla STESSA chiave 'shared_key'...")
    # 15 concurrent different clients writing sequentially to the same key
    threads = []
    results_3 = [None] * 15

    def worker_3(index):
        results_3[index] = run_client_command(host, port, f"SET_REQ client_shared_{index}:1 shared_key valore_{index}")

    for i in range(15):
        t = threading.Thread(target=worker_3, args=(i,))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    final_shared = run_client_command(host, port, "GETV shared_key")
    print(f"Stato finale di 'shared_key': {final_shared}")

    if "OK version=14" in final_shared:
        print(
            "-> [TEST 3 PASSED]: Versione finale corretta e nessun dato corrotto su accessi concorrenti ad una sola chiave.")
    else:
        print("-> [TEST 3 FAILED]: Stato della chiave condivisa inconsistente!")
        sys.exit(1)

    print("\n============================================================")
    print("CONCURRENCY & STRESS TEST COMPLETATO CON SUCCESSO")
    print("============================================================")

    # Shut down server cleanly
    server.shutdown()


if __name__ == "__main__":
    main()
