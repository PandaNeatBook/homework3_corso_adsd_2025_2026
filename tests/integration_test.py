from __future__ import annotations

import threading
import time
import pytest

from coordinator import Coordinator
from protocol_common import LineTCPServer, send_line
from router import Router
from shard_node import ShardStore

HOST = "127.0.0.1"


# --- HELPER FUNCTIONS ---

def start_shard(port: int) -> None:
    store = ShardStore()
    server = LineTCPServer(HOST, port, store.handle_line)
    threading.Thread(target=server.serve_forever, daemon=True).start()


def rpc(port: int, line: str) -> str:
    return send_line(HOST, port, line)


def wait_until(predicate, timeout=10.0, interval=0.05) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


# --- FIXTURES ---

@pytest.fixture(scope="module")
def cluster_setup():
    """
    Fixture per inizializzare l'intero sistema (Shard, Coordinator, Router).
    Viene eseguita una volta per modulo.
    """
    shard_ports = {"shard-1": 9101, "shard-2": 9102, "shard-3": 9103}
    router_public_port = 9200
    router_control_port = 9201
    coordinator_port = 9300

    # Avvia i primi due shard
    start_shard(shard_ports["shard-1"])
    start_shard(shard_ports["shard-2"])

    # Lo shard 3 verrà avviato dinamicamente durante il test ma la sua porta è riservata
    start_shard(shard_ports["shard-3"])

    # Avvia Coordinator (ora supporta la Two-Phase Commit in background)
    coordinator = Coordinator(HOST, router_control_port)
    coordinator_server = LineTCPServer(HOST, coordinator_port, coordinator.handle_line)
    threading.Thread(target=coordinator_server.serve_forever, daemon=True).start()

    # Avvia Router (ora dotato di Watchdog integrato)
    router = Router(HOST, coordinator_port)
    router_control_server = LineTCPServer(HOST, router_control_port, router.handle_control_line)
    router_public_server = LineTCPServer(HOST, router_public_port, router.handle_line)
    threading.Thread(target=router_control_server.serve_forever, daemon=True).start()
    threading.Thread(target=router_public_server.serve_forever, daemon=True).start()

    # Attendi che il sistema sia su
    time.sleep(0.5)
    assert rpc(router_public_port, "PING") == "OK PONG", "Il Router deve essere raggiungibile"

    return {
        "shard_ports": shard_ports,
        "router_public": router_public_port,
        "router_control": router_control_port,
        "coordinator": coordinator_port
    }


# --- TEST CASES ---

def test_router_coordinator_shardnode_integration(cluster_setup):
    router_public_port = cluster_setup["router_public"]
    shard_ports = cluster_setup["shard_ports"]

    # ------------------------------------------------------------------
    # Fase 1: topologia iniziale a 2 shard
    # ------------------------------------------------------------------
    for shard_id, port in list(shard_ports.items())[:2]:
        resp = rpc(router_public_port, f"ADD_SHARD {shard_id} {HOST}:{port}")
        assert resp == "OK", f"ADD_SHARD {shard_id} deve essere accettato"

    resp = rpc(router_public_port, "REBALANCE")
    assert resp == "OK rebalance_scheduled", "Primo REBALANCE accettato"

    topology_active = wait_until(
        lambda: "rebalancing=0" in rpc(router_public_port, "STATS") and "shards=2" in rpc(router_public_port, "STATS")
    )
    assert topology_active, "Topologia iniziale (2 shard) non attivata in tempo"

    # ------------------------------------------------------------------
    # Fase 2: scritture idempotenti di base
    # ------------------------------------------------------------------
    n_keys = 40
    for i in range(n_keys):
        resp = rpc(router_public_port, f"SET_REQ clientA:{i} key{i} value{i}")
        # Il router incrementa _global_version per ogni chiave
        assert resp == f"OK version={i}", f"Errore scrittura: SET_REQ key{i}"

    # Retry idempotente
    resp_retry = rpc(router_public_port, "SET_REQ clientA:0 key0 value0")
    assert resp_retry == "OK version=0", "Il retry SET_REQ clientA:0 deve restituire la risposta cached"

    # Riuso dello stesso request_id con payload diverso
    resp_conflict = rpc(router_public_port, "SET_REQ clientA:0 key0 ALTRO_VALORE")
    assert resp_conflict == "ERR_REQUEST_ID_CONFLICT", "Il riuso di request_id con payload diverso deve dare conflitto"

    resp_get = rpc(router_public_port, "GET key0")
    assert resp_get == "value0 0", "GET key0 incoerente dopo le scritture"

    # ------------------------------------------------------------------
    # Fase 3: CAS_REQ
    # ------------------------------------------------------------------
    resp_cas = rpc(router_public_port, "CAS_REQ clientA:100 key0 0 value0-v2")
    assert resp_cas == "OK version=40", "CAS_REQ fallita"

    resp_cas_retry = rpc(router_public_port, "CAS_REQ clientA:100 key0 0 value0-v2")
    assert resp_cas_retry == "OK version=40", "Retry CAS_REQ non ha restituito la risposta cached"

    resp_cas_conflict = rpc(router_public_port, "CAS_REQ clientA:101 key0 0 value0-v3")
    assert resp_cas_conflict == "ERR_CAS_CONFLICT current=40", "CAS_REQ con versione sbagliata non rifiutata"

    # ------------------------------------------------------------------
    # Fase 4: rebalance verso 3 shard, con verifica di lettura continua
    # ------------------------------------------------------------------
    resp = rpc(router_public_port, f"ADD_SHARD shard-3 {HOST}:{shard_ports['shard-3']}")
    assert resp == "OK", "ADD_SHARD shard-3 fallito"

    resp = rpc(router_public_port, "REBALANCE")
    assert resp == "OK rebalance_scheduled", "REBALANCE verso 3 shard fallito"

    is_rebalancing = wait_until(lambda: "rebalancing=1" in rpc(router_public_port, "STATS"))
    assert is_rebalancing, "Il Router non è entrato in modalità rebalancing"

    # 1. TEST CAS IMMEDIATO: facciamo la verifica prima che il Coordinator finisca!
    resp_cas_during = rpc(router_public_port, "CAS_REQ clientA:200 key1 1 value1-v2")
    assert resp_cas_during == "ERR_REBALANCING", "CAS_REQ non bloccata durante il rebalance"

    # 2. Nuove scritture durante il rebalance
    resp_new_write = rpc(router_public_port, "SET_REQ clientB:0 new_key_during_rebalance hello")
    assert resp_new_write == "OK version=41", "Scrittura durante il rebalance fallita"

    # 3. Letture durante il rebalance (fallback su N garantito e protetto dal Watchdog)
    # Questo ciclo dà tempo al Coordinator di finire la migrazione
    for i in range(n_keys):
        expected = "value0-v2 40" if i == 0 else f"value{i} {i}"
        got = rpc(router_public_port, f"GET key{i}")
        assert got == expected, f"Lettura inattesa durante il rebalance: key{i} -> {got} (atteso {expected})"

    resp_new_read = rpc(router_public_port, "GET new_key_during_rebalance")
    assert resp_new_read == "hello 41", "La nuova chiave non è leggibile subito"

    # 4. Attesa fine rebalance
    rebalance_done = wait_until(
        lambda: "rebalancing=0" in rpc(router_public_port, "STATS") and "shards=3" in rpc(router_public_port, "STATS"),
        timeout=10.0
    )
    assert rebalance_done, "Rebalance non completato in tempo utile"

    # ------------------------------------------------------------------
    # Fase 5: DELETE_REQ, tombstone e cleanup
    # ------------------------------------------------------------------
    resp_delete = rpc(router_public_port, "DELETE_REQ clientA:300 key2")
    assert resp_delete == "OK", "DELETE_REQ su chiave esistente fallita"

    # Nota: la DELETE usa internamente una SET di tombstone, consumando la version=43.

    resp_get_deleted = rpc(router_public_port, "GET key2")
    assert resp_get_deleted == "ERR_NOT_FOUND", "La GET su una chiave cancellata deve dare ERR_NOT_FOUND"

    resp_delete_retry = rpc(router_public_port, "DELETE_REQ clientA:300 key2")
    assert resp_delete_retry == "OK", "Retry DELETE_REQ fallito"

    resp_delete_missing = rpc(router_public_port, "DELETE_REQ clientA:301 key_che_non_esiste")
    assert resp_delete_missing == "ERR_NOT_FOUND", "DELETE_REQ su chiave inesistente deve dare ERR_NOT_FOUND"

    resp_keys = rpc(router_public_port, "KEYS").split()
    assert "key2" not in resp_keys, "Il comando KEYS include chiavi cancellate (tombstone)"
    assert "key3" in resp_keys, "Il comando KEYS omette chiavi esistenti"

    # ------------------------------------------------------------------
    # Fase 6: idempotenza a cavallo di un secondo rebalance
    # ------------------------------------------------------------------
    resp = rpc(router_public_port, f"REMOVE_SHARD shard-1 {HOST}:{shard_ports['shard-1']}")
    assert resp == "OK", "REMOVE_SHARD shard-1 fallito"

    resp = rpc(router_public_port, "REBALANCE")
    assert resp == "OK rebalance_scheduled", "REBALANCE per rimozione nodo fallito"

    second_rebalance_done = wait_until(
        lambda: "rebalancing=0" in rpc(router_public_port, "STATS") and "shards=2" in rpc(router_public_port, "STATS"),
        timeout=10.0
    )
    assert second_rebalance_done, "Rebalance di rimozione non completato in tempo utile"

    resp_retry_after_second_rebalance = rpc(router_public_port, "SET_REQ clientA:0 key0 value0")
    assert resp_retry_after_second_rebalance == "OK version=0", "Idempotenza rotta dopo il secondo rebalance"

    resp_get_after = rpc(router_public_port, "GET key0")
    assert resp_get_after == "value0-v2 40", "Dati persi/corrotti dopo il secondo rebalance"


if __name__ == "__main__":
    pytest.main(["-v", __file__])