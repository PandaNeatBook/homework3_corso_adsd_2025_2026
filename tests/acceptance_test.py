#!/usr/bin/env python3
"""
acceptance_test.py

Test automatici per il Router con retry idempotenti.

Questi test verificano il contratto principale del Router:
- una richiesta mutativa con request_id viene applicata al massimo una volta;
- un retry identico riceve la stessa risposta;
- lo stesso request_id con payload diverso viene rifiutato;
- un request_id evictato non viene rieseguito;
- il Router usa la logica Tombstone per i DELETE_REQ, nascondendo i dati.
- le risposte rispettano il nuovo formato (es. ERR_NOT_FOUND, ERR_CAS_CONFLICT).

I test usano una sottoclasse MockedRouter che intercetta i metodi di rete,
rendendo il test rapido, deterministico e indipendente dai socket.
"""

from __future__ import annotations

from src.protocol_common import ShardRef
from src.router import Router


class MockedRouter(Router):
    """
    Versione isolata del Router che simula il Data Plane (ShardNode)
    in memoria, scavalcando le chiamate di rete TCP per un testing ultra-rapido.
    """

    def __init__(self, request_window_size: int = 100) -> None:
        super().__init__("127.0.0.1", 9999, request_window_size)
        self._mock_store: dict[str, tuple[str, int]] = {}
        # Topologia fittizia per non far fallire l'hashing di routing
        self._active_topology = [ShardRef("dummy-shard", "127.0.0.1", 9100)]

    def _shard_get(self, shard: ShardRef, key: str) -> tuple[bool, str, int]:
        if key in self._mock_store:
            return True, self._mock_store[key][0], self._mock_store[key][1]
        return False, "", -1

    def _shard_set(self, shard: ShardRef, key: str, version: int, value: str) -> None:
        self._mock_store[key] = (value, version)

    def _shard_get_all(self, shard: ShardRef) -> dict[str, tuple[str, int]]:
        return self._mock_store


def assert_eq(actual: str, expected: str) -> None:
    if actual != expected:
        raise AssertionError(f"expected {expected!r}, got {actual!r}")


def test_ping() -> None:
    router = MockedRouter()
    assert_eq(router.handle_line("PING"), "OK PONG")


def test_set_retry_does_not_increment_version() -> None:
    router = MockedRouter()

    assert_eq(
        router.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    assert_eq(
        router.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    assert_eq(
        router.handle_line("GETV course"),
        "0",
    )


def test_new_set_with_new_request_id_increments_version() -> None:
    router = MockedRouter()

    assert_eq(
        router.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    assert_eq(
        router.handle_line("SET_REQ clientA:1 course systems"),
        "OK version=1",
    )

    assert_eq(
        router.handle_line("GETV course"),
        "1",
    )

    assert_eq(
        router.handle_line("GET course"),
        "systems 1",
    )


def test_cas_retry_success_is_not_applied_twice() -> None:
    router = MockedRouter()

    assert_eq(
        router.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    assert_eq(
        router.handle_line("CAS_REQ clientA:1 course 0 distributed-systems"),
        "OK version=1",
    )

    assert_eq(
        router.handle_line("CAS_REQ clientA:1 course 0 distributed-systems"),
        "OK version=1",
    )

    assert_eq(
        router.handle_line("GET course"),
        "distributed-systems 1",
    )


def test_cas_retry_failure_replays_same_error() -> None:
    router = MockedRouter()

    assert_eq(
        router.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    assert_eq(
        router.handle_line("SET_REQ clientA:1 course systems"),
        "OK version=1",
    )

    assert_eq(
        router.handle_line("CAS_REQ clientA:2 course 0 networks"),
        "ERR_CAS_CONFLICT current=1",
    )

    # Retry identico: deve riprodurre lo stesso errore applicativo.
    assert_eq(
        router.handle_line("CAS_REQ clientA:2 course 0 networks"),
        "ERR_CAS_CONFLICT current=1",
    )

    assert_eq(
        router.handle_line("GET course"),
        "systems 1",
    )


def test_request_id_conflict_is_detected() -> None:
    router = MockedRouter()

    assert_eq(
        router.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    # Stesso request_id, payload diverso: errore.
    assert_eq(
        router.handle_line("SET_REQ clientA:0 course systems"),
        "ERR_REQUEST_ID_CONFLICT",
    )

    # Lo stato non deve essere cambiato.
    assert_eq(
        router.handle_line("GET course"),
        "ads 0",
    )


def test_delete_retry_replays_original_success() -> None:
    router = MockedRouter()

    assert_eq(
        router.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    assert_eq(
        router.handle_line("DELETE_REQ clientA:1 course"),
        "OK",
    )

    # Retry identico: non deve rieseguire il delete (consumando un'altra versione globale).
    # Deve riprodurre la vecchia risposta OK.
    assert_eq(
        router.handle_line("DELETE_REQ clientA:1 course"),
        "OK",
    )

    assert_eq(
        router.handle_line("GET course"),
        "ERR_NOT_FOUND",
    )


def test_delete_not_found_is_also_replayed() -> None:
    router = MockedRouter()

    assert_eq(
        router.handle_line("DELETE_REQ clientA:0 missing"),
        "ERR_NOT_FOUND",
    )

    assert_eq(
        router.handle_line("DELETE_REQ clientA:0 missing"),
        "ERR_NOT_FOUND",
    )


def test_two_clients_same_sequence_number_are_not_confused() -> None:
    router = MockedRouter()

    assert_eq(
        router.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    # Stesso seq, ma client diverso: richiesta diversa (versione globale = 1).
    assert_eq(
        router.handle_line("SET_REQ clientB:0 course systems"),
        "OK version=1",
    )

    assert_eq(
        router.handle_line("GET course"),
        "systems 1",
    )

    # Retry clientA:0 deve riprodurre la vecchia risposta di clientA.
    assert_eq(
        router.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    # Ma lo stato corrente resta quello prodotto da clientB.
    assert_eq(
        router.handle_line("GET course"),
        "systems 1",
    )


def test_evicted_request_id_is_not_reexecuted() -> None:
    router = MockedRouter(request_window_size=2)

    assert_eq(
        router.handle_line("SET_REQ clientA:0 k v0"),
        "OK version=0",
    )

    assert_eq(
        router.handle_line("SET_REQ clientA:1 k v1"),
        "OK version=1",
    )

    # Questa terza richiesta forza l'eviction di clientA:0.
    assert_eq(
        router.handle_line("SET_REQ clientA:2 k v2"),
        "OK version=2",
    )

    # clientA:0 è troppo vecchio: il Router non deve rieseguirlo.
    assert_eq(
        router.handle_line("SET_REQ clientA:0 k v0"),
        "ERR_REQUEST_ID_EXPIRED",
    )

    # Lo stato resta quello più recente.
    assert_eq(
        router.handle_line("GET k"),
        "v2 2",
    )


def test_malformed_commands() -> None:
    router = MockedRouter()

    assert_eq(router.handle_line(""), "ERR empty_command")
    assert_eq(router.handle_line("UNKNOWN"), "ERR unknown_command")
    assert_eq(router.handle_line("SET_REQ clientA:0 onlykey"), "ERR usage: SET_REQ <request_id> <key> <value...>")
    assert_eq(router.handle_line("CAS_REQ clientA:0 k nope value"), "ERR bad_version")
    assert_eq(router.handle_line("SET_REQ badrequestid k value"), "ERR_INVALID_REQUEST_ID")


def run_all_tests() -> None:
    tests = [
        test_ping,
        test_set_retry_does_not_increment_version,
        test_new_set_with_new_request_id_increments_version,
        test_cas_retry_success_is_not_applied_twice,
        test_cas_retry_failure_replays_same_error,
        test_request_id_conflict_is_detected,
        test_delete_retry_replays_original_success,
        test_delete_not_found_is_also_replayed,
        test_two_clients_same_sequence_number_are_not_confused,
        test_evicted_request_id_is_not_reexecuted,
        test_malformed_commands,
    ]

    passed = 0

    for test in tests:
        test()
        passed += 1
        print(f"PASS {test.__name__}")

    print(f"\nAll tests passed: {passed}/{len(tests)}")


if __name__ == "__main__":
    run_all_tests()