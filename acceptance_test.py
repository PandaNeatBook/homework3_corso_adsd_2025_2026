#!/usr/bin/env python3
"""
acceptance_test.py

Test automatici per il KV store con retry idempotenti.

Questi test verificano il contratto principale del progetto:

- una richiesta mutativa con request_id viene applicata al massimo una volta;
- un retry identico riceve la stessa risposta;
- lo stesso request_id con payload diverso viene rifiutato;
- un request_id troppo vecchio, già uscito dalla finestra, non viene rieseguito;
- due client diversi non vengono confusi anche se usano lo stesso sequence number.

I test usano direttamente KVStore.handle_line().
Questo rende il test rapido, deterministico e indipendente da timing di rete.
Il protocollo TCP viene comunque coperto dal client/server manuale.
"""

from __future__ import annotations

from server import KVStore


def assert_eq(actual: str, expected: str) -> None:
    if actual != expected:
        raise AssertionError(f"expected {expected!r}, got {actual!r}")


def test_ping() -> None:
    store = KVStore()

    assert_eq(store.handle_line("PING"), "PONG")


def test_set_retry_does_not_increment_version() -> None:
    store = KVStore()

    assert_eq(
        store.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    assert_eq(
        store.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    assert_eq(
        store.handle_line("GETV course"),
        "OK version=0 ads",
    )


def test_new_set_with_new_request_id_increments_version() -> None:
    store = KVStore()

    assert_eq(
        store.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    assert_eq(
        store.handle_line("SET_REQ clientA:1 course systems"),
        "OK version=1",
    )

    assert_eq(
        store.handle_line("GETV course"),
        "OK version=1 systems",
    )


def test_cas_retry_success_is_not_applied_twice() -> None:
    store = KVStore()

    assert_eq(
        store.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    assert_eq(
        store.handle_line("CAS_REQ clientA:1 course 0 distributed-systems"),
        "OK version=1",
    )

    assert_eq(
        store.handle_line("CAS_REQ clientA:1 course 0 distributed-systems"),
        "OK version=1",
    )

    assert_eq(
        store.handle_line("GETV course"),
        "OK version=1 distributed-systems",
    )


def test_cas_retry_failure_replays_same_error() -> None:
    store = KVStore()

    assert_eq(
        store.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    assert_eq(
        store.handle_line("SET_REQ clientA:1 course systems"),
        "OK version=1",
    )

    assert_eq(
        store.handle_line("CAS_REQ clientA:2 course 0 networks"),
        "ERR version_mismatch current=1",
    )

    # Retry identico: deve riprodurre lo stesso errore.
    assert_eq(
        store.handle_line("CAS_REQ clientA:2 course 0 networks"),
        "ERR version_mismatch current=1",
    )

    assert_eq(
        store.handle_line("GETV course"),
        "OK version=1 systems",
    )


def test_request_id_conflict_is_detected() -> None:
    store = KVStore()

    assert_eq(
        store.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    # Stesso request_id, payload diverso: errore.
    assert_eq(
        store.handle_line("SET_REQ clientA:0 course systems"),
        "ERR request_id_conflict",
    )

    # Lo stato non deve essere cambiato.
    assert_eq(
        store.handle_line("GETV course"),
        "OK version=0 ads",
    )


def test_delete_retry_replays_original_success() -> None:
    store = KVStore()

    assert_eq(
        store.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    assert_eq(
        store.handle_line("DELETE_REQ clientA:1 course"),
        "OK deleted=true",
    )

    # Retry identico: non deve rieseguire il delete.
    # Deve riprodurre la vecchia risposta OK.
    assert_eq(
        store.handle_line("DELETE_REQ clientA:1 course"),
        "OK deleted=true",
    )

    assert_eq(
        store.handle_line("GETV course"),
        "NOT_FOUND",
    )


def test_delete_not_found_is_also_replayed() -> None:
    store = KVStore()

    assert_eq(
        store.handle_line("DELETE_REQ clientA:0 missing"),
        "NOT_FOUND",
    )

    assert_eq(
        store.handle_line("DELETE_REQ clientA:0 missing"),
        "NOT_FOUND",
    )


def test_two_clients_same_sequence_number_are_not_confused() -> None:
    store = KVStore()

    assert_eq(
        store.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    # Stesso seq, ma client diverso: richiesta diversa.
    assert_eq(
        store.handle_line("SET_REQ clientB:0 course systems"),
        "OK version=1",
    )

    assert_eq(
        store.handle_line("GETV course"),
        "OK version=1 systems",
    )

    # Retry clientA:0 deve riprodurre la vecchia risposta di clientA.
    assert_eq(
        store.handle_line("SET_REQ clientA:0 course ads"),
        "OK version=0",
    )

    # Ma lo stato corrente resta quello prodotto da clientB.
    assert_eq(
        store.handle_line("GETV course"),
        "OK version=1 systems",
    )


def test_evicted_request_id_is_not_reexecuted() -> None:
    store = KVStore(request_window_size=2)

    assert_eq(
        store.handle_line("SET_REQ clientA:0 k v0"),
        "OK version=0",
    )

    assert_eq(
        store.handle_line("SET_REQ clientA:1 k v1"),
        "OK version=1",
    )

    # Questa terza richiesta forza l'eviction di clientA:0.
    assert_eq(
        store.handle_line("SET_REQ clientA:2 k v2"),
        "OK version=2",
    )

    # clientA:0 è troppo vecchio: il server non deve rieseguirlo.
    assert_eq(
        store.handle_line("SET_REQ clientA:0 k v0"),
        "ERR request_id_expired",
    )

    # Lo stato resta quello più recente.
    assert_eq(
        store.handle_line("GETV k"),
        "OK version=2 v2",
    )


def test_malformed_commands() -> None:
    store = KVStore()

    assert_eq(store.handle_line(""), "ERR empty_command")
    assert_eq(store.handle_line("UNKNOWN"), "ERR unknown_command")
    assert_eq(store.handle_line("SET_REQ clientA:0 onlykey"), "ERR malformed")
    assert_eq(store.handle_line("CAS_REQ clientA:0 k nope value"), "ERR bad_version")
    assert_eq(store.handle_line("SET_REQ badrequestid k value"), "ERR bad_request_id")


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