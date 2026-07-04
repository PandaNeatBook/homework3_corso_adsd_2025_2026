# Homework 3: KV Store con Retry Idempotenti

Estensione di un Key-Value Store versionato con operazioni mutative idempotenti tramite `request_id`.

Il problema affrontato è tipico dei sistemi distribuiti: un client può inviare una scrittura, perdere la risposta e non sapere se il server l'abbia già applicata. Se ritenta alla cieca, rischia di applicare due volte lo stesso effetto.

La soluzione implementata associa ogni operazione mutativa a un identificatore:

```text
<client_id>:<seq>
```

Il server mantiene una request table in memoria. Se riceve di nuovo lo stesso `request_id` con lo stesso payload, restituisce la risposta salvata senza riapplicare l'effetto.

---

## File del progetto

| File | Contenuto |
|---|---|
| `server.py` | Server TCP multithread e logica del KV store |
| `client.py` | Client interattivo con generazione di `request_id` e comando `retry` |
| `acceptance_test.py` | Test automatici del contratto di idempotenza |
| `api-contract.md` | Contratto pubblico del protocollo |
| `SAFETY_LIVENESS.md` | Proprietà di safety e liveness |
| `TECHNICAL_NOTE.md` | Scelte tecniche, limiti ed evoluzioni possibili |
| `homework_3.md` | Traccia dell'homework |

---

## Requisiti

- Python 3.9 o superiore
- Nessuna dipendenza esterna
- Solo standard library

---

## Avvio

### Server

```bash
python3 server.py
```

Default:

```text
127.0.0.1:6379
```

Con host e porta espliciti:

```bash
python3 server.py --host 127.0.0.1 --port 6379
```

### Client

In un secondo terminale:

```bash
python3 client.py --client-id clientA
```

Oppure:

```bash
python3 client.py --host 127.0.0.1 --port 6379 --client-id clientA
```

---

## Comandi del client interattivo

| Comando client | Significato |
|---|---|
| `ping` | Verifica connessione |
| `get <key>` | Legge il valore |
| `getv <key>` | Legge valore e versione |
| `exists <key>` | Verifica esistenza |
| `keys` | Mostra le chiavi |
| `stats` | Mostra statistiche server |
| `set <key> <value...>` | Invia `SET_REQ` con nuovo `request_id` |
| `cas <key> <expected_version> <value...>` | Invia `CAS_REQ` |
| `delete <key>` | Invia `DELETE_REQ` |
| `retry` | Reinvia l'ultima richiesta mutativa con lo stesso `request_id` |
| `raw <command>` | Invia un comando grezzo al server |
| `help` | Mostra l'help |
| `quit` | Chiude la connessione |

---

## Protocollo supportato

### Letture

Le letture non usano `request_id`.

| Comando | Risposta |
|---|---|
| `PING` | `PONG` |
| `GET <key>` | `OK <value>` oppure `NOT_FOUND` |
| `GETV <key>` | `OK version=<n> <value>` oppure `NOT_FOUND` |
| `EXISTS <key>` | `OK true` oppure `OK false` |
| `KEYS` | `OK <key1> <key2> ...` oppure `OK` |
| `STATS` | `OK keys=<n> clients=<n> cached_requests=<n> window_size=<n>` |
| `QUIT` | `BYE` |

### Mutazioni idempotenti

Questi comandi transitano per la request table.

| Comando | Risposta |
|---|---|
| `SET_REQ <request_id> <key> <value...>` | `OK version=<n>` |
| `CAS_REQ <request_id> <key> <expected_version> <value...>` | `OK version=<n>`, `ERR version_mismatch current=<m>` oppure `ERR not_found` |
| `DELETE_REQ <request_id> <key>` | `OK deleted=true` oppure `NOT_FOUND` |

Casi speciali:

| Caso | Risposta |
|---|---|
| Stesso `request_id`, stesso payload | Replay della risposta salvata |
| Stesso `request_id`, payload diverso | `ERR request_id_conflict` |
| Retry fuori finestra | `ERR request_id_expired` |

### Mutazioni non idempotenti

Sono presenti solo per compatibilità e test manuale. Non fanno parte della garanzia di idempotenza.

```text
SET <key> <value...>
CAS <key> <expected_version> <value...>
DELETE <key>
```

I client corretti devono usare `SET_REQ`, `CAS_REQ` e `DELETE_REQ`.

---

## Esempio manuale

Avviare il server:

```bash
python3 server.py
```

Avviare il client:

```bash
python3 client.py --client-id clientA
```

Eseguire:

```text
ping
set corso ads
getv corso
retry
getv corso
cas corso 0 sistemi-distribuiti
retry
getv corso
delete corso
retry
getv corso
quit
```

Output atteso nei punti principali:

```text
-> PING
<- PONG

-> SET_REQ clientA:0 corso ads
<- OK version=0

-> SET_REQ clientA:0 corso ads
<- OK version=0

-> CAS_REQ clientA:1 corso 0 sistemi-distribuiti
<- OK version=1

-> CAS_REQ clientA:1 corso 0 sistemi-distribuiti
<- OK version=1

-> DELETE_REQ clientA:2 corso
<- OK deleted=true

-> DELETE_REQ clientA:2 corso
<- OK deleted=true

-> GETV corso
<- NOT_FOUND
```

Il punto importante è che il retry non incrementa la versione una seconda volta e non riesegue il delete.

---

## Test automatici

Eseguire:

```bash
python3 acceptance_test.py
```

La suite importa direttamente `KVStore` da `server.py` e testa `handle_line()`.

Questo rende i test rapidi, deterministici e indipendenti dal timing di rete.

I test coprono:

- retry di `SET_REQ`;
- retry di `CAS_REQ` riuscita;
- retry di `CAS_REQ` fallita;
- riuso dello stesso `request_id` con payload diverso;
- retry di `DELETE_REQ`;
- separazione tra client diversi;
- request id evictato dalla finestra;
- comandi malformati.

Output atteso:

```text
All tests passed: 11/11
```

---

## Garanzie principali

- Una richiesta mutativa identificata da `(client_id, seq)` viene applicata al massimo una volta entro la finestra mantenuta dal server.
- Un retry identico riceve la stessa risposta della prima esecuzione.
- Lo stesso `request_id` con payload diverso viene rifiutato.
- Un retry fuori finestra riceve `ERR request_id_expired` e non viene rieseguito.
- La request table è limitata a `N` richieste per client.

Default:

```text
N = 100
```

---

## Limiti dichiarati

- La request table è solo in memoria.
- L'idempotenza non sopravvive a un riavvio del server.
- Il sistema è single-node.
- Non c'è replica della request table.
- Non c'è autenticazione del `client_id`.
- Non c'è ordinamento globale tra client diversi.
- Non c'è garanzia exactly-once distribuita.
- La garbage collection ha costo `O(N)`, con `N` bounded e configurabile.

---

## Sintesi

Il progetto garantisce retry sicuri entro una singola istanza server e dentro la finestra dichiarata.

La logica centrale è:

```text
stesso request_id, stesso payload     -> replay risposta salvata
stesso request_id, payload diverso    -> ERR request_id_conflict
request_id già evictato               -> ERR request_id_expired
request_id mai visto                  -> prima esecuzione normale
```
