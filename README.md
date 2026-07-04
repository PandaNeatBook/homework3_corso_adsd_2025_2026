# Homework 3: KV Store con Retry Idempotenti

Estensione del KV store versionato con compare-and-set che introduce
l'idempotenza delle operazioni mutative tramite `request_id`.

## Problema risolto

In un sistema distribuito un client può inviare una scrittura, perdere la
risposta (timeout, disconnessione, crash del thread di rete) e non sapere
se il server l'abbia già applicata. Un retry alla cieca rischia di applicare
due volte lo stesso effetto su uno stato che nel frattempo potrebbe essere
cambiato.

La soluzione è che il client accompagni ogni operazione mutativa con un
identificatore univoco (`request_id`). Il server tiene traccia delle risposte
già calcolate e, in caso di retry, restituisce la risposta memorizzata senza
riapplicare l'effetto.

---

## File del progetto

| File                   | Contenuto                                                               |
| ---------------------- | ----------------------------------------------------------------------- |
| `api-contract.md`    | Contratto pubblico del protocollo: comandi, risposte, semantica, GC     |
| `SAFETY_LIVENESS.md` | Proprietà formali di safety (S1–S4) e liveness (L1–L3)               |
| `TECHNICAL_NOTE.md`  | Trade-off scelti, limiti della versione corrente, possibili evoluzioni  |
| `README.md`          | Questo file                                                             |
| `server.py`          | Server TCP multithread e logica del KV store                            |
| `client.py`          | Client interattivo con generazione di `request_id` e comando `retry` |
| `acceptance_test.py` | Test automatici del contratto di idempotenza                            |

---

## Come si esegue

### Avvio del server

```bash
python3 server.py
```

Il server ascolta su `127.0.0.1:6379` di default.

### Connessione con il client interattivo

```bash
python3 client.py
```

oppure con host e porta espliciti:

```bash
python3 client.py --host 127.0.0.1 --port 6379
```

### Esecuzione dei test di accettazione

```bash
python3 acceptance_test.py
```

La suite importa direttamente `KVStore` da `server.py` e testa `handle_line()`.
Questo rende i test rapidi, deterministici e indipendenti dal timing di rete.

---

## Comandi disponibili

### Comandi di sola lettura (naturalmente idempotenti)

| Comando          | Risposta                                                               |
| ---------------- | ---------------------------------------------------------------------- |
| `PING`         | `OK PONG`                                                            |
| `GET <key>`    | `OK <value>` oppure `NOT_FOUND`                                    |
| `GETV <key>`   | `OK version=<n> <value>` oppure `NOT_FOUND`                        |
| `EXISTS <key>` | `OK 1` oppure `OK 0`                                               |
| `KEYS`         | `OK <key1> <key2> ...` oppure `OK` se lo store è vuoto             |
| `STATS`        | `OK keys=<n> clients=<n> cached_requests=<n> window_size=<n>`      |
| `QUIT`         | `OK BYE` (chiude la connessione)                                   |
| `HELP`         | Mostra l'help                                                        |

### Comandi mutativi idempotenti (con `request_id`)

Il `request_id` ha il formato `<client_id>:<seq>` dove `seq` è un intero
non negativo strettamente crescente per ogni nuova operazione logica.
Esempio: `clientA:42`.

| Comando                                                      | Risposta (prima esecuzione)                                                              |
| ------------------------------------------------------------ | ---------------------------------------------------------------------------------------- |
| `SET_REQ <request_id> <key> <value...>`                    | `OK version=<n>`                                                                       |
| `CAS_REQ <request_id> <key> <expected_version> <value...>` | `OK version=<n>`, `ERR version_mismatch current=<m>` oppure `ERR not_found`          |
| `DELETE_REQ <request_id> <key>`                            | `OK` oppure `NOT_FOUND`                                                               |

Al retry con stesso `request_id` e stesso payload, il server restituisce
la risposta memorizzata senza riapplicare l'effetto.

### Comandi mutativi non idempotenti (compatibilità)

| Comando                                     | Risposta                                                                                    |
| ------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `SET <key> <value...>`                    | `OK version=<n>`                                                                          |
| `CAS <key> <expected_version> <value...>` | `OK version=<n>`, `ERR version_mismatch current=<m>` oppure `ERR not_found`             |
| `DELETE <key>`                            | `OK` oppure `NOT_FOUND`                                                                  |

Questi comandi non transitano per la request table. Il retry alla cieca
è a rischio del chiamante.

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
<- OK PONG

-> SET_REQ clientA:0 corso ads
<- OK version=0

-> SET_REQ clientA:0 corso ads
<- OK version=0

-> CAS_REQ clientA:1 corso 0 sistemi-distribuiti
<- OK version=1

-> CAS_REQ clientA:1 corso 0 sistemi-distribuiti
<- OK version=1

-> DELETE_REQ clientA:2 corso
<- OK

-> DELETE_REQ clientA:2 corso
<- OK

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

- **At-most-once execution**: una richiesta con dato `request_id` applica il
  proprio effetto allo store al più una volta.
- **Replay coerente**: la risposta al retry è identica a quella della prima
  esecuzione, inclusi gli errori applicativi (`ERR version_mismatch`,
  `NOT_FOUND`).
- **Memoria limitata**: per ogni `client_id` il server conserva al più `N=100`
  voci (sliding window). La garbage collection è O(N) per eviction, inline,
  senza thread di background.
- **Scadenza esplicita**: un retry fuori finestra riceve `ERR request_id_expired`
  invece di essere silenziosamente rieseguito.

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

## Esperimenti

### Esperimento 1: retry sicuro dopo timeout simulato

```
SET_REQ clientA:1 corso sistemi-distribuiti   → OK version=0
SET_REQ clientA:1 corso sistemi-distribuiti   → OK version=0  (retry)
GETV corso                                    → OK version=0 sistemi-distribuiti
```

La versione è 0 in entrambi i casi: l'effetto è stato applicato una sola volta.

### Esperimento 2: CAS_REQ non si applica due volte

```
SET_REQ clientA:10 contatore zero             → OK version=0
CAS_REQ clientA:11 contatore 0 uno           → OK version=1
CAS_REQ clientA:11 contatore 0 uno           → OK version=1  (retry)
GETV contatore                               → OK version=1 uno
```

La versione è 1, non 2: il retry ha restituito la risposta cached.

### Esperimento 3: conflitto di payload rilevato

```
SET_REQ clientA:20 chiave valore-A           → OK version=0
SET_REQ clientA:20 chiave valore-B           → ERR request_id_conflict
GETV chiave                                  → OK version=0 valore-A
```

Il server segnala il riuso errato dello stesso `request_id` con payload diverso.

### Esperimento 4: scadenza della finestra

```
-- invio 101 richieste distinte: clientA:0 ... clientA:100 --
SET_REQ clientA:101 k v                      → OK version=101  (evicta seq=0)
SET_REQ clientA:0 k v  (retry tardivo)       → ERR request_id_expired
```

Il retry fuori finestra riceve un errore esplicito invece di essere rieseguito.

---



## Tabella degli errori

| Risposta                             | Causa                                               |
| ------------------------------------ | --------------------------------------------------- |
| `ERR unknown_command`              | Comando non riconosciuto                            |
| `ERR invalid_request_id`           | `request_id` non nel formato `<id>:<seq>`       |
| `ERR request_id_expired`           | `seq <= eviction_boundary`; garanzia scaduta      |
| `ERR request_id_conflict`          | Stesso `request_id`, payload diverso               |
| `ERR version_mismatch current=<n>` | `CAS_REQ` fallita per versione non corrispondente |
| `ERR usage: ...`                   | Argomenti mancanti o malformati                     |
