# Contratto del Protocollo: KV Store Distribuito con Gateway, Rebalancing e Retry Idempotenti

Questa versione estende il protocollo del KV store versionato introducendo un'architettura distribuita. Il sistema è ora composto da un **Router** (gateway per i client), un **Coordinator** (per la migrazione a caldo dei dati) e nodi **ShardNode** (per la memorizzazione fisica).

Il problema fondamentale dell'idempotenza rimane: un client può perdere la risposta di una scrittura (timeout, disconnessione) e non sapere se il sistema l'abbia già applicata. Un retry cieco rischierebbe di duplicare l'effetto. In un sistema distribuito, si aggiunge il rischio che il dato si stia spostando a causa di un cambio di topologia (rebalancing).

La soluzione si basa sull'identificatore univoco (`request_id`). Il **Router** gestisce centralmente la request table, assorbe i retry e instrada le richieste inedite verso lo ShardNode corretto, mascherando al client la complessità della transizione.

> **Nota:** La garanzia di idempotenza è valida solo per la durata della sessione del Router. La request table è mantenuta esclusivamente nella RAM del Router; un riavvio azzera la tabella e le garanzie. Gli ShardNode sottostanti sono "stupidi" e non hanno nozione di idempotenza o client.

---

## Struttura del repository

```text
.
├── src/
│   ├── router.py
│   ├── coordinator.py
│   ├── shard_node.py
│   ├── client.py
│   └── protocol_common.py
│
├── tests/
│   ├── acceptance_test.py
│   └── integration_test.py
│
├── docs/
│   ├── api-contract.md
│   ├── homework_3.md
│   ├── SAFETY_LIVENESS.md
│   ├── TECHNICAL_NOTE.md
│   └── Presentazione/
│       └── Presentazione_Finale.pptx
│
├── archive/
│   └── course_materials/
│       ├── labs/
│       └── lessons/
│
├── requirements.txt
├── pytest.ini
└── README.md
```

Il codice principale del progetto è contenuto in `src/`. I test automatici sono in `tests/`. La documentazione tecnica e la traccia sono in `docs/`. La presentazione finale è in `docs/Presentazione/`. Il materiale didattico del corso è stato spostato in `archive/course_materials/`, così resta disponibile ma separato dal codice del progetto.

---

## Trasporto

- Protocollo testuale su TCP.
- Encoding UTF-8.
- Una richiesta per riga, terminata da `\n`.
- Una risposta per riga, terminata da `\n`.
- I client si connettono **unicamente alla porta pubblica del Router**. Ogni connessione è gestita da un thread dedicato.

---

## Modello dati e Tombstone

- **Chiavi:** stringhe senza spazi.
- **Valori:** stringhe UTF-8 senza newline, ma con spazi ammessi.
- **Versionamento Globale:** Il Sequence Number (versione) è un intero globale gestito dal Router. Parte da `0` alla prima scrittura assoluta nel sistema e cresce di `1` ad ogni scrittura su qualsiasi chiave.
- Una chiave assente ha versione implicita `-1`.
- **Cancellazione Logica:** `DELETE_REQ` **non cancella fisicamente** il dato. Sovrascrive il valore con una stringa sentinella `<TOMBSTONE>` e incrementa la versione globale. Questo previene la resurrezione di dati cancellati durante i fallback di lettura causati dai rebalance.

---

## Formato del `request_id`

Il `request_id` identifica univocamente una singola operazione mutativa emessa da un client specifico.

Formato:

```text
<client_id>:<seq>
```

Dove:
- `client_id` è una stringa senza spazi e senza due punti `:` (es. `clientA`, `worker-3`).
- `seq` è un numero intero non negativo, strettamente crescente per ogni nuova operazione logica dello stesso client.

---

## Contenuto della request table nel Router

Per ogni `(client_id, seq)` già visto, il Router memorizza due informazioni:

1. **Il payload canonico della richiesta:** il testo del comando senza il `request_id`, portato in maiuscolo. **Il valore è preservato esattamente come ricevuto**, spazi inclusi. Questo rileva i conflitti se si tenta di riutilizzare lo stesso `request_id` per payload diversi.
2. **La risposta calcolata:** la stringa prodotta la prima volta (inclusi errori come `ERR_CAS_CONFLICT current=5` o `ERR_NOT_FOUND`).

**Eccezione Rebalancing:** Le risposte `ERR_REBALANCING` (emesse per bloccare una CAS durante una migrazione) **non vengono salvate** in tabella, per consentire al client un retry legittimo una volta terminato il rebalance.

---

## Comportamento al retry e Sequenza di esecuzione

Il parsing sintattico avviene fuori dai lock. Errori come `ERR usage: ...` o `ERR_INVALID_REQUEST_ID` non vengono salvati.

Se il comando è ben formato, il Router garantisce l'atomicità tramite un lock per client:

1. Acquisizione del `client_lock` per lo specifico `client_id`.
2. Se `(client_id, seq)` è nella request table:
   - confronta il payload canonico;
   - se coincidono: restituisce la risposta salvata (**STOP**);
   - se differiscono: restituisce `ERR_REQUEST_ID_CONFLICT` (**STOP**).
3. Se `seq <= eviction_boundary[client_id]`:
   - restituisce `ERR_REQUEST_ID_EXPIRED` (**STOP**).
4. Altrimenti (prima esecuzione):
   - fotografa la topologia attiva senza bloccare la rete (`_routing_snapshot`);
   - se l'operazione è `CAS_REQ` e c'è un rebalance in corso, restituisce `ERR_REBALANCING` (**STOP, non salva in cache**);
   - calcola il nuovo Sequence Number globale (`_version_lock`);
   - esegue la chiamata di rete TCP verso lo ShardNode di destinazione;
   - salva la tupla `(payload_canonico, risposta_dello_shard)` nella request table.
5. Rilascia il `client_lock` e invia la risposta al client.

L'atomicità nel `client_lock` fa sì che due thread con lo stesso `request_id` si accodino sul Router: il secondo thread troverà la risposta in cache e non farà mai una chiamata di rete duplicata.

---

## Comandi mutativi (con request_id)

### `SET_REQ <client_id>:<seq> <key> <value...>`

Crea o sovrascrive `key` con `value`. Durante un rebalance viene instradata direttamente alla topologia nuova.

- Risposte possibili: `OK version=<n>`
- Retry con payload diverso: `ERR_REQUEST_ID_CONFLICT`

### `CAS_REQ <client_id>:<seq> <key> <expected_version> <value...>`

Aggiorna `key` solo se la versione corrente è `expected_version`. **Bloccata durante il rebalance**.

- Risposte possibili: `OK version=<n>`, `ERR_CAS_CONFLICT current=<m>`, `ERR_NOT_FOUND`
- Risposta transitoria durante rebalance: `ERR_REBALANCING`

### `DELETE_REQ <client_id>:<seq> <key>`

Se la chiave esiste, la maschera scrivendo il valore `<TOMBSTONE>` con una nuova versione globale.

- Risposte possibili: `OK`, `ERR_NOT_FOUND`

---

## Comandi di lettura e monitoraggio (senza request_id)

Durante un rebalance, le letture usano un meccanismo di **fallback**: interrogano la topologia nuova e, se la chiave non c'è e non è un Tombstone, interrogano quella vecchia.

| Comando | Risposta |
|---|---|
| `PING` | `OK PONG` |
| `GET <key>` | `<value> <version>` oppure `ERR_NOT_FOUND` |
| `GETV <key>` | `<version>` oppure `ERR_NOT_FOUND` |
| `KEYS` | `OK <key1> <key2> ...` (spazio-separati, omette automaticamente i tombstone) |
| `STATS` | `OK shards=<n> rebalancing=<0/1> new_shards=<n> clients=<n> cached_requests=<n> window_size=<n>` |
| `QUIT` | `OK BYE` |

---

## Comandi di Amministrazione

Comandi per modificare dinamicamente il cluster:

| Comando | Descrizione |
|---|---|
| `ADD_SHARD <id> <host:port>` | Prepara l'aggiunta di un nodo. |
| `REMOVE_SHARD <id> <host:port>` | Prepara la rimozione di un nodo attivo. |
| `REBALANCE` | Avvia una migrazione asincrona gestita dal Coordinator, basata su una procedura semplificata copy-commit-cleanup ispirata alla Two-Phase Commit. |

---

## Garbage Collection della request table

### Sliding window

Il Router conserva al più `N` voci recenti per ogni `client_id`, ordinate logicamente per `seq`. Quando la finestra supera la dimensione massima, viene rimossa una richiesta vecchia e viene aggiornato il limite inferiore di validità dei retry. Parametro configurabile: default `100`.

### Eviction boundary (low-watermark)

Quando una voce viene scartata, si aggiorna:

```text
eviction_boundary[client_id] = max(eviction_boundary[client_id], seq_evictato)
```

Se un retry ha `seq <= eviction_boundary`, il Router risponde `ERR_REQUEST_ID_EXPIRED`.

---

## Sicurezza e Progresso in Sintesi

- **Nessun doppio effetto di rete.** L'acquisizione serializzata sul Router previene doppi invii agli ShardNode.
- **Isolamento e Parallelismo.** L'accesso al Router serializza solo lo *stesso* client. Client diversi viaggiano in parallelo verso shard diversi.
- **Prevenzione Zombie.** I tombstone evitano letture di dati obsoleti durante il fallback di migrazione.
- **Fail-safe Rebalance.** La procedura copy-commit-cleanup e il Watchdog riducono il rischio di blocchi e perdita di dati se il Coordinator fallisce durante la migrazione.

---

## Test

La suite comprende test di accettazione sul Router e test di integrazione sull'intera architettura distribuita.

### Test di accettazione

I test in `tests/acceptance_test.py` verificano il contratto principale del Router:

- retry idempotenti;
- replay della stessa risposta;
- conflitto di payload con stesso `request_id`;
- eviction della request table;
- gestione di `DELETE_REQ`;
- errori di protocollo.

### Test di integrazione

I test in `tests/integration_test.py` eseguono l'intera architettura (Router, Coordinator e 3 ShardNode) simulando socket TCP reali.

Scenari testati:

1. **Bootstrap Topologia:** avvio con 2 shard tramite `ADD_SHARD` e `REBALANCE`.
2. **Idempotenza di Base:** scritture, retry, cache hit, rilevamento conflitti di payload.
3. **CAS:** aggiornamento condizionato su versione e replay idempotente dell'esito.
4. **Rebalance e Fallback:** aggiunta a caldo del 3° shard, letture continue durante la migrazione, blocco transitorio delle `CAS_REQ`.
5. **Tombstone Cleanup:** `DELETE_REQ`, mascheramento della chiave cancellata e verifica che `KEYS` non mostri i tombstone.
6. **Idempotenza dopo Rebalance:** verifica che un retry vecchio continui a ricevere la risposta cached anche dopo un cambio di topologia.

Per installare le dipendenze ed eseguire tutti i test:

```bash
python -m pip install -r requirements.txt
python -m pytest -q
```

Per eseguire solo il test di integrazione:

```bash
python -m pytest tests/integration_test.py -v
```

---

## Tabella degli errori

| Risposta | Causa |
| --- | --- |
| `ERR unknown_command` | Comando non riconosciuto dal Router |
| `ERR_INVALID_REQUEST_ID` | Formato non conforme a `<id>:<seq>` |
| `ERR_REQUEST_ID_EXPIRED` | `seq <= eviction_boundary`; garanzia scaduta |
| `ERR_REQUEST_ID_CONFLICT` | Stesso `request_id`, payload diverso |
| `ERR_CAS_CONFLICT current=<n>` | `CAS_REQ` fallita per versione non corrispondente |
| `ERR_REBALANCING` | `CAS_REQ` rifiutata perché c'è un cambio di topologia in corso |
| `ERR_NOT_FOUND` | La chiave non esiste o è mascherata da un tombstone |
| `ERR shard_unreachable: <exc>` | Lo ShardNode interno non è raggiungibile via rete dal Router |
| `ERR usage: ...` | Argomenti mancanti o malformati |
| `ERR bad_version` | `CAS_REQ` con versione non intera |