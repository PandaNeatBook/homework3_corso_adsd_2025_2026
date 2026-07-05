# Contratto del Protocollo: KV Store con Retry Idempotenti

Questa versione estende il protocollo del KV store versionato con
compare-and-set introducendo l'idempotenza delle operazioni mutative.

Il problema che si vuole risolvere è il seguente: un client può inviare
una scrittura, perdere la risposta (per timeout, disconnessione o crash
del proprio thread di rete) e non sapere se il server l'abbia già applicata.
Se il client ritenta alla cieca, rischia di applicare due volte lo stesso
effetto su uno stato che nel frattempo potrebbe essere cambiato.

La soluzione è che il client accompagni ogni operazione mutativa con un
identificatore univoco di richiesta (`request_id`). Il server tiene traccia
delle risposte già calcolate e, in caso di retry, restituisce la risposta
memorizzata senza riapplicare l'effetto.

> **Nota:** la garanzia di idempotenza è valida solo per la durata della sessione
> del server. La request table è mantenuta esclusivamente in memoria; un
> riavvio del server azzera la tabella e le garanzie non sopravvivono.

---

## Trasporto

- Protocollo testuale su TCP.
- Encoding UTF-8.
- Una richiesta per riga, terminata da `\n`.
- Una risposta per riga, terminata da `\n`.
- Ogni connessione è gestita da un thread dedicato.

---

## Modello dati

- **Chiavi:** stringhe senza spazi.
- **Valori:** stringhe UTF-8 senza newline, ma con spazi ammessi.
- Ogni chiave ha un valore e una versione intera associata.
- Una chiave assente ha versione implicita `-1`.
- La versione parte da `0` al primo inserimento e cresce di `1` ad ogni scrittura effettiva.
- `DELETE_REQ` cancella il valore e azzera la storia di versione della chiave: un successivo `SET_REQ` sulla stessa chiave reinizia da `version=0`.

---

## Formato del `request_id`

Il `request_id` identifica univocamente una singola operazione mutativa
emessa da un client specifico.

Formato:

```
<client_id>:<seq>
```

Dove:

- `client_id` è una stringa senza spazi e senza due punti `:`, che
  identifica il client (ad esempio `clientA`, `worker-3`, `node1`);
- `seq` è un numero intero non negativo, strettamente crescente per ogni
  nuova operazione logica dello stesso client; lo stesso `seq` può
  ricomparire solo come retry identico della stessa richiesta.

Esempi validi:

- `clientA:42`
- `worker-3:0`
- `node1:7`

---

## Contenuto della request table

Per ogni `(client_id, seq)` già visto, il server memorizza due informazioni:

1. **Il payload canonico della richiesta:** il testo del comando senza il `request_id`, con normalizzazione selettiva. Il nome del comando viene portato in maiuscolo; i token strutturali (chiave, versione attesa per `CAS_REQ`) vengono estratti invariati; **il valore è preservato esattamente come ricevuto**, escluso il solo newline finale. Questo garantisce che `hello   world` e `hello world` restino distinti. Serve a rilevare conflitti tra richieste con stesso `request_id` ma operazione diversa.
2. **La risposta calcolata:** la stringa di risposta prodotta la prima volta, compresi gli esiti di errore (ad esempio `ERR version_mismatch current=0`). Anche gli errori applicativi vengono memorizzati e riprodotti identici al retry successivo.

Esempio di voce nella tabella:

```text
client_id = "clientA"
seq       = 42
payload   = "SET_REQ key1 hello"
response  = "OK version=3"
```

---

## Comportamento al retry

Prima di tutto: il parsing e la validazione della sintassi avvengono fuori
dai lock. La request table viene aggiornata solo se il comando è riconosciuto come mutativo ben formato con request_id valido. Errori di parsing (`ERR usage: ...`, `ERR invalid_request_id`) non vengono mai salvati in tabella. Gli errori applicativi (`ERR version_mismatch`, `NOT_FOUND`) sono invece esiti legittimi di un comando ben formato e vengono memorizzati e riprodotti come qualsiasi altra risposta.

Quando il server riceve un comando mutativo valido con `(client_id, seq)`, esegue l'operazione garantendo l'atomicità locale tramite una strategia di locking a grana fine a due livelli. I lock vengono acquisiti in un ordine rigoroso e gerarchico (prima il client, poi la chiave) per prevenire deadlock.

L'invio effettivo della risposta al client avviene sempre al di fuori della sezione critica.

### Sequenza di esecuzione

1. Acquisizione del `client_lock` per lo specifico `client_id`.
2. Se `(client_id, seq)` è nella request table:
   - confronta il payload canonico della nuova richiesta con quello salvato;
   - se i payload coincidono: determina come risposta la risposta salvata;
   - se i payload differiscono: determina come risposta `ERR request_id_conflict`;
   - termina la sezione critica (rilascia il `client_lock`) e invia la risposta (**STOP**).
3. Se `seq <= eviction_boundary[client_id]`:
   - determina come risposta `ERR request_id_expired`;
   - termina la sezione critica (rilascia il `client_lock`) e invia la risposta (**STOP**).
4. Altrimenti è la prima esecuzione:
   - acquisizione del `key_lock` specifico per la chiave;
   - acquisizione del `store_structure_lock` (solo se l'operazione altera la struttura globale del dizionario, come una creazione o cancellazione);
   - applica l'effetto allo store e calcola la nuova versione;
   - rilascio del `store_structure_lock` e del `key_lock`;
   - costruisci la risposta e salva `(payload_canonico, risposta)` nella `request_table[(client_id, seq)]`;
   - rilascio del `client_lock`;
   - invia la risposta al client.

L'atomicità del controllo e del salvataggio all'interno del `client_lock` è fondamentale per la safety: senza di essa, due thread che ricevono lo stesso `request_id` contemporaneamente potrebbero superare entrambi il controllo e applicare l'effetto due volte.

---

## Comandi mutativi con request_id

### `SET_REQ <client_id>:<seq> <key> <value...>`

Crea o sovrascrive il valore di `key` con `value`.

- Richiesta: `SET_REQ clientA:42 corso sistemi-distribuiti`
- Payload canonico memorizzato: `SET_REQ corso sistemi-distribuiti`
- Risposte possibili (prima esecuzione): `OK version=<n>`
- Retry identico: `OK version=<n>` (risposta memorizzata, effetto non riapplicato)
- Retry con payload diverso: `ERR request_id_conflict`
- Retry fuori finestra: `ERR request_id_expired`

### `CAS_REQ <client_id>:<seq> <key> <expected_version> <value...>`

Aggiorna `key` con `value` solo se la versione corrente è `expected_version`.

- Richiesta: `CAS_REQ clientA:43 corso 0 sistemi-distribuiti-v2`
- Payload canonico memorizzato: `CAS_REQ corso 0 sistemi-distribuiti-v2`
- Risposte possibili (prima esecuzione): `OK version=<n>` oppure `ERR version_mismatch current=<m>`
- **Importante:** anche `ERR version_mismatch` viene memorizzato nella request table. Un retry con stesso `request_id` e stesso payload restituisce sempre lo stesso errore, indipendentemente dallo stato attuale della chiave. Lo store non viene modificato.

### `DELETE_REQ <client_id>:<seq> <key>`

Rimuove `key` dallo store e azzera la sua storia di versione.

- Richiesta: `DELETE_REQ clientA:44 corso`
- Payload canonico memorizzato: `DELETE_REQ corso`
- Risposte possibili (prima esecuzione): `OK` oppure `NOT_FOUND`
- Anche `NOT_FOUND` viene memorizzato e riprodotto identico al retry. Dopo una `DELETE_REQ`, la chiave ritorna allo stato "assente" (versione implicita `-1`). Un successivo `SET_REQ` produrrà `version=0`.

---

## Comandi di lettura (senza request_id)

Le operazioni di sola lettura sono idempotenti per natura. Non transitano per la request table e osservano lo stato corrente dello store acquisendo unicamente il `key_lock` temporaneo in fase di lettura.

| Comando | Risposta |
|---|---|
| `PING` | `OK PONG` |
| `GET <key>` | `OK <value>` oppure `NOT_FOUND` |
| `GETV <key>` | `OK version=<n> <value...>` oppure `NOT_FOUND` |
| `EXISTS <key>` | `OK 1` oppure `OK 0` |
| `KEYS` | `OK <key1> <key2> ...` (spazio-separati) oppure `OK` se lo store è vuoto |
| `STATS` | `OK keys=<n> clients=<n> cached_requests=<n> window_size=<n>` |
| `QUIT` | `OK BYE` e chiude la connessione |

---

## Garbage Collection della request table

### Strategia base: sliding window

Il server conserva, per ogni `client_id`, al più `N` voci recenti (ordinate per `seq`). Quando si inserisce una nuova voce e la finestra è piena, la voce con `seq` più basso viene eliminata (evictata). Il costo di ogni operazione di eviction è O(1) inline, senza gravare con thread di background. `N` è un parametro di configurazione (default: `100`).

### Eviction boundary (low-watermark)

Ogni volta che una voce viene evictata, il server aggiorna il campo:

```
eviction_boundary[client_id] = max(eviction_boundary[client_id], seq_evictato)
```

Questo permette al server di distinguere una prima richiesta legittima (`seq` mai visto) da un retry fuori finestra (`seq` evictato). Se `seq <= eviction_boundary[client_id]`, il server rigetta la richiesta con `ERR request_id_expired`.

---

## Proprietà di Safety in sintesi

- **Nessun doppio effetto.** L'acquisizione serializzata del `client_lock` impedisce che la stessa operazione mutativa sia applicata più di una volta.
- **Le richieste diverse non si confondono per chiave.** La chiave di ricerca nella request table è la tupla `(client_id, seq)`.
- **Il conflitto di payload viene rilevato.** Il server risponde `ERR request_id_conflict` bloccando tentativi errati di riutilizzo del `request_id`.
- **Il replay è coerente con l'effetto già applicato.** La risposta restituita al retry è identica alla stringa prodotta al termine della prima esecuzione e memorizzata nel `client_lock`.

## Proprietà di Liveness in sintesi

- **Massimo parallelismo (Liveness dei Lock).** Il sistema sfrutta lock a grana fine a due livelli (`_client_locks` e `_key_locks`) eliminando colli di bottiglia globali. Client diversi, o chiavi diverse gestite da client diversi, procedono in pieno parallelismo senza contese bloccanti.
- **Il server fa progresso.** L'eviction della memoria è gestita interamente a costo fisso senza pause di rete o stop-the-world per la garbage collection.
- **Un client corretto completa.** Il replay è sempre garantito purché il `seq` ritentato non sia scivolato fuori dalla finestra mobile di `N` richieste di quel client.

---

## Test di concorrenza e stress

Oltre ai test di accettazione sul contratto (`acceptance_test.py`), è prevista
una suite dedicata alla verifica delle garanzie di safety e liveness sotto
carico concorrente reale, usando socket TCP veri e più thread client
simultanei (non semplici chiamate dirette a `handle_line()`).

### Come funziona

Lo script avvia il server (`TCPKVServer`) in un thread daemon in background,
su una porta dedicata (es. `6380`, distinta da quella di default) per non
entrare in conflitto con un'eventuale istanza già in esecuzione. Dopo un
breve sleep per dare tempo al socket di fare bind e listen, verifica che il
server risponda con un `PING`.

Ogni comando viene inviato aprendo una connessione TCP reale tramite
`socket.create_connection`, scrivendo la richiesta e leggendo la risposta
riga per riga — replicando fedelmente il comportamento di un client reale,
incluso l'overhead di rete e la concorrenza a livello di thread del server.

### Scenari testati

**Test 1 — Idempotenza concorrente sullo stesso `request_id`**
10 thread inviano contemporaneamente lo stesso comando
`SET_REQ clientStress:1 key_stress valore_stress`. Ci si aspetta che tutte le
10 risposte siano identiche (`OK version=0`) e che la versione finale della
chiave resti `0`: l'effetto deve essere applicato esattamente una volta,
nonostante la race condition reale tra thread.

**Test 2 — Parallelismo tra client diversi**
10 thread, ciascuno con un `client_id` diverso, scrivono su 10 chiavi
diverse (`key_0`...`key_9`). Ci si aspetta che tutte le risposte siano
`OK version=0`, a conferma che il locking a grana fine (client_lock +
key_lock) non introduce contese spurie tra client e chiavi indipendenti.

**Test 3 — Concorrenza reale sulla stessa chiave**
15 client diversi (con `request_id` distinti) scrivono concorrentemente sulla
stessa chiave `shared_key`. Ci si aspetta che la versione finale sia `14`
(15 scritture effettive, da versione 0 a versione 14) e che lo stato finale
sia coerente, senza scritture perse o corruzione dovuta ad accessi
concorrenti sulla stessa risorsa.

### Cosa dimostra questa suite

- **Safety sotto race condition reali**: l'idempotenza sullo stesso
  `request_id` regge anche quando le richieste arrivano davvero in
  parallelo via rete, non solo in test sequenziali.
- **Liveness del locking a due livelli**: client e chiavi indipendenti non
  si bloccano a vicenda.
- **Correttezza sotto contesa reale**: scritture concorrenti sulla stessa
  chiave da client diversi vengono serializzate correttamente, senza perdite
  di versione.

Al termine, il server viene arrestato in modo pulito con `server.shutdown()`.

Esecuzione:

```bash
python3 stress_test.py
```

Output atteso in caso di successo:

```text
============================================================
CONCURRENCY & STRESS TEST COMPLETATO CON SUCCESSO!
============================================================
```

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
