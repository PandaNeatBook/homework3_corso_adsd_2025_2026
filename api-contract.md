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

## Tabella degli errori di protocollo

| Risposta | Causa |
|---|---|
| `ERR unknown_command` | Il comando non è riconosciuto |
| `ERR invalid_request_id` | Il `request_id` non ha il formato `<id>:<seq>` |
| `ERR request_id_expired` | `seq <= eviction_boundary`; il server non garantisce più il replay |
| `ERR request_id_conflict` | Stesso `request_id`, payload diverso da quello memorizzato |
| `ERR version_mismatch current=<n>` | `CAS_REQ` fallita per versione non corrispondente (memorizzato in tabella) |
| `ERR usage: ...` | Argomenti mancanti o malformati (non memorizzato in tabella) |
| `ERR bad_version` | La versione passata alla `CAS_REQ` non è un intero non negativo valido |
