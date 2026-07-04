# Contratto del Protocollo: KV Store con Retry Idempotenti

Questa versione estende il protocollo del KV store versionato con
compare-and-set introducendo l'idempotenza delle operazioni mutative.

Il problema che si vuole risolvere e' il seguente: un client puo' inviare
una scrittura, perdere la risposta (per timeout, disconnessione o crash
del proprio thread di rete) e non sapere se il server l'abbia gia' applicata.
Se il client ritenta alla cieca, rischia di applicare due volte lo stesso
effetto su uno stato che nel frattempo potrebbe essere cambiato.

La soluzione e' che il client accompagni ogni operazione mutativa con un
identificatore univoco di richiesta (`request_id`). Il server tiene traccia
delle risposte gia' calcolate e, in caso di retry, restituisce la risposta
memorizzata senza riapplicare l'effetto.

> Nota: la garanzia di idempotenza e' valida solo per la durata della sessione
> del server. La request table e' mantenuta esclusivamente in memoria; un
> riavvio del server azzera la tabella e le garanzie non sopravvivono.

---

## Trasporto

- Protocollo testuale su TCP.
- Encoding UTF-8.
- Una richiesta per riga, terminata da `\n`.
- Una risposta per riga, terminata da `\n`.
- Ogni connessione e' gestita da un thread dedicato.

---

## Modello dati

- Chiavi: stringhe senza spazi.
- Valori: stringhe UTF-8 senza newline, ma con spazi ammessi.
- Ogni chiave ha un valore e una versione intera associata.
- Una chiave assente ha versione implicita `-1`.
- La versione parte da `0` al primo inserimento e cresce di `1` ad ogni
  scrittura effettiva.
- `DELETE_REQ` cancella il valore e azzera la storia di versione della
  chiave: un successivo `SET_REQ` sulla stessa chiave reinizia da `version=0`.

---

## Formato del `request_id`

Il `request_id` identifica univocamente una singola operazione mutativa
emessa da un client specifico.

Formato:

```text
<client_id>:<seq>
```

Dove:

- `client_id` e' una stringa senza spazi e senza due punti `:`, che
  identifica il client (ad esempio `clientA`, `worker-3`, `node1`);
- `seq` e' un numero intero non negativo, strettamente crescente per ogni
  nuova operazione logica dello stesso client; lo stesso `seq` puo'
  ricomparire solo come retry identico della stessa richiesta.

Esempi validi:

```text
clientA:42
worker-3:0
node1:7
```

---

## Contenuto della request table

Per ogni `(client_id, seq)` gia' visto, il server memorizza due informazioni:

1. **il payload canonico della richiesta**: il testo del comando senza il
   `request_id`, con normalizzazione selettiva. Il nome del comando viene
   portato in maiuscolo; i token strutturali (chiave, versione attesa per
   `CAS_REQ`) vengono estratti invariati; **il valore e' preservato
   esattamente come ricevuto**, escluso il solo newline finale. Questo
   garantisce che `hello   world` e `hello world` restino distinti.
   Serve a rilevare conflitti tra richieste con stesso `request_id` ma
   operazione diversa.
2. **la risposta calcolata**: la stringa di risposta prodotta la prima volta,
   compresi gli esiti di errore (ad esempio `ERR version_mismatch current=0`).
   Anche gli errori applicativi vengono memorizzati e riprodotti identici
   al retry successivo.

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
dalla sezione critica. La request table viene aggiornata solo se il comando
e' riconosciuto come mutativo ben formato con `request_id` valido. Errori di
parsing (`ERR usage: ...`, `ERR invalid_request_id`) non vengono mai salvati
in tabella. Gli errori applicativi (`ERR version_mismatch`, `NOT_FOUND`) sono
invece esiti legittimi di un comando ben formato e vengono memorizzati e
riprodotti come qualsiasi altra risposta.


Quando il server riceve un comando mutativo valido con (`client_id, seq`),
esegue atomicamente rispetto a tutte le altre operazioni mutative i passi
di controllo della request table, eventuale applicazione allo store,
costruzione della risposta e salvataggio della risposta.

L'invio della risposta al client puo' avvenire fuori dalla sezione critica,
usando la stringa di risposta gia' determinata.

```
1. Se (client_id, seq) e' nella request table:
     a. confronta il payload canonico della nuova richiesta con quello salvato;
     b. se i payload coincidono: determina come risposta la risposta salvata;
     c. se i payload differiscono: determina come risposta ERR request_id_conflict;
     d. termina la sezione critica e invia la risposta al client (STOP).

2. Se seq <= eviction_boundary[client_id]:
     a. determina come risposta ERR request_id_expired;
     b. termina la sezione critica e invia la risposta al client (STOP).

3. Altrimenti e' la prima esecuzione:
     a. applica l'effetto allo store;
     b. costruisci la risposta;
     c. salva (payload_canonico, risposta) in request_table[(client_id, seq)];
     d. termina la sezione critica;
     e. invia la risposta al client.
```

L'atomicita' di questi passi e' fondamentale per la safety: senza di essa,
due thread che ricevono lo stesso `request_id` contemporaneamente potrebbero
superare entrambi il controllo al passo 1 e applicare l'effetto due volte.

Il campo `eviction_boundary[client_id]` e' il massimo `seq` gia' evictato
per quel client (vedi sezione Garbage Collection).

---

## Comandi mutativi con `request_id`

### `SET_REQ <client_id>:<seq> <key> <value...>`

Crea o sovrascrive il valore di `key` con `value`.

Richiesta:

```text
SET_REQ clientA:42 corso sistemi-distribuiti
```

Payload canonico memorizzato:

```text
SET_REQ corso sistemi-distribuiti
```

Risposte possibili (prima esecuzione):

```text
OK version=<n>
```

In caso di retry con stesso `request_id` e stesso payload:

```text
OK version=<n>
```

(la risposta memorizzata la prima volta, senza riapplicare l'effetto)

In caso di retry con stesso `request_id` ma payload diverso:

```text
ERR request_id_conflict
```

In caso di `seq` scaduto dalla finestra:

```text
ERR request_id_expired
```

### `CAS_REQ <client_id>:<seq> <key> <expected_version> <value...>`

Aggiorna `key` con `value` solo se la versione corrente e' `expected_version`.

Richiesta:

```text
CAS_REQ clientA:43 corso 0 sistemi-distribuiti-v2
```

Payload canonico memorizzato:

```text
CAS_REQ corso 0 sistemi-distribuiti-v2
```

Risposte possibili (prima esecuzione):

```text
OK version=<n>
```

oppure (versione non corrispondente):

```text
ERR version_mismatch current=<m>
```

**Importante**: anche `ERR version_mismatch` viene memorizzato nella
request table. Un retry con stesso `request_id` e stesso payload restituisce
sempre lo stesso errore, indipendentemente dallo stato attuale della chiave.
Lo store non viene modificato.

In caso di retry con payload diverso:

```text
ERR request_id_conflict
```

### `DELETE_REQ <client_id>:<seq> <key>`

Rimuove `key` dallo store e azzera la sua storia di versione.

Richiesta:

```text
DELETE_REQ clientA:44 corso
```

Payload canonico memorizzato:

```text
DELETE_REQ corso
```

Risposte possibili (prima esecuzione):

```text
OK
```

oppure (chiave assente):

```text
NOT_FOUND
```

Anche `NOT_FOUND` viene memorizzato e riprodotto identico al retry.

Semantica della versione dopo `DELETE_REQ`: la chiave ritorna allo stato
"assente" con versione implicita `-1`. Un successivo `SET_REQ` produce
`version=0` come se la chiave non fosse mai esistita.

---

## Comandi di lettura (senza `request_id`)

Le operazioni di sola lettura sono idempotenti per natura. Non transitano
per la request table e osservano lo stato corrente dello store al momento
dell'esecuzione.

- `PING` — risponde `OK PONG`
- `GET <key>` — risponde `OK <value>` oppure `NOT_FOUND`
- `GETV <key>` — risponde `OK version=<n> <value...>` oppure `NOT_FOUND`
- `EXISTS <key>` — risponde `OK 1` oppure `OK 0`
- `KEYS` — risponde `OK <key1> <key2> ...` (spazio-separati) oppure `OK` se lo
  store e' vuoto
- `STATS` — risponde `OK keys=<n> clients=<n> cached_requests=<n> window_size=<n>`
- `QUIT` — risponde `OK BYE` e chiude la connessione

Nota sul formato di `GETV`: i metadati (`version=<n>`) precedono il valore
per evitare ambiguita' di parsing quando il valore contiene spazi o la
stringa `version=`. Il client estrae `version=<n>` come secondo token e
tratta il resto della riga come valore esatto.

Nota su `STATS`: restituisce in una sola risposta quattro contatori utili
per monitorare lo stato del server:

- `keys`: numero di chiavi attualmente presenti nello store;
- `clients`: numero di `client_id` distinti con almeno una voce nella request table;
- `cached_requests`: numero totale di voci nella request table (somma su tutti i client);
- `window_size`: dimensione massima configurata della sliding window (`N`).

`STATS` e' un comando di sola lettura: non transita per la request table e
non richiede `request_id`.

---

## Garbage Collection della request table

### Strategia base: sliding window

Il server conserva, per ogni `client_id`, al piu' `N` voci recenti (ordinate
per `seq`). Quando si inserisce una nuova voce e la finestra e' piena, la voce
con `seq` piu' basso viene evictata.

`N` e' un parametro di configurazione; il valore di default consigliato e' `100`.

La strategia sliding window assume che un client corretto non invii nuove
operazioni logiche con `seq` minore di quelli gia' emessi, salvo retry di
richieste ancora presenti nella finestra. Richieste fortemente fuori ordine
possono ridurre la durata effettiva della garanzia, perche' una nuova voce
potrebbe evictare una voce vecchia ancora in attesa di retry.

### Eviction boundary (low-watermark)

Ogni volta che una voce viene evictata, il server aggiorna il campo:

```text
eviction_boundary[client_id] = max(eviction_boundary[client_id], seq_evictato)
```

Questo campo e' fondamentale per distinguere due situazioni altrimenti
indistinguibili:

- `seq` **mai visto**: il client sta inviando una prima richiesta legittima;
- `seq` **gia' evictato**: il client sta ritentando fuori finestra.

Senza l'eviction boundary, il server non puo' distinguere una richiesta mai vista da una richiesta gia' evictata; quindi rischierebbe di trattare un retry fuori finestra come una nuova prima esecuzione.

Il controllo sul passo 2 del processo di retry e' quindi:

```text
se seq <= eviction_boundary[client_id]: ERR request_id_expired
```

Se un `client_id` non ha mai subito eviction, `eviction_boundary[client_id]`
e' considerato pari a `-1`. Poiche' `seq` e' non negativo, la condizione
`seq <= -1` non e' mai vera: nessuna prima richiesta valida viene
erroneamente rifiutata come scaduta.

### Estensione opzionale: ACK cumulativo

Il comando `ACK <client_id> <seq>` puo' essere implementato come estensione
opzionale per permettere al client di liberare esplicitamente voci dalla
finestra.

Effetto: il server evicta tutte le voci con `seq <= ack_seq` e aggiorna
`eviction_boundary` di conseguenza.

Questa estensione non e' richiesta dalla versione base del contratto e non
e' necessaria per la correttezza. Aumenta la complessita' semantica in
presenza di retry still-in-flight o richieste fuori ordine. Se implementata,
deve essere documentata separatamente.

---

## Proprieta' di Safety

**Nessun doppio effetto.**
Una stessa operazione mutativa, identificata dal `request_id`, viene
applicata al piu' una volta allo store, anche se il client la invia piu' volte
con lo stesso payload. Questa garanzia dipende dall'atomicita' della sezione
critica descritta in "Comportamento al retry".

**Le richieste diverse non si confondono per chiave.**
La chiave di ricerca nella request table e' `(client_id, seq)`, non la chiave
KV. Due operazioni dello stesso client su chiavi diverse con `seq` diversi
sono trattate come richieste indipendenti.

**Il conflitto di payload viene rilevato.**
Se lo stesso `request_id` viene usato con un payload diverso (errore del
client), il server risponde `ERR request_id_conflict` senza applicare alcun
effetto e senza sovrascrivere la risposta gia' memorizzata.

**Il replay e' coerente con l'effetto gia' applicato.**
La risposta memorizzata riflette esattamente l'esito della prima applicazione,
compresi gli esiti di errore applicativi. Un retry di un `CAS_REQ` fallito
per `version_mismatch` ottiene sempre lo stesso errore. Gli errori di
parsing non vengono memorizzati e non sono soggetti a questa garanzia.

---

## Proprieta' di Liveness

**Il server fa progresso.**
La GC della request table e' locale al singolo client e a costo limitato
dalla dimensione della finestra `N`. Non richiede operazioni di rete ne'
scansioni globali dello store.

**Un client corretto completa.**
Il replay di un `request_id` e' garantito finche' la richiesta originale
e' ancora presente nella finestra delle ultime `N` voci memorizzate per
quel client. Se il client invia nuove mutazioni, le voci piu' vecchie
vengono progressivamente evictate: un retry di una richiesta ormai uscita
dalla finestra riceve `ERR request_id_expired` e non e' piu' coperto dalla
garanzia di idempotenza.

**La scadenza e' informativa, non deterministica sull'esito originale.**
`ERR request_id_expired` non significa che la richiesta originale non sia
stata applicata: significa solo che il server non conserva piu' abbastanza
informazione per garantire il replay idempotente. Il client che riceve
questo errore deve decidere autonomamente come procedere, ad esempio
leggendo lo stato corrente con `GETV` prima di emettere una nuova richiesta.

---

## Cosa non e' garantito

- **Nessuna sopravvivenza al riavvio**: la request table e' esclusivamente
  in memoria. Un riavvio del server azzera la tabella. Un retry di una
  richiesta precedente al crash viene trattato come prima esecuzione.
  L'aggiunta di persistenza (WAL della request table) e' una nota opzionale
  futura, non parte di questo contratto.
- **Nessuna garanzia di unicita' del `client_id`**: e' responsabilita' del
  client scegliere un identificatore univoco nel proprio dominio.
- **Nessun ordinamento globale** tra richieste di client diversi.
- **Nessuna replica** della request table su altri nodi.

---

## Comandi mutativi senza `request_id` (compatibilita')

I comandi `SET`, `CAS` e `DELETE` (senza il suffisso `_REQ`) non fanno
parte del contratto idempotente. Se presenti nell'implementazione, sono
da trattare come operazioni non-idempotenti di compatibilita' con client
precedenti: vengono eseguiti direttamente sullo store senza transitare
per la request table, e il retry alla cieca e' a rischio del chiamante.

I client corretti devono usare esclusivamente `SET_REQ`, `CAS_REQ` e
`DELETE_REQ` per tutte le operazioni mutative.

---

## Tabella degli errori di protocollo

| Risposta                             | Causa                                                                        |
| ------------------------------------ | ---------------------------------------------------------------------------- |
| `ERR unknown_command`              | Il comando non e' riconosciuto                                               |
| `ERR invalid_request_id`           | Il`request_id` non ha il formato `<id>:<seq>`                            |
| `ERR request_id_expired`           | `seq <= eviction_boundary`; il server non garantisce piu' il replay        |
| `ERR request_id_conflict`          | Stesso`request_id`, payload diverso da quello memorizzato                  |
| `ERR version_mismatch current=<n>` | `CAS_REQ` fallita per versione non corrispondente (memorizzato in tabella) |
| `ERR usage: ...`                   | Argomenti mancanti o malformati (non memorizzato in tabella)                 |

---

## Punto chiave del design

L'idempotenza non cambia la semantica dello store sottostante. Cambia il
contratto verso il client su quante volte un effetto viene prodotto, e
aggiunge una distinzione tra:

- retry legittimo: stesso `request_id`, stesso payload → risposta cached;
- errore del client: stesso `request_id`, payload diverso → conflitto esplicito;
- retry fuori finestra: `seq` evictato → scadenza esplicita;
- prima richiesta: `seq` mai visto e non evictato → esecuzione normale.

In un sistema distribuito, un client non puo' distinguere tra "il server ha
risposto ma la risposta e' andata persa" e "il server non ha mai ricevuto la
richiesta". Con il `request_id` e la request table, il retry diventa sicuro
entro la finestra dichiarata.