# Design Note: Implementazione dei Retry Idempotenti

Questo documento descrive le scelte di design alla base del contratto
`api-contract.md` e fornisce ai colleghi le linee guida per l'implementazione.

Non e' un codice: e' la spiegazione del "perche'" delle scelte, con i punti
critici da rispettare per non violare le proprieta' di safety e liveness
dichiarate nel contratto.

---

## Problema da risolvere

In un sistema distribuito un client che invia una scrittura puo' non sapere
se il server l'ha applicata. Le cause tipiche sono:

- timeout del client prima che la risposta arrivi;
- disconnessione di rete dopo che il server ha gia' applicato l'operazione;
- crash e riavvio del client con stato parziale.

Se il client ritenta alla cieca, rischia un **doppio effetto**: un valore
scritto due volte, un contatore incrementato due volte, una riga duplicata.

La soluzione e' fare in modo che ogni operazione mutativa abbia un
identificatore univoco. Il server ricorda gli esiti gia' calcolati e, se vede
lo stesso identificatore, risponde con l'esito memorizzato senza rieseguire.

---

## Struttura della request table

La request table e' il cuore dell'implementazione. La sua struttura logica e':

```
request_table[client_id][seq] = (payload_canonico, risposta)
eviction_boundary[client_id]  = max_seq_evictato
```

**Ogni voce contiene due campi**, non uno solo:

1. `payload_canonico`: il testo del comando senza il `request_id`,
   normalizzato. Serve a rilevare il caso "stesso `request_id`, operazione
   diversa" che sarebbe un bug del client.

2. `risposta`: la stringa di risposta calcolata la prima volta, **inclusi
   gli esiti di errore**. Un `CAS_REQ` che ha prodotto `ERR version_mismatch`
   deve essere memorizzato e riprodotto identico al retry.

Se si salva solo la risposta, il server non puo' distinguere un retry
legittimo da un errore del client che ha riusato un `request_id` per
un'operazione diversa. L'unico esito corretto in quel caso e'
`ERR request_id_conflict`, non silenziosamente rispondere con il risultato
di una richiesta diversa.

---

## Perche' serve l'eviction boundary (low-watermark)

La sliding window evicta le voci piu' vecchie quando supera `N` elementi.
Dopo l'eviction, il server non ha piu' traccia del `seq` eliminato.

Senza un campo aggiuntivo, quando arriva un `seq` non presente nella finestra,
il server non sa distinguere tra:

- `seq` **mai visto**: il client sta facendo una prima richiesta legittima,
  l'effetto deve essere applicato;
- `seq` **gia' evictato**: il client sta ritentando fuori finestra,
  il server deve rispondere `ERR request_id_expired`.

La confusione tra i due casi e' un bug di correttezza grave:

- nel primo caso, trattarlo come scaduto significa rifiutare una richiesta
  valida;
- nel secondo caso, trattarlo come primo arrivo significa eseguire un effetto
  che potrebbe essere un doppio.

La soluzione e' mantenere per ogni client un **eviction boundary**: il valore
piu' alto di `seq` mai evictato dalla finestra di quel client.

```
eviction_boundary[client_id] = max(seq evictati finora)
```

La logica di lookup diventa:

```
se seq e' in finestra           → replay (controlla payload, poi risposta)
se seq <= eviction_boundary     → ERR request_id_expired
altrimenti                      → prima esecuzione
```

Se un `client_id` non ha mai subito eviction, `eviction_boundary` e'
assente e il secondo controllo non scatta.

---

## Sequenza corretta di aggiornamento

La risposta deve essere salvata nella request table **prima** di essere
inviata al client, e **dopo** che l'effetto e' stato applicato allo store.

La sequenza corretta per un comando mutativo e':

```
1. controlla request_table[client_id][seq]
   → se presente: verifica payload, poi restituisci risposta (STOP)
2. controlla eviction_boundary[client_id]
   → se seq <= boundary: ERR request_id_expired (STOP)
3. applica l'effetto allo store
4. costruisci la risposta
5. salva (payload_canonico, risposta) in request_table[client_id][seq]
6. invia la risposta al client
```

Se si inverte il passo 5 e il passo 6 (si invia prima di salvare), esiste
una finestra in cui il server ha risposto ma la risposta non e' ancora
memorizzata. Un retry in quella finestra verrebbe trattato come prima
esecuzione, violando la safety.

---

## Atomicita' rispetto ai thread

In un server multithread, **tutti i passi 1-5** devono avvenire dentro lo
stesso lock che protegge lo store. In questo modo:

- nessun altro thread puo' modificare la chiave tra il controllo in
  request_table e l'applicazione dell'effetto;
- nessun retry concorrente dello stesso `request_id` puo' passare il
  controllo al passo 1 contemporaneamente e applicare l'effetto due volte.

Tenere il lock durante la scrittura in request_table e' necessario, non
ottimizzabile via.

---

## Payload canonico: come calcolarlo

Il payload canonico e' il testo del comando senza il campo `request_id`,
con normalizzazione **selettiva**:

- il nome del comando viene portato in maiuscolo;
- i token strutturali (chiave, versione attesa nel caso di `CAS_REQ`)
  vengono estratti invariati;
- **il valore finale viene preservato esattamente come ricevuto**, escluso
  il solo newline di fine riga.

Questo e' necessario perche' il valore puo' contenere spazi interni
legittimi. Se si collassasse l'intero whitespace, `hello   world` e
`hello world` diventerebbero indistinguibili, e un retry con valore
leggermente diverso non verrebbe rilevato come conflitto.

Esempi:

```text
SET_REQ clientA:42 corso sistemi   distribuiti
  → payload: "SET_REQ corso sistemi   distribuiti"

CAS_REQ clientA:43 corso 0 sistemi   distribuiti
  → payload: "CAS_REQ corso 0 sistemi   distribuiti"

DELETE_REQ clientA:44 corso
  → payload: "DELETE_REQ corso"
```

In pratica, il calcolo del payload canonico consiste nel:

1. decodificare la riga ricevuta e rimuovere il solo `\n` finale;
2. dividere sulla prima occorrenza di spazio per separare il comando;
3. dividere l'argomento restante per estrarre il `request_id`;
4. ricombinare: `COMANDO_UPPER token_strutturali valore_esatto`.

Il confronto al retry e' una semplice uguaglianza di stringhe tra il
payload canonico appena calcolato e quello memorizzato.

---

## Sliding window: struttura dati suggerita

```python
class RequestWindow:
    def __init__(self, max_size: int = 100) -> None:
        # seq -> (payload_canonico, risposta)
        self._cache: dict[int, tuple[str, str]] = {}
        self._max_size = max_size
        self._eviction_boundary: int | None = None

    def get(self, seq: int) -> tuple[str, str] | None:
        """Restituisce (payload, response) se seq e' in finestra, None altrimenti."""
        return self._cache.get(seq)

    def is_expired(self, seq: int) -> bool:
        """True se seq e' stato evictato dalla finestra."""
        return (
            self._eviction_boundary is not None
            and seq <= self._eviction_boundary
        )

    def put(self, seq: int, payload: str, response: str) -> None:
        """Inserisce una nuova voce ed evicta la piu' vecchia se necessario."""
        if seq in self._cache:
            return  # non sovrascrivere una voce gia' presente
        self._cache[seq] = (payload, response)
        if len(self._cache) > self._max_size:
            oldest = min(self._cache)
            del self._cache[oldest]
            if self._eviction_boundary is None or oldest > self._eviction_boundary:
                self._eviction_boundary = oldest
```

La `RequestTable` principale e' `dict[str, RequestWindow]` indicizzata per
`client_id`. Un `client_id` non presente significa "mai visto".

---

## Gestione del conflitto di payload

Quando al passo 1 si trova la voce in finestra ma il payload non corrisponde:

```python
cached_payload, cached_response = window.get(seq)
if canonical_payload != cached_payload:
    return "ERR request_id_conflict", False
```

Non si applica alcun effetto, non si sovrascrive la voce memorizzata,
non si aggiorna la finestra.

---

## ACK: estensione opzionale

Il comando `ACK <client_id> <seq>` e' opzionale. Se implementato:

- evicta tutte le voci con `seq <= ack_seq` per quel client;
- aggiorna `eviction_boundary` al valore `ack_seq` se maggiore del corrente.

Non e' necessario per la correttezza della versione base. Non va implementato
prima che il resto funzioni, perche' aggiunge complessita' semantica in
presenza di retry ancora in volo o richieste fuori ordine.

---

## Cosa verificare nei test di safety

Un test di safety minimo per `SET_REQ`:

```
1. SET_REQ clientA:1 key val1          → OK version=0
2. GET key                             → OK val1
3. SET_REQ clientA:1 key val1  (retry) → OK version=0  (stesso risultato)
4. GETV key                            → OK val1 version=0  (non version=1)
```

Un test di conflitto di payload:

```
1. SET_REQ clientA:2 key val1          → OK version=1
2. SET_REQ clientA:2 key val2  (payload diverso) → ERR request_id_conflict
3. GETV key                            → OK val1 version=1  (stato invariato)
```

Un test di scadenza (con window size = 2 per semplicita'):

```
1. SET_REQ clientA:1 key a             → OK version=0
2. SET_REQ clientA:2 key b             → OK version=1
3. SET_REQ clientA:3 key c             → OK version=2  (evicta seq=1)
4. SET_REQ clientA:1 key a  (retry)    → ERR request_id_expired
5. SET_REQ clientA:4 key d             → OK version=3  (il server non e' bloccato)
```

Un test di prima richiesta dopo eviction (distinguere "mai visto" da "evictato"):

```
-- window size = 2, seq=5 mai inviato prima --
1. SET_REQ clientA:3 key a             → OK version=0
2. SET_REQ clientA:4 key b             → OK version=1  (evicta seq=3, boundary=3)
3. SET_REQ clientA:5 key c             → OK version=2  (prima richiesta, seq 5 > boundary 3)
```

---

## Punto di partenza per il codice

Il lab piu' vicino come struttura e' `labs/kv_store/cas_versioning/server.py`.

Ha gia':

- server multithread con un thread per connessione;
- lock su tutte le operazioni mutative;
- command dispatch table;
- comandi `SET`, `CAS`, `DELETE`, `GET`, `GETV`.

L'estensione richiede di:

1. implementare `RequestWindow` con `get`, `is_expired`, `put`;
2. creare `request_table: dict[str, RequestWindow]` nello store;
3. aggiungere i handler `SET_REQ`, `CAS_REQ`, `DELETE_REQ` che applicano
   la sequenza a 6 passi descritta sopra;
4. tenere `request_table` sotto lo stesso lock dello store;
5. aggiungere `ERR request_id_conflict` e `ERR request_id_expired` alla
   tabella degli errori.

I comandi originali `SET`, `CAS`, `DELETE` possono restare invariati per
compatibilita' con i client esistenti (non portano `request_id`).

**Attenzione al formato di `GETV`**: il contratto ha cambiato il formato
della risposta da `OK <value> version=<n>` a `OK version=<n> <value...>`.
Aggiornare di conseguenza il handler di `GETV` nel server, il parser
della risposta nel client e tutti i test che verificano l'output di `GETV`.

---

## Scelte lasciate all'implementatore

Le seguenti scelte sono libere, purche' documentate:

- il valore esatto di `N` (window size);
- se implementare il comando `ACK` opzionale;
- se tenere statistiche su quanti replay sono stati serviti;
- se `seq` non monotoni sono accettati (il contratto non li vieta, ma la
  finestra sliding potrebbe evictarli prima del previsto).
