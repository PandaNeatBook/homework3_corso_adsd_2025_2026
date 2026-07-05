# Proprietà di Safety e Liveness: KV Store con Retry Idempotenti

Questo documento descrive le proprietà di correttezza del progetto **KV Store con retry idempotenti tramite `request_id`**.

Il sistema è un Key-Value Store single-node, TCP, multithread, con stato in memoria. L'obiettivo è impedire che il retry di una richiesta mutativa produca due volte lo stesso effetto.

---

## Proprietà di Safety

Le proprietà di safety affermano che **non accade mai qualcosa di scorretto**.
In questo sistema, "scorretto" significa principalmente: un effetto applicato più
di una volta, una risposta incoerente con l'effetto prodotto, o due richieste
distinte confuse tra loro.

---

### S1 — Nessun doppio effetto

**Proprietà.** Un'operazione mutativa identificata da `(client_id, seq)`
viene applicata allo store **al più una volta**, indipendentemente da quante
volte il client invia la stessa richiesta con lo stesso `request_id` e
stesso payload.

**Perché vale.** Il lookup nella request table e l'applicazione dell'effetto allo store sono protetti da una strategia di locking a grana fine a due livelli. I lock vengono acquisiti in un ordine rigoroso e gerarchico (prima il client, poi la chiave) per prevenire deadlock e garantire l'atomicità locale:

```
1. Acquisizione del `client_lock` per il client_id specifico.
2. Controlla request_table[(client_id, seq)]
   → trovato: restituisci la risposta memorizzata (STOP, nessun effetto).
3. Controlla eviction_boundary[client_id]
   → seq <= boundary: ERR request_id_expired (STOP, nessun effetto).
4. Acquisizione del `key_lock` specifico per la chiave.
5. Acquisizione del `store_structure_lock` (solo se l'operazione altera la struttura globale del dizionario).
6. Applica l'effetto allo store e calcola la nuova versione.
7. Rilascio del `store_structure_lock` e del `key_lock`.
8. Costruisci e salva (payload_canonico, risposta) in request_table.
9. Rilascio del `client_lock`.
```

Poiché il controllo iniziale (2) e il salvataggio finale (8) avvengono sotto lo stesso client_lock, due thread che ricevono lo stesso request_id contemporaneamente si serializzano alla radice: il secondo trova già la voce in tabella e restituisce la risposta cached senza toccare lo store o acquisire lock aggiuntivi.

**Cosa potrebbe violarla.** Se il client_lock non coprisse sia la fase di check iniziale che quella di salvataggio in cache, due thread potrebbero superare entrambi il controllo e acquisire in sequenza il key_lock, applicando l'effetto due volte. Inoltre, se l'ordine di acquisizione dei lock (client -> chiave) non fosse strettamente rispettato, si verificherebbero dei deadlock.

---

### S2 — Il replay è coerente con l'effetto applicato

**Proprietà.** La risposta restituita al retry è sempre quella prodotta
durante la prima esecuzione, e riflette esattamente l'esito di quella prima
applicazione. In particolare:

- se la prima esecuzione ha prodotto `OK version=3`, il retry riceve
  `OK version=3` anche se nel frattempo la chiave è stata aggiornata;
- se la prima esecuzione ha prodotto `ERR version_mismatch current=5`,
  il retry riceve lo stesso errore, anche se la versione della chiave
  è cambiata;
- se la prima esecuzione ha prodotto `NOT_FOUND` (su un `DELETE_REQ`
  di chiave assente), il retry riceve `NOT_FOUND`.

**Perché vale.** La risposta viene salvata nella request table dopo che l'effetto è stato applicato allo store, ma prima che il client_lock venga rilasciato. Non esiste finestra temporale in cui un altro thread associato allo stesso client_id possa intromettersi tra la modifica dello store e la memorizzazione della risposta. Qualunque retry successivo si metterà in coda sul client_lock e troverà la risposta già in tabella.

**Cosa potrebbe violarla.** Inviare la risposta al client o rilasciare il client_lock prima di salvarla in tabella aprirebbe una finestra in cui un retry concorrente potrebbe essere trattato come prima esecuzione. Questa inversione è espressamente vietata dal design.

---

### S3 — Richieste diverse non si confondono

**Proprietà.** Due operazioni con `seq` diversi, o di `client_id` diversi,
sono trattate come richieste indipendenti. La chiave di ricerca nella request
table è `(client_id, seq)`, non la chiave KV.

**Perché vale.** La request table è indicizzata per coppia `(client_id, seq)`.
Due richieste con `seq` diversi producono voci distinte, anche se toccano la
stessa chiave KV. Due client diversi con lo stesso `seq` producono voci
distinte perché il `client_id` è diverso.

**Cosa potrebbe violarla.** Usare solo la chiave KV come indice della request
table causerebbe la confusione di operazioni distinte dello stesso client.

---

### S4 — Il conflitto di payload viene rilevato e segnalato

**Proprietà.** Se lo stesso `request_id` viene inviato con un payload diverso
da quello memorizzato (errore del client, bug, race condition lato chiamante),
il server risponde `ERR request_id_conflict` senza applicare alcun effetto e
senza sovrascrivere la risposta già memorizzata.

**Perché vale.** Oltre alla risposta, la request table memorizza il **payload
canonico** della prima richiesta. Al retry, il payload canonico della nuova
richiesta viene confrontato con quello memorizzato. Se differiscono, il server
produce `ERR request_id_conflict` e termina la sezione critica senza modificare
né lo store né la tabella.

**Cosa potrebbe violarla.** Memorizzare solo la risposta senza il payload
canonico renderebbe impossibile distinguere un retry legittimo da un riuso
errato dello stesso `request_id`. Il server risponderebbe silenziosamente con
il risultato di un'operazione diversa da quella richiesta.

---

### S5 — Retry fuori finestra non rieseguito

**Proprietà.** Se una richiesta è stata eliminata dalla finestra della request table, un retry successivo non viene eseguito come nuova richiesta.

Il server risponde:

```text
ERR request_id_expired
```

Esempio con finestra `N = 2`:

```text
SET_REQ clientA:0 k v0  -> OK version=0
SET_REQ clientA:1 k v1  -> OK version=1
SET_REQ clientA:2 k v2  -> OK version=2
SET_REQ clientA:0 k v0  -> ERR request_id_expired
GETV k                  -> OK version=2 v2
```

**Motivazione.** Il server mantiene un boundary per ogni client:

```text
_eviction_boundary[client_id]
```

Se arriva un `seq` minore o uguale al boundary, il server sa che quella richiesta è troppo vecchia e non può più garantirne il replay.

---

### S6 — Errori di parsing non memorizzati

**Proprietà.** Gli errori sintattici o di parsing non vengono salvati nella request table.

Esempi:

```text
ERR usage: SET_REQ <request_id> <key> <value...>
ERR usage: CAS_REQ <request_id> <key> <expected_version> <value...>
ERR usage: DELETE_REQ <request_id> <key>
ERR invalid_request_id
ERR bad_version
```

**Motivazione.** La request table viene aggiornata solo per richieste mutative ben formate. Se il client corregge una richiesta malformata e la reinvia con lo stesso `request_id`, il server la tratta come prima richiesta valida.

---

## Proprietà di Liveness

Le proprietà di liveness affermano che **qualcosa di desiderato continua a
poter accadere**. In questo sistema ciò significa: il server fa progresso,
la memoria è limitata, e un client corretto può completare le proprie
operazioni.

---

### L1 — La memoria della request table è limitata

**Proprietà.** Per ogni `client_id`, la request table contiene al più `N`
voci (window size, default 100). Un singolo client non può causare crescita
illimitata della memoria del server, indipendentemente da quante richieste
invia.

**Perché vale.** La struttura dati è una sliding window per client. Ogni
volta che si inserisce una nuova voce e la finestra ha raggiunto `N`
elementi, la voce con `seq` più basso viene rimossa. La dimensione è
mantenuta a ≤ N voci per ogni `client_id`. Il costo di ogni eviction è O(1).

**Cosa potrebbe violarla.** Non implementare la politica di eviction (conservare
tutte le voci per sempre) causerebbe crescita lineare illimitata della memoria
con il numero totale di richieste ricevute.

---

### L2 — La garbage collection non blocca il servizio

**Proprietà.** L'eviction delle voci più vecchie dalla finestra non richiede
operazioni di rete, scansioni globali, pause o background thread. Non
introduce latency spike per i client.

**Perché vale.** L'eviction avviene **inline** dentro la stessa acquisizione
di lock della nuova richiesta. Costa al più una cancellazione dal dizionario
(O(1)) e un aggiornamento di `eviction_boundary`. Non esiste un processo
separato di garbage collection che potrebbe interferire con il servizio o
richiedere lock aggiuntivi.

**Cosa potrebbe violarla.** Usare una scadenza temporale (TTL) richiederebbe
un thread di background che scansiona periodicamente la tabella, introducendo
pause e complessità di locking. La sliding window evita questo problema.

---

### L3 — Un client corretto può sempre completare una sequenza di retry

**Proprietà.** Un client che usa `seq` strettamente crescenti e non invia
più di `N` nuove richieste logiche senza aver completato il retry di una
precedente riceverà sempre la risposta idempotente (cached) per quella
richiesta.

**Perché vale.** Con `seq` monotoni crescenti, la voce più vecchia nella
finestra è sempre quella con `seq` più basso. Finché il client non avanza
di più di `N` posizioni senza attendere l'acknowledgement, quella voce
rimane nella finestra.

**Cosa potrebbe violarla.** Un client che invia `N+1` nuove richieste
logiche prima di completare il retry della prima causa l'eviction di quella
prima voce. Il retry successivo riceverà `ERR request_id_expired`. Questo
è dichiarato nel contratto come condizione fuori dalla garanzia: è una
violazione del protocollo lato client, non un difetto del server.

---

### L4 — Errori terminali non bloccano il servizio

**Proprietà.** Richieste malformate, in conflitto o fuori finestra non bloccano il server.

Il server restituisce un errore esplicito e termina la gestione della richiesta:

```text
ERR malformed
ERR bad_request_id
ERR request_id_conflict
ERR request_id_expired
```

Non ci sono attese su risorse esterne, consenso distribuito o retry automatici interni.

---

### L5 — Operazioni concorrenti non creano colli di bottiglia globali (Liveness dei Lock)

**Proprietà.** Richieste inviate da client diversi, o dirette verso chiavi diverse da client diversi, procedono in pieno parallelismo senza bloccarsi a vicenda.

**Perché vale.** Il sistema non utilizza un singolo lock globale transazionale, ma delega il blocco a una strategia a grana fine (_client_locks e _key_locks). Il lock globale (_meta_lock) viene trattenuto per un tempo microscopico, solo per allocare e recuperare l'oggetto lock dal dizionario. Di conseguenza:
- Due client che operano in simultanea su chiavi diverse non subiscono alcuna contesa di blocco (liveness massima).
- Due client che operano sulla stessa chiave competono solo sull'acquisizione del key_lock, per il tempo strettamente necessario a calcolare la nuova versione.

**Cosa potrebbe violarla.** L'uso di un singolo lock globale per l'intera sequenza di lookup, validazione, scrittura e cache azzererebbe il parallelismo, costringendo tutti i client a mettersi in fila indiana per accedere al database (violazione della liveness prestazionale sotto stress).

## Limiti dichiarati

### Nessuna persistenza

Store e request table sono solo in memoria. Dopo un riavvio, il server perde sia i dati sia le risposte cached.

Conseguenza: un retry precedente al riavvio può essere trattato come prima esecuzione.

---

### Nessuna exactly-once distribuita

Il progetto garantisce **at-most-once execution locale entro finestra**.

Non garantisce:

- exactly-once distribuita;
- consenso;
- replica;
- recovery idempotente dopo crash;
- ordinamento globale tra client diversi.

---

### Nessuna autenticazione del client_id

Il server accetta il `client_id` dichiarato dal client. Un client malevolo potrebbe dichiarare l'identità di un altro client.

Questo è fuori dal contratto corrente.

---

## Conclusione

Il progetto garantisce che una richiesta mutativa ben formata, identificata da `(client_id, seq)`, produca il proprio effetto al massimo una volta entro la finestra mantenuta dal server.

La garanzia si basa su:

```text
request_id
payload canonico
response cached
lock sulla sezione check-then-apply
sliding window
eviction boundary
```

Questa combinazione rende sicuro il retry locale entro una singola istanza server e rende esplicite le condizioni fuori garanzia.
