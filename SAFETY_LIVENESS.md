# Proprietà di Safety e Liveness: KV Store con Retry Idempotenti

Questo documento formalizza le proprietà di correttezza garantite dal protocollo
descritto in `api-contract.md`. Per ogni proprietà viene indicata la motivazione
tecnica ("Perché vale") e la condizione che potrebbe violarla.

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

**Perché vale.** Il lookup nella request table e l'applicazione dell'effetto
allo store avvengono dentro la stessa sezione critica (stesso lock). La
sequenza è atomica rispetto a tutti gli altri thread:

```
1. Controlla request_table[(client_id, seq)]
   → trovato: restituisci la risposta memorizzata (STOP, nessun effetto)
2. Controlla eviction_boundary[client_id]
   → seq <= boundary: ERR request_id_expired (STOP, nessun effetto)
3. Applica l'effetto allo store
4. Costruisci la risposta
5. Salva (payload_canonico, risposta) in request_table
```

Poiché i passi 1–5 sono atomici, due thread che ricevono lo stesso
`request_id` contemporaneamente si serializzano: il secondo trova già la
voce in tabella e restituisce la risposta cached senza toccare lo store.

**Cosa potrebbe violarla.** Se il lock non coprisse l'intera sequenza 1–5,
due thread potrebbero superare entrambi il controllo al passo 1 e applicare
l'effetto due volte. Questa violazione non è possibile nell'implementazione
corretta.

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

**Perché vale.** La risposta viene salvata nella request table **dopo** che
l'effetto è stato applicato e **prima** che venga inviata al client (passi
4→5→6). Non esiste finestra temporale tra la modifica dello store e la
memorizzazione della risposta: i due passi sono dentro lo stesso lock.
Qualunque retry successivo trova la risposta già in tabella.

**Cosa potrebbe violarla.** Inviare la risposta al client prima di salvarla
in tabella aprirebbe una finestra in cui un retry potrebbe essere trattato
come prima esecuzione. Questa inversione è espressamente vietata dal design.

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

### L4 — L'idempotenza non sopravvive al failover Primary→Secondary

**Proprietà (limite dichiarato).** Se il Primary crasha e il Secondary si
promuove a nuovo Primary, la request table del nuovo Primary è **vuota**.
Un retry inviato dal client al nuovo Primary viene trattato come prima
esecuzione, con rischio di doppio effetto.

**Perché accade.** La request table è mantenuta esclusivamente in memoria
locale del nodo che riceve la richiesta. Il meccanismo di replicazione
trasferisce al Secondary solo gli effetti applicati allo store (le coppie
chiave/valore), ma non le voci della request table. Il Secondary non ha
modo di sapere quali `(client_id, seq)` il Primary aveva già elaborato.

Questo implica che la sequenza:

```
1. Client invia  SET_REQ clientA:42 corso val  → Primary risponde OK v=3
2. La risposta va persa (timeout / disconnessione)
3. Primary crasha → Secondary si promuove a Primary
4. Client fa retry: SET_REQ clientA:42 corso val
5. Nuovo Primary non ha la request table → applica l'effetto di nuovo
                                            → DOPPIO EFFETTO ❌
```

non è prevenuta dall'implementazione corrente.

**Cosa servirebbe per garantirla.** Eliminare questa limitazione richiederebbe:

1. **Replicare la request table**: ogni voce `(client_id, seq) → (payload, risposta)`
   deve essere propagata al Secondary insieme all'effetto sullo store;
2. **Replicazione sincrona**: il Primary deve rispondere `OK` al client solo dopo
   che il Secondary ha confermato di aver persistito sia l'effetto che la voce
   nella request table. La replicazione asincrona non è sufficiente perché
   lascia aperta la stessa finestra di rischio.

Queste estensioni non fanno parte del contratto corrente e non sono
implementate. Questo limite è coerente con la sezione *"Cosa non è garantito"*
del file `api-contract.md` che dichiara: *"Nessuna replica della request
table su altri nodi."*

---

## Tabella riassuntiva

| Proprietà | Garantita | Condizione |
|---|---|---|
| Nessun doppio effetto (S1) | Sì | `(client_id, seq)` nella finestra |
| Replay coerente con l'effetto (S2) | Sì | Incondizionata |
| Richieste distinte non si confondono (S3) | Sì | Incondizionata |
| Conflitto di payload rilevato (S4) | Sì | Incondizionata |
| Memoria limitata per client (L1) | Sì | Window size N applicata |
| GC non blocca il servizio (L2) | Sì | Eviction inline O(1) |
| Retry garantito per client corretto (L3) | Sì | Client rispetta finestra N |
| Sopravvivenza al riavvio | No | Request table solo in memoria |
| Garanzia dopo seq evictato (S1) | No | Fuori dalla sliding window |
| Ordinamento globale tra client | No | Design single-node |
| Idempotenza dopo failover Primary→Secondary (L4) | No | Request table non replicata sul Secondary |
