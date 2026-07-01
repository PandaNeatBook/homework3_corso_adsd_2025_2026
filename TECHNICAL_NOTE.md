# Nota Tecnica: Scelte di Design, Limiti e Possibili Evoluzioni

Questo documento descrive i trade-off accettati nella progettazione del
protocollo di retry idempotenti, i limiti che rimangono nella versione corrente,
e le evoluzioni possibili discusse ma non implementate.

---

## Il Problema Centrale

In un sistema distribuito un client non può distinguere tra tre scenari
dopo un timeout su una scrittura:

1. Il server non ha mai ricevuto la richiesta.
2. Il server ha ricevuto e applicato la richiesta, ma la risposta è andata persa.
3. Il server ha ricevuto la richiesta ma ha fallito prima di applicarla.

Senza idempotenza, un retry nel caso 2 produce un **doppio effetto**: un valore
scritto due volte, una versione incrementata due volte. Il meccanismo del
`request_id` converte un'operazione potenzialmente duplicata in un'operazione
provabilmente singola.

> La promessa introdotta è: un `SET_REQ`, `CAS_REQ` o `DELETE_REQ` con un
> dato `request_id` applica il proprio effetto allo store **al più una volta**,
> e i retry successivi ricevono la stessa risposta della prima esecuzione.

---

## Costi Accettati

| Costo | Descrizione |
|---|---|
| Memoria aggiuntiva | Il server mantiene una request table per client, limitata a N voci. Non è zero, ma è bounded. |
| Latenza leggermente maggiore | Ogni operazione mutativa esegue 2 lookup aggiuntivi nel dizionario (lettura e scrittura), entrambi O(1). |
| Lock contention aumentata | La request table condivide il lock dello store. Tutte le operazioni mutative serializzano. |
| Onere lato client | Il client deve generare e tracciare i valori di `seq`. Il server non offre aiuto in questa scelta. |

---

## Strategia di Garbage Collection: Sliding Window

### Strategie considerate

| Strategia | Memoria | Rischio correttezza | Complessità |
|---|---|---|---|
| Conserva tutte le voci per sempre | Illimitata | Nessuno | Minima |
| Scadenza temporale (TTL) | Limitata nel tempo | Dipende dalle assunzioni sul clock | Media |
| Sliding window (scelta) | Limitata per conteggio | Richiede disciplina del client | Bassa |
| ACK cumulativo esplicito | Limitata esplicitamente | Richiede un round-trip aggiuntivo | Media |

### Perché la sliding window

La sliding window è stata scelta perché:

- non richiede infrastruttura di clock o timer;
- fornisce un confine chiaro e testabile: esattamente `N` voci per client;
- il costo di eviction è O(1) per operazione, inline, senza background thread.

Il **rischio principale** della sliding window è che la dimensione della
finestra `N` diventa parte implicita del contratto client-server. Un client
che invia `N+1` richieste senza attendere il completamento del retry della
prima rompe silenziosamente la garanzia. Questo è dichiarato esplicitamente
nel contratto.

### Il ruolo dell'eviction boundary

La sliding window da sola non è sufficiente. Quando un `seq` non è presente
nella finestra, il server deve distinguere tra:

- `seq` **mai visto**: prima richiesta legittima → eseguire;
- `seq` **già evictato**: retry fuori finestra → `ERR request_id_expired`.

Senza un campo aggiuntivo, i due casi sarebbero indistinguibili. L'**eviction
boundary** (low-watermark) risolve il problema: tiene traccia del massimo `seq`
mai evictato per ogni client. La logica di controllo diventa:

```
se seq è in finestra           → replay
se seq <= eviction_boundary    → ERR request_id_expired
altrimenti                     → prima esecuzione
```

Se un `client_id` non ha mai subito eviction, `eviction_boundary` è
considerato -1: nessuna prima richiesta valida (con `seq >= 0`) viene
erroneamente rifiutata come scaduta.

---

## Design del Lock

Un unico `threading.Lock` protegge sia `_data` (il KV store) sia la request
table. Questa scelta è intenzionale: il lookup, la scrittura sullo store e la
memorizzazione della risposta devono apparire atomici rispetto ai thread
concorrenti.

Separare il lock dello store dal lock della request table introdurrebbe una
finestra in cui due thread potrebbero entrambi superare il controllo sulla
request table e applicare l'effetto due volte, violando S1.

Il costo è che tutte le operazioni mutative serializzano globalmente. Per
un'implementazione didattica con un numero limitato di client concorrenti,
questo è accettabile.

---

## Scelte Lasciate all'Implementatore

Le seguenti scelte sono libere, purché documentate:

| Scelta | Valore scelto | Motivazione |
|---|---|---|
| Window size N | 100 | Valore di default; configurabile. Bilanciamento tra memoria usata e durata della garanzia. |
| Implementazione del comando `ACK` | Non implementato (opzionale) | Aggiunge complessità semantica non necessaria per la versione base. |
| Accettazione di `seq` non monotoni | Accettati | Il contratto non li vieta; la window sliding può evictarli prima del previsto. |

---

## Limiti della Versione Corrente

| Limite | Descrizione |
|---|---|
| Nessuna persistenza | La request table e il KV store sono in memoria. Un riavvio del server azzera tutto. Un retry dopo il riavvio viene trattato come prima esecuzione. |
| Nessuna autenticazione del `client_id` | Il server accetta il `client_id` dichiarato dal client senza verifica. Un client malintenzionato può impersonare un altro. |
| Single-node | La garanzia di idempotenza è locale a una singola istanza del server. In un sistema replicato, la request table dovrebbe essere replicata o centralizzata. |
| `N` è un parametro statico | Modificare la window size richiede il riavvio del server e la consapevolezza coordinata dei client. |

---

## Possibili Evoluzioni

### Persistenza della request table

Scrivere ogni nuova voce della request table su un append-only log prima
di rispondere al client. Al riavvio, replay del log per ricostruire la
tabella. Questo renderebbe la garanzia di idempotenza sopravvivere ai crash
del server, al costo di un'operazione di I/O su ogni operazione mutativa.

### ACK cumulativo

Il comando `ACK <client_id> <seq>` (già descritto nel contratto come
estensione opzionale) permette al client di liberare esplicitamente le voci
dalla finestra. Questo rimuove il vincolo implicito sulla dimensione della
finestra e rende il protocollo di pulizia esplicito, ma richiede un round-trip
aggiuntivo e complessità semantica in presenza di retry ancora in volo.

### Lock per client

Sostituire il lock globale con un lock per `client_id` sulla request table,
combinato con un lock per chiave sullo store. Aumenta il parallelismo tra
client diversi, ma richiede un ordinamento disciplinato delle acquisizioni
per evitare deadlock.

### Idempotenza distribuita

In un sistema replicato o shardato, la request table deve essere replicata
o collocata su un coordinatore che tutte le repliche consultano. Questo è
il problema dell'"exactly-once" distribuito e richiede coordinamento a livello
di consenso (Paxos, Raft).

---

## Relazione con gli Argomenti del Corso

| Argomento del corso | Rilevanza |
|---|---|
| KV store con CAS e versioning (lezione 17) | I comandi idempotenti estendono direttamente il CAS store: `CAS_REQ` aggiunge idempotenza a `CAS`. |
| Sezioni critiche e safety (lezione 10) | Il lock che protegge la sequenza check-then-apply è lo stesso meccanismo discusso per `INCR`. |
| Replica primary-secondary (lezione 12) | L'idempotenza è necessaria ma non sufficiente per i retry sicuri in un sistema replicato. |
| Quorum (lezione 14) | Una scrittura con quorum non è idempotente a meno che ogni replica implementi deduplicazione. |
| Clock logici e `seq` (lezione 21) | Il `seq` svolge un ruolo analogo a un timestamp logico: impone ordine e abilita deduplicazione senza clock fisici condivisi. |
