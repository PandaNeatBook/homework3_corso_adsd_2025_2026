# Proprietà di Safety e Liveness

Questo documento descrive le proprietà di correttezza del progetto **KV Store con retry idempotenti tramite `request_id`**.

Il sistema è un Key-Value Store single-node, TCP, multithread, con stato in memoria. L'obiettivo è impedire che il retry di una richiesta mutativa produca due volte lo stesso effetto.

---

## 1. Modello considerato

Il server mantiene:

```text
_data[key] = (value, version)
_request_table[client_id][seq] = (payload_canonico, response)
_eviction_boundary[client_id] = massimo seq già eliminato dalla finestra
```

Le operazioni mutative idempotenti sono:

```text
SET_REQ <client_id>:<seq> <key> <value...>
CAS_REQ <client_id>:<seq> <key> <expected_version> <value...>
DELETE_REQ <client_id>:<seq> <key>
```

Le operazioni di lettura (`GET`, `GETV`, `EXISTS`, `KEYS`, `STATS`, `PING`) non transitano per la request table.

La garanzia vale solo:

- finché il server resta in esecuzione;
- finché la richiesta è ancora nella finestra mantenuta dal server;
- per richieste mutative ben formate con `request_id` valido.

---

# 2. Proprietà di Safety

Le proprietà di safety indicano cosa non deve mai accadere.

Nel progetto, gli errori da evitare sono:

- applicare due volte la stessa richiesta mutativa;
- confondere richieste diverse;
- rispondere a un retry con una risposta incoerente;
- rieseguire una richiesta già evictata;
- accettare lo stesso `request_id` con payload diverso.

---

## S1 — Nessun doppio effetto

**Proprietà.** Una richiesta mutativa identificata da `(client_id, seq)` viene applicata allo store al massimo una volta, finché resta nella finestra del server.

Esempio:

```text
SET_REQ clientA:0 corso ads   -> OK version=0
SET_REQ clientA:0 corso ads   -> OK version=0
GETV corso                    -> OK version=0 ads
```

La versione resta `0`, quindi il retry non ha prodotto una seconda scrittura.

**Motivazione.** Il server controlla la request table prima di applicare l'operazione. Se trova già `(client_id, seq)` con lo stesso payload, restituisce la risposta salvata e non modifica `_data`.

Il controllo della request table, l'applicazione dell'effetto e il salvataggio della risposta avvengono dentro lo stesso lock. Questo impedisce che due thread applichino contemporaneamente la stessa richiesta.

---

## S2 — Replay coerente

**Proprietà.** Un retry con stesso `request_id` e stesso payload riceve esattamente la risposta prodotta durante la prima esecuzione.

La risposta viene riprodotta anche se lo stato dello store è cambiato nel frattempo.

Esempio:

```text
SET_REQ clientA:0 k v1  -> OK version=0
SET_REQ clientA:1 k v2  -> OK version=1
SET_REQ clientA:0 k v1  -> OK version=0
GETV k                  -> OK version=1 v2
```

Il retry restituisce la risposta storica `OK version=0`, ma lo stato corrente resta quello più recente.

**Motivazione.** La request table salva anche la risposta:

```text
request_table[client_id][seq] = (payload_canonico, response)
```

Al retry, il server non ricalcola l'esito sullo stato corrente: restituisce la risposta già salvata.

---

## S3 — Richieste diverse non si confondono

**Proprietà.** Due richieste sono considerate uguali solo se coincidono `client_id`, `seq` e payload canonico.

Esempio:

```text
SET_REQ clientA:0 course ads      -> OK version=0
SET_REQ clientB:0 course systems  -> OK version=1
```

`clientA:0` e `clientB:0` sono richieste distinte, anche se usano lo stesso sequence number.

**Motivazione.** La request table è indicizzata per client e sequence number:

```text
request_table[client_id][seq]
```

La chiave KV non identifica la richiesta.

---

## S4 — Conflitto di payload rilevato

**Proprietà.** Se lo stesso `request_id` viene riutilizzato con payload diverso, il server restituisce:

```text
ERR request_id_conflict
```

e non modifica lo store.

Esempio:

```text
SET_REQ clientA:0 corso ads      -> OK version=0
SET_REQ clientA:0 corso systems  -> ERR request_id_conflict
GETV corso                       -> OK version=0 ads
```

**Motivazione.** Il server salva il payload canonico della prima richiesta. Al retry confronta il nuovo payload con quello salvato. Se sono diversi, non è un retry legittimo.

---

## S5 — Retry fuori finestra non rieseguito

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

## S6 — Errori di parsing non memorizzati

**Proprietà.** Gli errori sintattici o di parsing non vengono salvati nella request table.

Esempi:

```text
ERR malformed
ERR bad_request_id
ERR bad_version
```

**Motivazione.** La request table viene aggiornata solo per richieste mutative ben formate. Se il client corregge una richiesta malformata e la reinvia con lo stesso `request_id`, il server la tratta come prima richiesta valida.

---

# 3. Proprietà di Liveness

Le proprietà di liveness indicano cosa deve poter continuare ad accadere.

Nel progetto, la liveness riguarda:

- memoria limitata;
- garbage collection non bloccante;
- completamento dei retry entro finestra;
- risposta esplicita anche per richieste scadute o invalide.

---

## L1 — Memoria limitata per client

**Proprietà.** Per ogni `client_id`, il server conserva al massimo `N` richieste nella request table.

Default:

```text
N = 100
```

**Motivazione.** Dopo ogni nuova richiesta mutativa valida, il server controlla la dimensione della finestra del client. Se supera `N`, elimina la voce con `seq` minimo.

Questo impedisce crescita illimitata della memoria.

---

## L2 — Garbage collection locale e bounded

**Proprietà.** La garbage collection è locale alla finestra del singolo client.

Non richiede:

- comunicazione di rete;
- thread di background;
- timer;
- scansioni globali dello store.

Nella versione corrente, il server cerca il `seq` minimo nella finestra del client. Il costo è:

```text
O(N)
```

con `N` bounded e configurabile.

---

## L3 — Client corretto completa i retry entro finestra

**Proprietà.** Un client che usa sequence number monotoni e ritenta prima che la richiesta esca dalla finestra riceve la risposta salvata.

Esempio: con `N = 100`, se un client invia `clientA:42` e lo ritenta prima di emettere più di 100 nuove richieste logiche, il server può ancora fare replay.

Se il client ritenta troppo tardi, riceve:

```text
ERR request_id_expired
```

Questa è una condizione fuori dalla garanzia dichiarata, non un blocco del server.

---

## L4 — Errori terminali non bloccano il servizio

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

# 4. Limiti dichiarati

## Nessuna persistenza

Store e request table sono solo in memoria. Dopo un riavvio, il server perde sia i dati sia le risposte cached.

Conseguenza: un retry precedente al riavvio può essere trattato come prima esecuzione.

---

## Nessuna idempotenza dopo failover

Il sistema è single-node e non replica la request table.

In un sistema primary-secondary, se il primary applica una richiesta ma crasha prima che il client riceva la risposta, un secondary promosso senza request table non saprebbe che quel `request_id` è già stato eseguito.

Per garantire idempotenza dopo failover servirebbe replicare anche:

```text
(client_id, seq) -> (payload_canonico, response)
```

---

## Nessuna exactly-once distribuita

Il progetto garantisce **at-most-once execution locale entro finestra**.

Non garantisce:

- exactly-once distribuita;
- consenso;
- replica;
- recovery idempotente dopo crash;
- ordinamento globale tra client diversi.

---

## Nessuna autenticazione del client_id

Il server accetta il `client_id` dichiarato dal client. Un client malevolo potrebbe dichiarare l'identità di un altro client.

Questo è fuori dal contratto corrente.

---

# 5. Tabella riassuntiva

| Proprietà | Garantita | Condizione |
|---|---|---|
| Nessun doppio effetto | Sì | Richiesta nella finestra |
| Replay coerente | Sì | Stesso `request_id`, stesso payload |
| Conflitto di payload rilevato | Sì | Stesso `request_id`, payload diverso |
| Client diversi non confusi | Sì | `client_id` diverso |
| Retry fuori finestra non rieseguito | Sì | Uso di `eviction_boundary` |
| Memoria limitata | Sì | Finestra di dimensione `N` |
| GC locale e bounded | Sì | Costo `O(N)`, con `N` bounded |
| Sopravvivenza al riavvio | No | Request table in memoria |
| Idempotenza dopo failover | No | Request table non replicata |
| Exactly-once distribuita | No | Fuori dal contratto |
| Autenticazione client | No | Fuori dal contratto |

---

# 6. Conclusione

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
