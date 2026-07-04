# Nota Tecnica: Scelte di Design, Limiti e Possibili Evoluzioni

Questa nota descrive le scelte tecniche principali del progetto **KV Store con retry idempotenti tramite `request_id`**.

Il sistema implementato è un server TCP single-node, multithread, con stato in memoria e operazioni mutative idempotenti.

---

## 1. Problema affrontato

In un sistema distribuito un client può inviare una scrittura, perdere la risposta e non sapere se il server l'abbia già applicata.

Senza un meccanismo di deduplicazione, un retry può produrre un doppio effetto.

Esempio:

```text
SET corso ads   -> OK version=0   (risposta persa)
SET corso ads   -> OK version=1   (retry trattato come nuova scrittura)
```

Il valore finale può sembrare corretto, ma la versione è stata incrementata due volte.

La promessa del progetto è:

```text
Una richiesta mutativa con request_id applica il proprio effetto al massimo una volta.
Un retry identico riceve la stessa risposta della prima esecuzione.
```

---

## 2. Soluzione scelta

Ogni operazione mutativa usa un identificatore:

```text
<client_id>:<seq>
```

Esempi:

```text
SET_REQ clientA:0 corso ads
CAS_REQ clientA:1 corso 0 sistemi-distribuiti
DELETE_REQ clientA:2 corso
```

Il server mantiene una request table:

```text
request_table[client_id][seq] = (payload_canonico, response)
```

Quando arriva una richiesta mutativa:

```text
1. se request_id è già presente e il payload coincide:
      replay della risposta salvata;

2. se request_id è già presente ma il payload è diverso:
      ERR request_id_conflict;

3. se request_id è troppo vecchio ed è già stato evictato:
      ERR request_id_expired;

4. altrimenti:
      applicazione normale, salvataggio della risposta e possibile eviction.
```

---

## 3. Scelte implementative principali

| Scelta | Motivazione |
|---|---|
| Server single-node | La traccia riguarda i retry idempotenti, non replica o consenso |
| Request table in memoria | Soluzione semplice e sufficiente per dimostrare il contratto |
| Payload canonico salvato | Serve a distinguere retry legittimo e riuso errato dello stesso request_id |
| Response cached salvata | Serve a restituire al retry la risposta storica, non una risposta ricalcolata |
| Lock globale | Garantisce atomicità tra controllo request table e applicazione allo store |
| Sliding window per client | Limita la memoria usata dalla request table |
| Eviction boundary | Evita che un retry vecchio venga rieseguito come nuova richiesta |

---

## 4. Lock e concorrenza

Il server è multithread: ogni connessione client viene gestita da un thread.

Per evitare race condition, un unico lock protegge:

```text
_data
_request_table
_eviction_boundary
```

La sezione critica copre:

```text
controllo request_id -> applicazione effetto -> salvataggio risposta -> eviction
```

Questa scelta riduce il parallelismo, ma rende semplice e sicura la proprietà principale: **nessun doppio effetto**.

---

## 5. Versioni e CAS

Ogni chiave ha una versione locale.

Regole:

```text
chiave assente                  -> versione implicita -1
prima SET_REQ su chiave assente -> version=0
scrittura successiva            -> version+1
DELETE_REQ                      -> rimuove la chiave
SET_REQ dopo DELETE_REQ         -> riparte da version=0
```

`CAS_REQ` aggiorna una chiave solo se la versione corrente coincide con quella attesa.

Anche una `CAS_REQ` fallita per `version_mismatch` viene salvata nella request table, così il retry restituisce lo stesso errore.

---

## 6. Garbage collection

Il server non conserva tutti i `request_id` per sempre.

Per ogni client mantiene al massimo `N` richieste recenti.

Default:

```text
N = 100
```

Quando la finestra supera `N`, viene rimossa la voce con sequence number minimo.

Per evitare che un retry evictato venga rieseguito, il server mantiene:

```text
eviction_boundary[client_id]
```

Se arriva:

```text
seq <= eviction_boundary[client_id]
```

il server risponde:

```text
ERR request_id_expired
```

Nella versione corrente, l'eviction cerca il `seq` minimo nella finestra del client. Il costo è quindi `O(N)`, con `N` bounded e configurabile.

---

## 7. Limiti dichiarati

| Limite | Conseguenza |
|---|---|
| Request table solo in memoria | La garanzia non sopravvive al riavvio |
| Nessuna persistenza dello store | I dati vengono persi al riavvio |
| Sistema single-node | La garanzia è locale a una sola istanza |
| Nessuna replica della request table | Non c'è idempotenza dopo failover |
| Nessuna autenticazione del client_id | Un client può dichiarare un id arbitrario |
| Nessun ordinamento globale tra client | Le versioni sono locali alle chiavi |

---

## 8. Possibili evoluzioni

- **Persistenza della request table**: usare un log append-only per ricostruire le risposte cached dopo un crash.
- **ACK cumulativo**: introdurre `ACK <client_id> <seq>` per liberare esplicitamente vecchi request id.
- **Lock più fini**: usare lock per client o per chiave, aumentando il parallelismo ma anche la complessità.
- **Replica della request table**: necessaria per garantire idempotenza anche dopo failover.
- **Consenso distribuito**: necessario per estendere la proprietà a più nodi in modo forte.

---

## 9. Collegamento con il corso

Il progetto usa direttamente concetti centrali del corso:

| Argomento | Collegamento |
|---|---|
| KV store | Interfaccia testuale e stato chiave-valore |
| Thread e sezioni critiche | Lock per proteggere stato condiviso |
| Versioning e CAS | `GETV` e `CAS_REQ` |
| Safety | Nessun doppio effetto e replay coerente |
| Liveness | Memoria bounded e retry entro finestra |
| Replica/failover | Dichiarati come limiti non implementati |
| Clock logici | `seq` come ordinamento locale del client |

---

## 10. Sintesi

Il progetto non promette exactly-once distribuita.

Promette una garanzia più limitata ma precisa:

```text
entro una singola istanza server,
finché il request_id resta nella finestra,
una richiesta mutativa ben formata produce il proprio effetto al massimo una volta.
```

Questa garanzia è realizzata tramite:

```text
request_id + payload canonico + response cached + lock + sliding window + eviction boundary
```
