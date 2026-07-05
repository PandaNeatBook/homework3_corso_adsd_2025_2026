# Nota Tecnica: Scelte di Design, Limiti e Possibili Evoluzioni

Questo documento descrive i trade-off accettati nella progettazione del protocollo del **KV Store Distribuito con Gateway, Rebalancing e Retry Idempotenti**, i limiti che rimangono nella versione corrente e le evoluzioni possibili per scenari di produzione reali.

Il sistema implementato è un'architettura distribuita composta da un **Router** (Gateway stateless rispetto ai dati, ma stateful per l'idempotenza e la topologia), un **Coordinator** (motore di migrazione) e molteplici **ShardNode** (nodi di storage in memoria).

---

## Il Problema Centrale

In un sistema distribuito, le incertezze di rete si moltiplicano. Un client non può distinguere tra vari scenari dopo un timeout su una scrittura, e senza idempotenza un retry rischia di produrre un **doppio effetto**.

A questo si aggiunge la necessità di **scalare orizzontalmente** (aggiungere/rimuovere nodi) senza disservizi. Durante un rebalancing, i dati si spostano, e una richiesta (nuova o ritentata) deve essere instradata correttamente senza che il client legga dati vecchi o scriva su nodi non più competenti.

> La promessa del sistema è: un `SET_REQ`, `CAS_REQ` o `DELETE_REQ` applica il proprio effetto allo store **al più una volta**. Il Router maschera interamente la complessità della topologia e del rebalancing, offrendo un'interfaccia lineare e coerente.

---

## Costi Accettati

| Costo | Descrizione |
| :--- | :--- |
| **Memoria aggiuntiva sul Gateway** | Il Router mantiene una request table per client, limitata a N voci (sliding window). |
| **Overhead di Rete** | Ogni operazione mutativa richiede almeno un hop di rete interno (Router → ShardNode). Il Router apre e chiude connessioni TCP sincrone verso i nodi dati per ogni comando, impattando la latenza. |
| **Collo di Bottiglia Globale** | Il Router calcola centralmente il Sequence Number globale. Questo limita le scritture massime teoriche del cluster alle prestazioni di calcolo/lock del Router stesso. |
| **Complessità Architetturale** | La migrazione a caldo richiede fallback in lettura, blocchi mirati (CAS sospese durante il rebalance) e l'uso di Tombstone logici per la coerenza. |

---

## Strategia di Garbage Collection: Sliding Window

La gestione della memoria per la request table sul **Router** segue la logica della Sliding Window con un *eviction boundary* (`_eviction_boundary`).

Questa scelta è stata mantenuta rispetto al sistema a nodo singolo perché:
- Non richiede infrastruttura di clock o timer distribuiti.
- Il costo di eviction è O(1) per operazione, eseguito *inline* sul Router senza background thread.
- Definisce un contratto rigoroso col client: se il client avanza di più di `N` richieste senza attendere gli ACK, i retry delle richieste vecchie verranno rigettati in modo sicuro e deterministico (`ERR_REQUEST_ID_EXPIRED`).

---

## Design del Lock, Rebalancing e Architettura Distribuita

L'architettura ha richiesto una riprogettazione totale dei lock, dividendoli tra i componenti del sistema per massimizzare la Liveness.

### Sul Router (Gateway)
- **`client_lock`:** Isola le richieste dello stesso client. Include la verifica in cache, l'inoltro sincrono della chiamata di rete verso lo ShardNode e il salvataggio della risposta. Questo garantisce che due retry identici concorrenti non inneschino mai due chiamate di rete verso i nodi dati.
- **`_version_lock`:** Lock microscopico e rapidissimo per incrementare il Sequence Number globale per le mutazioni.
- **`_topology_lock`:** Protegge la topologia di routing. Viene acquisito per istanti brevissimi (es. in `_routing_snapshot()`) in modo lock-free rispetto all'I/O di rete, per decidere dove inviare i dati senza bloccare il traffico globale.

### Sugli ShardNode (Data Plane)
- **`key_lock` e `structure_lock`:** Gli ShardNode ignorano client e topologie. Proteggono semplicemente l'integrità del dizionario locale a grana fine per chiave, garantendo altissimo parallelismo.

---

## Sequence Number Globale e Tombstone

A differenza del sistema a nodo singolo (dove le versioni erano locali alla chiave e resettate alla cancellazione), in un sistema distribuito con migrazioni questo approccio causa la **corruzione dei dati (Zombie Data)**.

**Regole Distribuite:**
1. **Versione Globale:** Il Router assegna una versione strettamente crescente ad ogni scrittura. Lo ShardNode accetta la scrittura solo se la versione in ingresso è `> ` della versione attualmente memorizzata.
2. **Tombstone logici:** Il comando `DELETE_REQ` non rimuove fisicamente la chiave, ma scrive un valore sentinella `<TOMBSTONE>` con una versione globale incrementata.
3. **Lettura con Fallback:** Durante un rebalance, il Router legge prima dal nodo *nuovo* e poi dal nodo *vecchio*. Se il Router trovasse una chiave assente sul nodo nuovo, farebbe fallback sul vecchio, **resuscitando** un dato appena cancellato. Il Tombstone sul nodo nuovo blocca il fallback: informa il Router che il dato è esplicitamente assente. Il cleanup fisico avviene solo a rebalance concluso, in modo asincrono.

---

## Migrazione Sicura (Two-Phase Commit)

Per garantire la Safety in caso di guasti hardware durante il rebalancing, il Coordinator usa un pattern in 3 step:
1. **Prepare:** Copia i dati dai vecchi ai nuovi shard. Nessun dato originale viene cancellato.
2. **Commit:** Invia `ACK_REBALANCE_END` al Router, che accetta la topologia.
3. **Cleanup:** Il Coordinator elimina le copie vecchie.

Se il Coordinator o la rete falliscono nelle prime due fasi, un **Watchdog** sul Router si accorge del timeout e abortisce la migrazione, ripristinando la vecchia topologia. Poiché la Fase 3 non era ancora iniziata, nessun dato è andato perso.

---

## Limiti della Versione Corrente

| Limite | Descrizione |
| :--- | :--- |
| **Nessuna Persistenza** | L'intero cluster gira in RAM. Un crash di uno ShardNode comporta la perdita della sua partizione di dati. Un crash del Router azzera la request table, rompendo la garanzia di idempotenza per le richieste in volo al momento del crash. |
| **Router come SPOF** | Il Router è un Single Point of Failure (SPOF) sia per la raggiungibilità del sistema sia per il collo di bottiglia generato dal `_version_lock` globale. |
| **Overhead TCP (`send_line`)** | L'apertura e chiusura di un socket TCP per ogni singolo comando interno Router ↔ ShardNode consuma porte effimere e devasta il throughput massimo. |
| **Hashing Modulo (N)** | L'assegnazione delle chiavi usa `hash(key) % N`. Aggiungere un nodo rimescola una quantità massiccia di chiavi in tutto il cluster, saturando la rete durante il rebalance. |

---

## Possibili Evoluzioni (Produzione)

### 1. Consistent Hashing (Topologia ad Anello)
Sostituire il semplice modulo con il Consistent Hashing (es. Hash Ring) ridurrebbe drasticamente la quantità di dati da migrare durante l'aggiunta o rimozione di un nodo (solo le chiavi nell'arco di anello adiacente si sposterebbero, non l'intero database).

### 2. Versionamento Distribuito (HLC / Vector Clocks)
Rimuovere il `_global_version` dal Router. Demandare il versionamento a clock ibridi logico-fisici (Hybrid Logical Clocks) generati dai nodi o timestamps locali per partizione, eliminando il collo di bottiglia globale delle scritture.

### 3. Connection Pooling / Keep-Alive
Introdurre un pool di connessioni persistenti sul Router (es. tramite HTTP/2 multiplexing, gRPC o connessioni TCP riutilizzabili) per azzerare la latenza di handshake e teardown dei socket verso gli ShardNode.

### 4. High Availability del Router (Raft/Paxos)
Rendere il Router replicato. Utilizzare un protocollo di consenso come Raft per mantenere sincronizzata la `request_table` e lo stato della topologia su un cluster di 3 o 5 Router, garantendo idempotenza e operatività anche se il Router primario esplode.