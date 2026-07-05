```markdown
# Proprietà di Safety e Liveness: Router, Idempotenza e Rebalancing

Questo documento descrive le proprietà di correttezza architetturale del progetto **KV Store Distribuito con Gateway (Router), Rebalancing e Retry Idempotenti**.

Il sistema è passato da un singolo nodo a un'architettura distribuita composta da un **Router** (che gestisce l'idempotenza e l'instradamento), un **Coordinator** (che gestisce la migrazione dei dati) e molteplici **ShardNode** (che memorizzano fisicamente il dato). L'obiettivo è garantire transizioni di topologia sicure, prevenire la perdita o la resurrezione di dati e impedire che il retry di una richiesta mutativa produca due volte lo stesso effetto.

---

## Proprietà di Safety

Le proprietà di safety affermano che **non accade mai qualcosa di scorretto**.
In questo sistema, "scorretto" significa: un effetto applicato più di una volta, perdita di dati durante la migrazione, resurrezione di dati cancellati, o risposte incoerenti ai retry.

---

### S1 — Nessun doppio effetto (Idempotenza di Rete)

**Proprietà.** Un'operazione mutativa identificata da `(client_id, seq)` viene inoltrata agli ShardNode **al più una volta**, indipendentemente dai retry del client.

**Perché vale.** La garanzia è applicata sul Router. Il lookup nella request table e l'inoltro di rete sono protetti dal `client_lock`:

```text
1. Acquisizione del `client_lock` per il client_id specifico sul Router.
2. Controlla request_table[(client_id, seq)]
   → trovato: restituisci la risposta memorizzata (STOP, nessuna rete).
3. Controlla eviction_boundary[client_id]
   → seq <= boundary: ERR_REQUEST_ID_EXPIRED (STOP, nessuna rete).
4. Fotografia lock-free della topologia di routing.
5. Generazione del Sequence Number globale.
6. Chiamata di rete (TCP) allo ShardNode di destinazione.
7. Costruisci e salva (payload_canonico, risposta) in request_table.
8. Rilascio del `client_lock`.

```

Due thread che ricevono lo stesso `request_id` contemporaneamente sul Router si serializzano: il secondo trova già la voce in tabella e restituisce la risposta cached senza contattare gli shard.

---

### S2 — Il replay è coerente con l'effetto applicato

**Proprietà.** La risposta restituita al retry è sempre quella prodotta durante la prima esecuzione, anche se lo stato dello ShardNode è cambiato.

* Se la prima esecuzione ha prodotto `OK version=3`, il retry riceve `OK version=3`.
* Se la prima esecuzione ha prodotto `ERR_CAS_CONFLICT current=5`, il retry riceve lo stesso errore.
* Se la prima esecuzione ha prodotto `ERR_NOT_FOUND`, il retry riceve `ERR_NOT_FOUND`.

**Eccezione di transitorietà:** La risposta `ERR_REBALANCING` (generata quando una CAS è bloccata per migrazione in corso) **non** viene salvata nella request table, permettendo al client di ritentare legittimamente a rebalance concluso.

---

### S3 — Richieste diverse non si confondono

**Proprietà.** Due operazioni con `seq` diversi, o di `client_id` diversi, sono trattate come indipendenti. L'indice della cache sul Router è la tupla `(client_id, seq)`.

---

### S4 — Il conflitto di payload viene rilevato e segnalato

**Proprietà.** Se lo stesso `request_id` viene riutilizzato con un payload diverso, il Router risponde `ERR_REQUEST_ID_CONFLICT` senza inoltrare nulla agli ShardNode. Il payload canonico viene memorizzato insieme alla risposta proprio per permettere questo controllo.

---

### S5 — Retry fuori finestra non rieseguito

**Proprietà.** Se una richiesta è stata evictata (finestra piena), un retry successivo riceve in modo deterministico `ERR_REQUEST_ID_EXPIRED` controllando l'`eviction_boundary[client_id]`. Il Router non tenta mai di ri-eseguirla.

---

### S6 — Prevenzione dei "Dati Zombie" (Tombstone)

**Proprietà.** Un dato cancellato non riappare mai magicamente durante il fallback di lettura di un rebalance.

**Perché vale.** Nel sistema distribuito, `DELETE_REQ` non cancella fisicamente la chiave, ma esegue una `SET` del valore speciale `<TOMBSTONE>` con una versione aggiornata globale. Durante il rebalance, il Router legge la topologia nuova: se trova il Tombstone, sa che l'assenza è autoritativa e **non** fa fallback sulla topologia vecchia, impedendo di leggere e resuscitare un dato obsoleto non ancora eliminato.

---

### S7 — Migrazione Sicura (Two-Phase Commit Locale)

**Proprietà.** Il fallimento del Coordinator durante un rebalance non causa mai perdita di dati.

**Perché vale.** La migrazione segue 3 fasi rigorose:

1. **Prepare (Copia):** Il Coordinator copia i dati sui nuovi shard, ma *non* li rimuove dai vecchi.
2. **Commit:** Il Coordinator notifica al Router l'`ACK_REBALANCE_END`. Il Router cambia la topologia. (Punto di non ritorno).
3. **Cleanup:** Solo ora il Coordinator rimuove fisicamente i vecchi dati. Se crasha in fase 1 o 2, i dati originali sono ancora intatti e il Router abortisce la migrazione leggendo la topologia vecchia.

---

## Proprietà di Liveness

Le proprietà di liveness affermano che **il sistema fa progresso e un client corretto può completare le proprie operazioni**, anche in presenza di guasti parziali.

---

### L1 — La memoria del Gateway (Router) è limitata

**Proprietà.** Per ogni `client_id`, la request table contiene al più `N` voci (sliding window). L'eviction costa O(1) e avviene inline. Un client non può esaurire la RAM del Router.

---

### L2 — Il Rebalancing non blocca il sistema per sempre (Watchdog)

**Proprietà.** Se il Coordinator crasha "silenziosamente" durante una migrazione, il sistema non rimane incastrato permanentemente nello stato `_rebalancing = True` (che bloccherebbe a vita le CAS e appesantirebbe le GET).

**Perché vale.** Il Router possiede un thread di **Watchdog** che, avviato all'`ACK_REBALANCE_START`, pinge costantemente il control plane del Coordinator. Se il Coordinator non risponde per `N` tentativi consecutivi, il Router abortisce autonomamente la migrazione ripristinando la topologia iniziale e sbloccando il traffico.

---

### L3 — Un client corretto può sempre completare una sequenza di retry

**Proprietà.** Un client riceverà sempre la risposta cached (senza side-effect ripetuti) purché non faccia avanzare il proprio `seq` di più di `N` (window size) posizioni senza aver prima ricevuto l'acknowledgement della prima richiesta.

---

### L4 — Liveness dei Lock nel Sistema Distribuito

**Proprietà.** Il sistema minimizza i punti di contesa globale, garantendo parallelismo massivo.

**Perché vale.**

* **Sul Router:** I `client_lock` isolano le sessioni. L'incremento del Sequence Number globale usa un lock velocissimo (`_version_lock`) isolato dall'I/O di rete. Le richieste di client diversi non si bloccano mai a vicenda.
* **Sugli ShardNode:** Le scritture e letture sono protette da `key_locks` specifici per singola chiave. Milioni di chiavi diverse possono essere aggiornate in parallelo senza contesa.

---

## Limiti dichiarati

### Nessuna persistenza

Router e ShardNode operano in RAM. Un riavvio azzera i dati (ShardNode) e la tabella di idempotenza (Router). Le garanzie non sopravvivono a un crash completo del processo.

### Limitazioni del Sequence Number Globale

Il Router centralizza la generazione delle versioni (`_global_version`). In sistemi su scala planetaria, questo formerebbe un collo di bottiglia prestazionale (si prediligono clock vettoriali o timestamp HLC), ma è accettabile nell'ambito del presente protocollo.

### Nessuna Autenticazione

Il Router si fida ciecamente del `client_id` dichiarato. La "spoofing" dell'identità non è mitigata a livello applicativo.
