# Architettura e Flusso Operativo del Sistema Distribuito

L'implementazione concreta di questa architettura distribuita si articola su tre componenti distinti che cooperano tramite socket TCP: il **Router**, lo **ShardNode** e il **Coordinator**.

---

## L'Implementazione (Struttura e Codice dei Componenti)

Nell'architettura distribuita, lo stato del sistema e la responsabilità della sincronizzazione sono rigidamente separati per garantire la scalabilità e l'isolamento dei guasti.

### A. Il Router (Il Gateway Stateful)
Il Router è l'unico punto di accesso per i client. È **stateless** rispetto ai dati applicativi (non memorizza chiavi-valore), ma è **stateful** per quanto riguarda la topologia del cluster e l'idempotenza.

* **Stato in memoria (`__init__` - `_active_topology`):** La lista ordinata dei nodi Shard attivi.
* **Stato in memoria (`__init__` - `_topology_new`):** Popolata solo durante il rebalancing per tracciare la topologia di destinazione.
* **Stato in memoria (`__init__` - `_requests`):** La request table nidificata.
* **Stato in memoria (`__init__` - `_evicted_until`):** L'eviction boundary per client per la sliding window.
* **Stato in memoria (`__init__` - `_global_version`):** Il contatore globale delle versioni del cluster.
* **Gestione dei Lock:** Utilizza tre lock dedicati: il `client_lock` (uno per ciascun `client_id` per serializzare i retry ed evitare chiamate di rete duplicate), il `_version_lock` (velocissimo, solo per incrementare la versione globale) e il `_topology_lock` (per leggere la topologia in modo thread-safe senza bloccare le operazioni di rete).

### B. Lo ShardNode / ShardStore (Lo Storage "Stupido")
Lo ShardNode è l'unità di memorizzazione fisica dei dati. È completamente passivo: ignora chi sia il client, non sa cosa sia un `request_id` e non ha nozione di rebalancing.

* **Stato in memoria:** Un semplice dizionario Python `self._data: dict[str, StoredValue]` che associa ad ogni chiave una dataclass contenente valore (stringa) e versione (intero).
* **Gestione dei Lock:** Applica un `key_lock` e un `structure_lock` a livello locale per garantire che le letture e le scritture sulla singola chiave siano thread-safe ed estremamente veloci.
* **La regola di scrittura:** Accetta una scrittura (`SHARD_SET`) solo se la versione proposta dal Router è strettamente maggiore della versione correntemente memorizzata per quella chiave.

### C. Il Coordinator (Il Motore di Migrazione)
Il Coordinator gestisce esclusivamente il Control Plane durante le fasi di transizione della topologia (rebalancing).

* **Ruolo operativo:** Non tocca mai il traffico dei client. Quando viene attivato dal Router, apre connessioni TCP verso lo ShardNode di origine, legge i dati tramite comandi speciali di dump (`SHARD_GET_ALL`), ricalcola lo shard di destinazione per ciascuna chiave e scrive i dati sul nuovo ShardNode preservandone la versione originale.

---

## Punto 4: Flusso di Funzionamento delle Operazioni

Il comportamento del Router si differenzia nettamente tra comandi di lettura (non mutativi) e comandi di scrittura (mutativi).

### Diagramma di Flusso Generale

```text
                ┌────────────────────────────────────────┐
                │        Client invia una Richiesta      │
                └───────────────────┬────────────────────┘
                                    │
                      Senza request_id? (Lettura)
                                    ├──────────────────────────────┐
                                    ▼ NO (Scrittura)               ▼ SI
                       ┌──────────────────────────┐   ┌──────────────────────────┐
                       │ Acquisisce client_lock   │   │ Calcola Shard su         │
                       │ specifico del client     │   │ Topologia Nuova          │
                       └────────────┬─────────────┘   └────────────┬─────────────┘
                                    │                              │
                     Request_id in Cache?                          │ Chiave Trovata?
                      ┌─────────────┴─────────────┐                ├─────────────┬
                   SI ▼                        NO ▼             SI ▼          NO ▼          
         ┌───────────────────┐       ┌────────────────────┐  ┌───────────┐ ┌───────────┐   
         │ Replay Risposta o │       │ seq <= Boundary?   │  │Restituisce│ │In corso   │   
         │ ERR_CONFLICT      │       ┌────────────┬───────┘  │Dato       │ │Rebalance? │   
         └───────────────────┘    SI ▼         NO ▼          └───────────┘ ├─────┬─────┤   
                               ┌───────────┐ ┌────────────────────┐     SI ▼       NO ▼│
                               │ERR_EXPIRED│ │In corso Rebalance? │  ┌───────────┐ ┌───────────┐
                               │           │ ├────────────┬───────┘  │Interroga  │ │ERR_NOT_   │
                               └───────────┘          YES ▼     NO ▼ │Topologia  │ │FOUND      │
                                        Is CAS_REQ?       │     │    │Vecchia    │ └───────────┘
                                        ┌─────┴─────┐     │     │    │(Fallback) │
                                     SI ▼        NO ▼     │     │    └─────┬─────┘
                               ┌───────────┐ ┌────────────┴─────┴───┐      │
                               │  ERR_     │ │ Invia a Shard        │      ▼
                               │REBALANCING│ │ con Versione Globale │   Trovata?
                               │           │ │                      │   ├───┬
                               └───────────┘ └──────────────────────┘  SI▼   NO▼
                                                                      ┌───┐ ┌───┐
                                                                      │OK │ │ERR│
                                                                      └───┘ └───┘

```

### A. Flusso delle Operazioni di Lettura (GET, GETV, KEYS)

Le letture sono naturalmente idempotenti e non richiedono `request_id` o acquisizione del `client_lock`.

* **Senza Rebalancing:** Il Router calcola lo shard responsabile della chiave tramite l'hashing modulo sulla topologia attiva (hash(key) % N).
* **Interrogazione Standard:** Invia una richiesta TCP sincrona allo ShardNode selezionato e restituisce la risposta direttamente al client.
* **Durante il Rebalancing (Read-Fallback):** Il Router interroga per primo lo ShardNode calcolato sulla topologia nuova (N+1).
* **Gestione Riscontro Positivo (Rebalance):** Se la chiave viene trovata (o se viene letto un Tombstone logico), la ricerca si ferma e restituisce il valore (o `ERR_NOT_FOUND` se è un tombstone).
* **Gestione Riscontro Negativo (Rebalance):** Se la chiave non è presente sul nuovo shard, il Router esegue il fallback di lettura interrogando lo ShardNode calcolato sulla topologia vecchia (N). Se presente la restituisce, altrimenti restituisce `ERR_NOT_FOUND`.

### B. Flusso delle Operazioni Mutative (SET_REQ, CAS_REQ, DELETE_REQ)

Le mutazioni richiedono tassativamente il `request_id` e passano attraverso la request table.

* **Parsing sintattico:** Avviene fuori da qualsiasi lock. Se mancano parametri, il server risponde immediatamente con `ERR usage:....`
* **Acquisizione del Lock:** Il Router acquisisce il lock specifico per quel `client_id`.
* **Verifica Cache (Idempotenza):** Controlla se la tupla (`client_id`, `seq`) è nella request table.
* **Esito Cache - Payload Identico:** Restituisce immediatamente la risposta cached (es. replay di successo o replay di errori passati).
* **Esito Cache - Payload Diverso:** Restituisce `ERR_REQUEST_ID_CONFLICT`.
* **Verifica Scadenza:** Controlla se `seq <= eviction_boundary[client_id]`. Se vero, restituisce `ERR_REQUEST_ID_EXPIRED`.
* **Instradamento con Rebalancing (CAS_REQ):** Il Router abortisce immediatamente l'operazione restituendo `ERR_REBALANCING`. Questa risposta non viene salvata in cache per consentire al client di ritentare al termine della migrazione.
* **Instradamento con Rebalancing (SET_REQ o DELETE_REQ):** Il Router instrada la richiesta direttamente sulla topologia nuova (N+1).
* **Instradamento senza Rebalancing:** Il Router instrada sulla topologia attiva corrente.
* **Esecuzione e Incremento Versione:** Il Router acquisisce il `_version_lock`, incrementa il Sequence Number globale e assegna questa nuova versione alla scrittura.
* **Invio allo ShardNode:** Invia il comando allo ShardNode selezionato.
* **Salvataggio in Cache:** Salva la risposta dello shard e il payload canonico nella request table.
* **Garbage Collection:** Esegue la pulizia inline della finestra se supera la dimensione massima prevista.
* **Rilascio e Risposta:** Rilascia il `client_lock` e risponde al client.
