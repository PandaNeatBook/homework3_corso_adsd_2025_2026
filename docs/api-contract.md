# Contratto del Protocollo: Router Gateway, Idempotenza e Rebalancing

Questa versione estende il protocollo del KV store versionato introducendo un'architettura distribuita a shard (nodi multipli), il supporto alla migrazione a caldo dei dati (rebalancing) e l'idempotenza delle operazioni mutative.

Il problema principale dell'idempotenza rimane invariato: un client può inviare una scrittura, perdere la risposta (per timeout o disconnessione) e non sapere se l'effetto sia stato applicato. Ritentare alla cieca rischia di causare doppie scritture. 
A questo si aggiunge la complessità del sistema distribuito: il **Router** funge da gateway unico, calcola su quale shard si trova la chiave, instrada la richiesta e maschera al client le complessità della transizione durante un cambio di topologia (rebalance).

La soluzione si basa sul passaggio di un `request_id` per ogni operazione mutativa, elaborato e validato dal Router prima di interrogare fisicamente i nodi dati (ShardNode).

> **Nota:** la request table e le garanzie di idempotenza sono mantenute in memoria sul Router. Un riavvio del Router azzera la tabella. Gli ShardNode sottostanti ignorano il concetto di client e idempotenza, operando esclusivamente in base alle versioni.

---

## Trasporto

*   Protocollo testuale su TCP.
*   Encoding UTF-8.
*   Una richiesta per riga, terminata da `\n`.
*   Una risposta per riga, terminata da `\n`.
*   Ogni connessione è gestita da un thread dedicato sul Router.

---

## Modello dati e Tombstone

*   **Chiavi:** stringhe senza spazi.
*   **Valori:** stringhe UTF-8 senza newline, ma con spazi ammessi.
*   Il Router assegna un Sequence Number globale (versione) che parte da `0` al primo inserimento in assoluto e cresce ad ogni scrittura sul sistema, garantendo versioni strettamente crescenti.
*   Una chiave assente ha versione implicita `-1` (il sistema risponde con `ERR_NOT_FOUND`).
*   **Gestione DELETE (Tombstone):** a differenza dei sistemi a nodo singolo, `DELETE_REQ` **non** cancella fisicamente la chiave azzerandone la versione. Il Router esegue invece una "cancellazione logica", sovrascrivendo la chiave con un valore sentinella (`<TOMBSTONE>`) e una *nuova* versione aggiornata. Questo previene la "resurrezione" dei dati cancellati durante un fallback di lettura nel mezzo di un rebalancing.

---

## Formato del `request_id`

Il `request_id` identifica univocamente una singola operazione mutativa emessa da un client specifico.

Formato: `<client_id>:<seq>`

Dove:
*   `client_id` è una stringa senza spazi e senza due punti `:`, che identifica il client (es. `clientA`, `worker-3`).
*   `seq` è un numero intero non negativo, strettamente crescente per ogni nuova operazione logica.

---

## Contenuto della request table nel Router

Per ogni `(client_id, seq)` già visto, il Router memorizza:

1.  **Il payload canonico della richiesta:** il testo del comando senza il `request_id`, con il comando in maiuscolo. Il valore è preservato esattamente come ricevuto. Questo serve a rilevare conflitti (riuso di `seq` per payload differenti).
2.  **La risposta calcolata:** la stringa di risposta prodotta la prima volta che il Router ha contattato lo ShardNode, compresi gli esiti applicativi (`ERR_CAS_CONFLICT current=0`, `ERR_NOT_FOUND`).

**Eccezione per il Rebalancing:** le risposte di tipo `ERR_REBALANCING` (emesse quando si tenta una `CAS_REQ` durante un cambio di topologia) sono esiti transitori legati allo stato dell'infrastruttura. **Non vengono salvate** nella request table, garantendo che un retry successivo alla fine del rebalance possa essere elaborato correttamente (Liveness).

---

## Comportamento al retry e Sequenza di Esecuzione

Il parsing sintattico avviene fuori dai lock. Gli errori di sintassi (`ERR usage:...`, `ERR_INVALID_REQUEST_ID`) non vengono salvati.

Se il comando mutativo è valido, il Router esegue le seguenti operazioni:

1.  Acquisisce il `client_lock` specifico per il `client_id`.
2.  Controlla la request table per `(client_id, seq)`:
    *   Se presente con payload identico: restituisce la risposta salvata.
    *   Se presente con payload diverso: restituisce `ERR_REQUEST_ID_CONFLICT`.
3.  Verifica la finestra di eviction:
    *   Se `seq <= eviction_boundary[client_id]`: restituisce `ERR_REQUEST_ID_EXPIRED`.
4.  Altrimenti, procede all'**esecuzione (rete)**:
    *   Scatta una fotografia coerente della topologia di routing.
    *   Se è in corso un rebalance e l'operazione è una `CAS_REQ`, abortisce immediatamente con `ERR_REBALANCING`.
    *   Altrimenti, individua lo ShardNode responsabile, genera la nuova versione globale, e invia il comando di I/O (via socket TCP).
5.  Salva la tupla `(payload_canonico, risposta)` nella request table.
6.  Rilascia il `client_lock` e invia la risposta al client.

L'acquisizione del lock per client garantisce l'atomicità ed evita che richieste di rete duplicate e concorrenti vengano inoltrate simultaneamente agli ShardNode.

---

## Comandi mutativi con request_id

### `SET_REQ <client_id>:<seq> <key> <value...>`
Crea o sovrascrive il valore di `key` con `value`. Durante un rebalance, la scrittura viene instradata direttamente sulla topologia nuova (N+1).
*   Risposte possibili: `OK version=<n>`
*   Retry identico: `OK version=<n>`
*   Retry con payload diverso: `ERR_REQUEST_ID_CONFLICT`

### `CAS_REQ <client_id>:<seq> <key> <expected_version> <value...>`
Aggiorna `key` solo se la versione corrente coincide. L'operazione è **bloccata** durante un rebalance.
*   Risposte possibili: `OK version=<n>` | `ERR_CAS_CONFLICT current=<m>` | `ERR_NOT_FOUND`
*   In caso di rebalance in corso: `ERR_REBALANCING` (esito non cachato).
*   I fallimenti applicativi (`ERR_CAS_CONFLICT` / `ERR_NOT_FOUND`) vengono regolarmente salvati e riprodotti al retry.

### `DELETE_REQ <client_id>:<seq> <key>`
Rimuove la chiave inserendo logicamente un Tombstone con una nuova versione. Se la chiave non esiste, non applica nulla.
*   Risposte possibili: `OK` | `ERR_NOT_FOUND`

---

## Comandi di lettura e monitoraggio (senza request_id)

Le operazioni di lettura non necessitano del request_id. Durante un rebalance, il Router implementa un **fallback in lettura**: interroga prima la topologia nuova e, se la chiave non viene trovata, effettua una seconda chiamata alla topologia vecchia, unendo trasparentemente i dati per il client. I Tombstone letti vengono tradotti direttamente in un risultato di assenza.

| Comando | Risposta |
| :--- | :--- |
| `PING` | `OK PONG` |
| `GET <key>` | `<value> <version>` oppure `ERR_NOT_FOUND` |
| `GETV <key>` | `<version>` oppure `ERR_NOT_FOUND` |
| `KEYS` | `OK <key1> <key2> ...` oppure `OK` (ignora i tombstone) |
| `STATS` | `OK shards=<n> rebalancing=<0/1> new_shards=<n> clients=<n> cached_requests=<n> window_size=<n>` |
| `QUIT` | `OK BYE` (chiude la connessione) |

---

## Comandi di Amministrazione della Topologia

Questi comandi governano il ciclo di vita del cluster e avviano il rebalancing. Vengono impartiti al Router, che dialoga in background con il Coordinator e gli ShardNode.

| Comando | Descrizione / Risposta |
| :--- | :--- |
| `ADD_SHARD <id> <host:port>` | Aggiunge un nodo ai pendenti. Risponde `OK` o `ERR shard_already_present` / `ERR rebalance_in_progress`. |
| `REMOVE_SHARD <id> <host:port>` | Segna un nodo attivo per la rimozione. Risponde `OK` o `ERR shard_not_found`. |
| `REBALANCE` | Istanzia la nuova topologia calcolando la differenza tra nodi attivi, aggiunti e rimossi, avvisando il Coordinator di avviare la Two-Phase Commit di migrazione dei dati. Risponde `OK rebalance_scheduled` oppure `ERR nothing_to_rebalance` / `ERR coordinator_rejected`. |

---

## Tabella degli Errori di Protocollo (Client-Facing)

| Risposta | Causa |
| :--- | :--- |
| `ERR unknown_command` | Il comando non è riconosciuto dal Router |
| `ERR_INVALID_REQUEST_ID` | Il `request_id` non rispetta il formato prescrittivo `<id>:<seq>` |
| `ERR_REQUEST_ID_EXPIRED` | `seq <= eviction_boundary`; la memoria è stata evictata, replay non garantito |
| `ERR_REQUEST_ID_CONFLICT` | Stesso `request_id`, ma operazione diversa in ingresso |
| `ERR_NOT_FOUND` | La chiave richiesta non esiste (o è un tombstone) |
| `ERR_CAS_CONFLICT current=<n>` | `CAS_REQ` fallita perché la versione corrente differisce da `expected_version` |
| `ERR_REBALANCING` | `CAS_REQ` bloccata transitoriamente a causa di un rebalance in corso della topologia |
| `ERR shard_unreachable: <exc>` | Nodo dati offline o timeout di rete durante la comunicazione interna |
| `ERR usage: ...` | Argomenti mancanti o mal formati nel protocollo |
| `ERR bad_version` | La `expected_version` della `CAS_REQ` non è un numero intero valido |