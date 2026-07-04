# Contratto del Protocollo: KV Store con Retry Idempotenti

Questo documento definisce l'interfaccia pubblica e la semantica osservabile del progetto **KV Store con retry idempotenti tramite `request_id`**.

Il problema affrontato è il seguente: un client può inviare una richiesta mutativa, perdere la risposta e non sapere se il server l'abbia già applicata. Un retry cieco può quindi produrre due volte lo stesso effetto.

La soluzione è associare ogni operazione mutativa a un identificatore univoco:

```text
<client_id>:<seq>
```

Il server mantiene una request table in memoria e, in caso di retry, restituisce la risposta già calcolata senza riapplicare l'effetto.

---

## 1. Trasporto

- Protocollo testuale su TCP.
- Encoding UTF-8.
- Una richiesta per riga, terminata da `\n`.
- Una risposta per riga, terminata da `\n`.
- Ogni connessione client è gestita da un thread dedicato.
- Host e porta di default:

```text
127.0.0.1:6379
```

---

## 2. Modello dati

- Chiavi: stringhe senza spazi.
- Valori: stringhe UTF-8 senza newline; gli spazi sono ammessi.
- Ogni chiave ha una versione intera locale.
- Una chiave assente ha versione implicita `-1`.
- Il primo inserimento produce `version=0`.
- Ogni scrittura effettiva successiva incrementa la versione di `1`.
- `DELETE_REQ` rimuove la chiave; un successivo `SET_REQ` riparte da `version=0`.

---

## 3. Formato del request_id

Formato:

```text
<client_id>:<seq>
```

Dove:

- `client_id` è una stringa non vuota, senza spazi e senza `:`;
- `seq` è un intero non negativo;
- per ogni nuova operazione logica dello stesso client, `seq` dovrebbe crescere monotonicamente;
- lo stesso `seq` può ricomparire solo come retry identico della stessa richiesta.

Esempi validi:

```text
clientA:0
clientA:42
worker-3:7
```

Esempi non validi:

```text
clientA
clientA:-1
client A:0
client:A:0
```

---

## 4. Request table

Il server mantiene:

```text
request_table[client_id][seq] = (payload_canonico, response)
```

Per ogni richiesta mutativa valida già vista, il server salva:

- il payload canonico della richiesta;
- la risposta prodotta alla prima esecuzione.

Il payload canonico serve a distinguere un retry legittimo dal riuso errato dello stesso `request_id`.

---

## 5. Semantica del retry

Quando arriva una richiesta mutativa valida:

```text
1. Se (client_id, seq) è già nella request table:
      - stesso payload  -> restituisce la risposta cached;
      - payload diverso -> ERR request_id_conflict.

2. Se seq <= eviction_boundary[client_id]:
      - restituisce ERR request_id_expired.

3. Altrimenti:
      - applica l'operazione allo store;
      - costruisce la risposta;
      - salva payload e risposta nella request table;
      - esegue eventuale eviction;
      - restituisce la risposta.
```

Il controllo della request table, l'applicazione dell'effetto, il salvataggio della risposta e l'eventuale eviction avvengono dentro la stessa sezione critica protetta da lock.

Gli errori di parsing non vengono salvati nella request table. Gli errori applicativi prodotti da richieste ben formate, come `ERR version_mismatch current=<n>` o `NOT_FOUND` su `DELETE_REQ`, vengono invece salvati e riprodotti al retry.

---

## 6. Comandi di sola lettura

Le operazioni di sola lettura non usano `request_id` e osservano lo stato corrente dello store.

| Comando | Risposta |
|---|---|
| `PING` | `PONG` |
| `GET <key>` | `OK <value>` oppure `NOT_FOUND` |
| `GETV <key>` | `OK version=<n> <value>` oppure `NOT_FOUND` |
| `EXISTS <key>` | `OK true` oppure `OK false` |
| `KEYS` | `OK <key1> <key2> ...` oppure `OK` se vuoto |
| `STATS` | `OK keys=<n> clients=<n> cached_requests=<n> window_size=<n>` |
| `QUIT` | `BYE` |

---

## 7. Comandi mutativi idempotenti

Questi comandi transitano per la request table.

### SET_REQ

```text
SET_REQ <client_id>:<seq> <key> <value...>
```

Effetto: crea o sovrascrive il valore di `key`.

Risposte:

```text
OK version=<n>
ERR request_id_conflict
ERR request_id_expired
```

Esempio:

```text
SET_REQ clientA:0 corso ads
-> OK version=0
```

Un retry identico restituisce la stessa risposta senza riapplicare l'effetto.

---

### CAS_REQ

```text
CAS_REQ <client_id>:<seq> <key> <expected_version> <value...>
```

Effetto: aggiorna `key` solo se la versione corrente coincide con `expected_version`.

Risposte:

```text
OK version=<n>
ERR version_mismatch current=<m>
ERR not_found
ERR request_id_conflict
ERR request_id_expired
```

Esempio:

```text
CAS_REQ clientA:1 corso 0 sistemi-distribuiti
-> OK version=1
```

Se una `CAS_REQ` fallisce per `version_mismatch`, il retry identico restituisce lo stesso errore salvato.

---

### DELETE_REQ

```text
DELETE_REQ <client_id>:<seq> <key>
```

Effetto: rimuove `key` dallo store.

Risposte:

```text
OK deleted=true
NOT_FOUND
ERR request_id_conflict
ERR request_id_expired
```

Esempio:

```text
DELETE_REQ clientA:2 corso
-> OK deleted=true
```

Anche `NOT_FOUND`, se prodotto da `DELETE_REQ` ben formata, viene salvato e riprodotto al retry.

---

## 8. Comandi mutativi non idempotenti

I seguenti comandi sono presenti solo per compatibilità e test manuale. Non transitano per la request table e non sono sicuri rispetto al retry cieco.

| Comando | Risposta |
|---|---|
| `SET <key> <value...>` | `OK version=<n>` |
| `CAS <key> <expected_version> <value...>` | `OK version=<n>`, `ERR version_mismatch current=<m>` oppure `ERR not_found` |
| `DELETE <key>` | `OK deleted=true` oppure `NOT_FOUND` |

I client corretti devono usare `SET_REQ`, `CAS_REQ` e `DELETE_REQ`.

---

## 9. Garbage collection della request table

Il server non conserva tutti i `request_id` per sempre.

Per ogni `client_id`, conserva al massimo `N` richieste recenti.

Default:

```text
N = 100
```

Quando la finestra supera `N`, viene rimossa la voce con `seq` minimo.

Per distinguere una richiesta mai vista da un retry ormai evictato, il server mantiene:

```text
eviction_boundary[client_id]
```

Regola:

```text
se seq <= eviction_boundary[client_id]:
    ERR request_id_expired
```

Nella versione corrente, l'eviction cerca il `seq` minimo nella finestra del client. Il costo è quindi `O(N)`, con `N` bounded e configurabile.

---

## 10. Errori

| Risposta | Significato | Salvata nella request table |
|---|---|---|
| `ERR unknown_command` | Comando non riconosciuto | No |
| `ERR malformed` | Numero di argomenti errato | No |
| `ERR bad_request_id` | `request_id` non valido | No |
| `ERR bad_version` | Versione attesa non numerica | No |
| `ERR request_id_conflict` | Stesso `request_id`, payload diverso | No |
| `ERR request_id_expired` | Retry fuori finestra | No |
| `ERR version_mismatch current=<n>` | `CAS_REQ` ben formata, ma versione non corrispondente | Sì |
| `ERR not_found` | `CAS`/`CAS_REQ` su chiave assente | Sì solo per `CAS_REQ` |
| `NOT_FOUND` | Chiave assente | Sì solo per `DELETE_REQ` |

---

## 11. Garanzie del contratto

Il protocollo garantisce:

- una richiesta mutativa con stesso `(client_id, seq)` e stesso payload produce il proprio effetto al massimo una volta;
- il retry riceve la stessa risposta della prima esecuzione;
- richieste con stesso `request_id` ma payload diverso vengono rifiutate;
- un retry fuori finestra non viene rieseguito, ma riceve `ERR request_id_expired`;
- la memoria della request table è limitata dalla finestra `N`.

---

## 12. Cosa non è garantito

Il protocollo non garantisce:

- persistenza dopo riavvio;
- sopravvivenza della request table a crash del server;
- replica della request table;
- idempotenza dopo failover;
- exactly-once distribuita;
- autenticazione del `client_id`;
- ordinamento globale tra client diversi;
- replay oltre la finestra `N`.

---

## 13. Sintesi

Il sistema distingue quattro casi:

```text
stesso request_id, stesso payload     -> replay della risposta cached
stesso request_id, payload diverso    -> ERR request_id_conflict
request_id già evictato               -> ERR request_id_expired
request_id mai visto                  -> prima esecuzione normale
```

Questa è la promessa centrale del contratto pubblico.
