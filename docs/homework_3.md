
## Homework 3: Retry Idempotenti Con `request_id`

### Obiettivo

Rendere sicuri i retry delle operazioni mutative.

In un sistema distribuito, un client puo' inviare una scrittura, perdere la
risposta e non sapere se il server l'abbia applicata. Se ritenta alla cieca,
rischia di applicare due volte lo stesso effetto.

### Interfaccia proposta

Esempi:

```text
SET_REQ clientA:42 key value
CAS_REQ clientA:43 key 7 value
DELETE_REQ clientA:44 key
```

Il `request_id` identifica univocamente una richiesta mutativa del client.

### Requisiti minimi

- il server deve ricordare l'esito delle richieste gia' viste;
- ripetere lo stesso `request_id` deve restituire la stessa risposta senza riapplicare l'effetto;
- il contratto deve dire quando un `request_id` puo' essere dimenticato;
- i test devono simulare almeno un retry dopo timeout del client.

### Safety

Proprieta' da discutere:

- la stessa richiesta mutativa non deve produrre effetti doppi;
- due richieste diverse non devono essere confuse solo perche' toccano la stessa chiave;
- il replay della risposta deve essere coerente con l'effetto gia' applicato.

### Liveness

Proprieta' da discutere:

- il server non puo' conservare per sempre tutti i `request_id`;
- la garbage collection dei request id non deve bloccare il servizio;
- un client corretto deve poter completare una sequenza di retry.

### Hint

Una soluzione base e':

```text
request_table[client_id][sequence_number] = response
```

Il punto difficile e' la pulizia.

Possibili strategie:

- conservare solo gli ultimi `N` request id per client;
- usare numeri di sequenza monotoni e un ack cumulativo;
- usare una scadenza temporale, dichiarando che oltre quella finestra il retry non e' piu' garantito.

