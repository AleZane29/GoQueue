# GoQueue — Design Document

> Versione 0.1 — work in progress, aggiornare a ogni decisione architetturale
> rilevante.

---

## 1. Obiettivo

GoQueue è un job queue distribuito scritto in Go che permette a un'applicazione
di eseguire lavoro in background in modo affidabile, con supporto nativo per
code a priorità, retry automatico e osservabilità tramite OpenTelemetry.

Il problema che risolve: ogni backend ha bisogno di eseguire operazioni
asincrone (invio email, elaborazione file, notifiche) senza bloccare la risposta
all'utente. Farlo in modo robusto — senza perdere job, senza eseguirli due
volte, con visibilità su cosa sta succedendo — è più complesso di quanto sembri.

---

## 2. Scope

### In scope

- Code a priorità multipla (high, medium, low) con weighted polling
- Worker pool configurabile (numero di worker, concorrenza per coda)
- Retry con backoff esponenziale e dead letter queue
- Persistenza su Postgres (`SELECT ... FOR UPDATE SKIP LOCKED`)
- API REST per sottomissione e ispezione dei job
- Instrumentazione OpenTelemetry (trace, metriche, log)
- Dashboard Grafana funzionante via Docker Compose

### Out of scope (decisione esplicita)

- Distribuzione multi-nodo: GoQueue gira su un singolo processo. La scalabilità
  orizzontale è lasciata fuori per contenere la complessità; il design non la
  preclude ma non la implementa.
- Scheduling temporale (cron): i job vengono eseguiti il prima possibile, non a
  un orario pianificato.
- Autenticazione multi-tenant: l'API usa una singola API key configurata via
  environment variable.

---

## 3. Architettura

```
┌─────────────────────────────────────────────────────────┐
│                        GoQueue                          │
│                                                         │
│  ┌──────────┐    ┌───────────┐    ┌──────────────────┐  │
│  │ HTTP API │───▶│ Dispatcher│───▶│   Worker Pool    │  │
│  └──────────┘    └─────┬─────┘    │  ┌────────────┐  │  │
│                        │          │  │  Worker 1  │  │  │
│                        ▼          │  │  Worker 2  │  │  │
│                  ┌───────────┐    │  │  Worker N  │  │  │
│                  │ Postgres  │◀───│  └────────────┘  │  │
│                  │  (queue)  │    └──────────────────┘  │
│                  └───────────┘                          │
│                        │                                │
│                        ▼                                │
│                  ┌───────────┐                          │
│                  │    OTel   │──▶ Prometheus / Tempo    │
│                  │ Collector │                          │
│                  └───────────┘                          │
└─────────────────────────────────────────────────────────┘
```

**HTTP API**: riceve richieste di sottomissione job e query sullo stato. Scrive
job su Postgres.

**Dispatcher**: loop interno che, per ogni worker libero, seleziona il prossimo
job da eseguire rispettando la politica di scheduling (vedi sezione 5). Usa
`SELECT ... FOR UPDATE SKIP LOCKED` per garantire che due dispatcher non
prendano lo stesso job.

**Worker Pool**: insieme di goroutine che eseguono job. Ogni worker riceve un
job dal dispatcher, lo esegue, e aggiorna lo stato su Postgres.

**Postgres**: unica fonte di verità. Contiene job, code, esecuzioni e
configurazione.

---

## 4. Schema dati

```sql
CREATE TABLE queues (
    id      SERIAL PRIMARY KEY,
    name    TEXT NOT NULL UNIQUE,          -- "high", "medium", "low"
    weight  INT  NOT NULL DEFAULT 1        -- usato dal weighted polling
);

CREATE TABLE jobs (
    id                 UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id           INT         NOT NULL REFERENCES queues(id),
    name               TEXT        NOT NULL,  -- es. "send_email"
    status             TEXT        NOT NULL DEFAULT 'pending',
                                              -- pending | running | completed | failed | dead
    type               TEXT        NOT NULL,  -- handler da invocare
    payload            JSONB       NOT NULL DEFAULT '{}',
    max_time_to_execute INTERVAL   NOT NULL DEFAULT '5 minutes',
    max_attempts       INT         NOT NULL DEFAULT 3,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    scheduled_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE executions (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id       UUID        NOT NULL REFERENCES jobs(id),
    worker_id    TEXT        NOT NULL,
    attempt      INT         NOT NULL,
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ,
    status       TEXT        NOT NULL,  -- running | completed | failed
    error        TEXT                   -- messaggio di errore se fallito
);

-- Index critico per le performance dello scheduler
CREATE INDEX idx_jobs_queue_status_scheduled
    ON jobs (queue_id, status, scheduled_at)
    WHERE status = 'pending';
```

### Note sul modello

`Execution` è separata da `Job` deliberatamente: ogni retry genera una nuova
riga in `executions`, mantenendo la storia completa dei tentativi. Questo
permette di calcolare latenza media per tentativo e di ispezionare l'errore di
ogni singolo fallimento.

`scheduled_at` è distinto da `created_at`: consente di implementare in futuro il
delay di un job (es. "esegui tra 10 minuti") senza cambiare lo schema.

`status = 'dead'` indica un job finito nella dead letter queue: ha esaurito i
tentativi, non verrà più rieseguito automaticamente.

---

## 5. Algoritmo di scheduling

### Weighted polling con priorità

Il dispatcher non estrae semplicemente il job più vecchio. Ad ogni ciclo esegue
questo algoritmo:

1. Per ogni worker libero, costruisce un "giro" di pull pesato dalle code:
   - High: 5 pull
   - Medium: 3 pull
   - Low: 1 pull

2. Per ogni slot del giro, tenta `SELECT ... FOR UPDATE SKIP LOCKED LIMIT 1`
   sulla coda corrispondente ordinando per `scheduled_at ASC` (FIFO).

3. Se una coda è vuota, il suo slot viene saltato e si passa alla successiva nel
   giro.

**Perché questo design invece di pari/dispari worker con algoritmi diversi**:
assegnare algoritmi diversi a worker diversi rende il comportamento dipendente
da quale worker è libero in quel momento — imprevedibile e difficile da
debuggare. Con il weighted polling, il comportamento è deterministico e
configurabile centralmente.

**Starvation prevention**: i job Low avanzano sempre grazie ai loro slot nel
giro. Con i pesi 5/3/1, in uno scenario di carico costante su tutte le code, un
job Low viene eseguito ogni ~9 job totali — sufficiente per evitare starvation
nella maggior parte dei casi d'uso.

### Gestione di maxTimeToExecute

`max_time_to_execute` è il timeout massimo per una singola esecuzione. Un job
worker che supera questo limite viene interrotto via context cancellation
(`context.WithTimeout`). Il job torna in `pending` e viene rischedulato per un
retry.

Un job runner separato (goroutine di "orfan detection") gira ogni 30 secondi e
cerca job in stato `running` la cui esecuzione attiva ha
`started_at < now() - max_time_to_execute`. Questi job vengono rimessi in
`pending` — coprono il caso in cui il processo crashi senza poter fare cleanup.

---

## 6. Retry e failure

```
Tentativo 1 → fallisce → attesa 2^1 * base_delay → Tentativo 2
Tentativo 2 → fallisce → attesa 2^2 * base_delay → Tentativo 3
Tentativo 3 → fallisce → status = "dead" → Dead Letter Queue
```

Il `scheduled_at` del job viene aggiornato a `now() + backoff` dopo ogni
fallimento, così il job non viene ripreso immediatamente.

`base_delay` è configurabile per coda (default: 30 secondi). Il backoff è
esponenziale con jitter ±10% per evitare thundering herd se molti job falliscono
contemporaneamente.

### Dead Letter Queue

I job `dead` non vengono eliminati — restano in Postgres con `status = 'dead'` e
l'errore dell'ultimo tentativo in `executions`. L'API espone un endpoint
`POST /jobs/{id}/retry` per riportare manualmente un job `dead` in `pending` con
il contatore dei tentativi azzerato.

---

## 7. API REST

| Method   | Path               | Descrizione                                               |
| -------- | ------------------ | --------------------------------------------------------- |
| `POST`   | `/jobs`            | Sottomette un nuovo job                                   |
| `GET`    | `/jobs/{id}`       | Stato e dettagli di un job                                |
| `GET`    | `/jobs`            | Lista job con filtri (queue, status, page)                |
| `POST`   | `/jobs/{id}/retry` | Rimette in pending un job dead                            |
| `DELETE` | `/jobs/{id}`       | Cancella un job pending                                   |
| `GET`    | `/queues`          | Lista code con statistiche (pending, running, dead count) |
| `GET`    | `/health`          | Liveness check                                            |
| `GET`    | `/metrics`         | Endpoint Prometheus                                       |

Autenticazione: `Authorization: Bearer <API_KEY>` su tutte le route tranne
`/health` e `/metrics`.

### Esempio payload sottomissione

```json
POST /jobs
{
  "queue": "high",
  "name": "send_welcome_email",
  "type": "email",
  "payload": {
    "to": "user@example.com",
    "template": "welcome"
  },
  "max_time_to_execute": "30s",
  "max_attempts": 5
}
```

---

## 8. Osservabilità

Tutti i componenti sono instrumentati con OpenTelemetry. Il Docker Compose di
sviluppo include Grafana, Prometheus e Tempo pronti all'uso.

### Trace

Ogni job genera uno span dal momento della sottomissione al completamento, con i
seguenti attributi custom:

```
goqueue.job.id
goqueue.job.name
goqueue.job.type
goqueue.job.queue
goqueue.job.attempt
goqueue.worker.id
```

Gli span di retry sono figli dello span padre del job, così la trace mostra
l'intera storia di un job in un'unica visualizzazione su Tempo.

### Metriche (Prometheus)

```
goqueue_jobs_submitted_total{queue, type}
goqueue_jobs_completed_total{queue, type}
goqueue_jobs_failed_total{queue, type, reason}
goqueue_jobs_dead_total{queue, type}
goqueue_job_duration_seconds{queue, type}     -- histogram
goqueue_queue_depth{queue, status}            -- gauge
goqueue_worker_active{worker_id}              -- gauge
```

### Dashboard Grafana

La repository include un `dashboard.json` preconfigurato con: throughput per
coda nel tempo, latenza di esecuzione (p50/p95/p99), tasso di fallimento,
profondità delle code, job in dead letter queue.

---

## 9. Trade-off documentati

| Decisione                     | Alternativa scartata                      | Motivazione                                                                                                                                                                                                                                       |
| ----------------------------- | ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Postgres come unica storage   | Redis per la coda                         | `SKIP LOCKED` è sufficiente per questo carico; evita una dipendenza infrastrutturale aggiuntiva. Redis può essere aggiunto in futuro come cache dello stato per la dashboard.                                                                     |
| At-least-once delivery        | Exactly-once                              | Exactly-once richiede distributed transactions o idempotency tokens gestiti lato applicazione. At-least-once è più semplice e sufficiente se i job sono progettati per essere idempotenti.                                                        |
| Weighted polling              | Worker pari/dispari con algoritmi diversi | Comportamento deterministico e configurabile centralmente; più semplice da debuggare e da spiegare.                                                                                                                                               |
| Single-process                | Multi-nodo                                | Fuori scope per v1; il design non lo preclude (il lock su Postgres funziona già con più istanze dello stesso processo).                                                                                                                           |
| FIFO all'interno di ogni coda | SJF (shortest job first)                  | SJF richiede una stima del tempo di esecuzione che spesso non è disponibile a priori. `maxTimeToExecute` come upper bound non è sufficiente per un ordinamento significativo. SJF è documentato come possibile estensione configurabile per coda. |

---

## 10. Struttura del repository

```
goqueue/
├── cmd/
│   └── server/         # entrypoint principale
├── internal/
│   ├── api/            # handler HTTP, middleware, routing
│   ├── dispatcher/     # loop di scheduling e weighted polling
│   ├── worker/         # pool di worker, esecuzione job
│   ├── store/          # query Postgres, transazioni
│   ├── telemetry/      # setup OpenTelemetry
│   └── model/          # struct condivise (Job, Queue, Execution)
├── migrations/         # file SQL ordinati numericamente
├── deploy/
│   ├── docker-compose.yml
│   └── grafana/        # dashboard.json e datasource config
├── docs/
│   └── DESIGN.md       # questo file
├── tests/
│   └── integration/    # test end-to-end con Postgres reale
└── README.md
```

---

## 11. Decisioni ancora aperte

- [ ] Formato di serializzazione del payload: JSONB su Postgres è la scelta
      naturale, ma valutare se aggiungere validazione dello schema a livello
      applicativo.
- [ ] Configurazione dei pesi del weighted polling: hardcoded per v1,
      configurabile per coda in v2?
- [ ] Interfaccia per registrare handler: come un'applicazione esterna registra
      il codice da eseguire per `type = "email"`? Valutare plugin via subprocess
      o HTTP callback verso l'applicazione chiamante.
- [ ] Strategia di test per il dispatcher: mock del clock per testare orfan
      detection e backoff senza aspettare tempi reali.
