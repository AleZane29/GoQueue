- Scrivere test per verificare che il numero di attempt di ogni job venga
  rispettato e aggiornato correttamente -> _aggiungere prima la gestione di
  aggiornamento degli attempt_

-Aggiornare design spiegando l'uso del strategy pattern per l'esecuzione in base
al tipo dei vari job

- verificare il fallimento per max_time_to_execute

# Path

## 1. Setup del progetto (1-2 ore)

Prima di tutto: go mod init, struttura delle cartelle esattamente come nel
DESIGN.md, un docker-compose.yml minimale con solo Postgres, e un Makefile con i
comandi che userai ogni giorno (make run, make migrate, make test). Niente
codice applicativo ancora — solo l'impalcatura.

## 2. Migration SQL (2-3 ore)

Scrivi i file SQL in migrations/ e uno script che li applica in ordine. Lo
schema è già definito nel DESIGN.md — traducilo in file numerati
(001_create_queues.sql, 002_create_jobs.sql, 003_create_executions.sql).
Verifica che girino correttamente sul Postgres locale. Questo è il tuo
"fondamenta" — tutto il resto si costruisce sopra.

## 3. internal/model (1-2 ore)

Le struct Go che rispecchiano le tabelle: Job, Queue, Execution, più le costanti
per gli stati (StatusPending, StatusRunning, ecc.) e i tipi custom dove servono.
Nessuna logica, solo definizioni. È il vocabolario condiviso da tutto il
progetto.

## 4. internal/store (2-3 giorni)

Le query Postgres: InsertJob, FetchNextJob (qui vive il SELECT ... FOR UPDATE
SKIP LOCKED), UpdateJobStatus, InsertExecution, UpdateExecution,
FetchOrphanedJobs. Scrivi un test di integrazione per ognuna usando un Postgres
reale (non mock) — è la parte più critica dell'intero sistema e deve essere
blindata prima di procedere. Se FetchNextJob ha un bug, tutto il resto crolla.

## 5. internal/worker (2-3 giorni)

Una singola goroutine che: prende un job dallo store, lo esegue con un
context.WithTimeout, aggiorna lo stato. Per ora l'esecuzione può essere fittizia
(time.Sleep + log) — l'importante è che il ciclo di vita del job funzioni
correttamente end-to-end. Poi il pool: N goroutine che girano in parallelo.

## 6. internal/dispatcher (2 giorni)

Il weighted polling: il loop che decide quale coda interrogare e passa i job ai
worker. A questo punto hai il sistema core funzionante — producer manuale (via
codice di test) → Postgres → dispatcher → worker → aggiornamento stato. È il
primo momento in cui puoi vedere il sistema girare davvero.

## 7. internal/api (2-3 giorni)

Gli endpoint REST nell'ordine di utilità: prima POST /jobs e GET /jobs/{id} (ti
permettono di smettere di inserire job a mano nel codice di test), poi GET
/queues, poi il resto. Aggiungi l'autenticazione per ultima, dopo che gli
endpoint funzionano.

## 8. internal/telemetry (2-3 giorni)

OpenTelemetry va aggiunto dopo che la logica core funziona, non mentre la stai
costruendo — altrimenti il rumore degli span distrae dal debugging. Aggiungi
prima i trace sullo store e sul worker, poi le metriche, poi configura Docker
Compose con Prometheus, Tempo e Grafana.
