package worker

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/AleZane29/GoQueue/internal/model"
	"github.com/AleZane29/GoQueue/internal/store"
)

type HandlerFunc func(ctx context.Context, job *model.Job) error

type Worker struct {
	id       string
	store    *store.Store
	handlers map[string]HandlerFunc
	jobCh    <-chan *model.Job
}

// type WorkerPool struct {
// 	workers []*Worker
// 	store   *store.Store
// }

func (w *Worker) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case job := <-w.jobCh:
				w.processNextJob(ctx, job)
			}
		}
	}()
}

func (w *Worker) processNextJob(ctx context.Context, job *model.Job) {
	execId, err := w.store.InsertExecution(ctx, job.Id, w.id, job.Attempts)
	if err != nil {
		return
	}

	// esegui il job con timeout
	jobCtx, cancel := context.WithTimeout(ctx, job.MaxTimeToExecute)
	defer cancel()

	execErr := w.handler(jobCtx, job)

	// gestisci l'esito
	if execErr == nil {
		w.store.TerminateExecution(ctx, execId, model.StatusCompleted, "")
		w.store.UpdateJobStatus(ctx, job.Id, model.StatusCompleted)
	} else {
		if errors.Is(execErr, context.DeadlineExceeded) {
			fmt.Printf("Job %s timed out after %s\n", job.Name, job.MaxTimeToExecute)
			execErr = fmt.Errorf("Job exceeded max time to execute\n")
		}
		w.store.TerminateExecution(ctx, execId, model.StatusFailed, execErr.Error())
		w.handleFailure(ctx, job, execErr)
	}
}

func (w *Worker) handler(ctx context.Context, job *model.Job) error {
	handlerJob, exists := w.handlers[job.Type]
	if !exists {
		fmt.Printf("Handler for job of type %s doesn't exist\n", job.Type)
		return fmt.Errorf("Handler for job of type %s doesn't exist\n", job.Type)
	}
	fmt.Printf("Worker %s is processing job %s\n", w.id, job.Name)

	err := handlerJob(ctx, job)

	return err
}

// internal/worker/worker.go
func (w *Worker) RegisterHandler(jobType string, handler HandlerFunc) {
	w.handlers[jobType] = handler
}

func (w *Worker) handleFailure(ctx context.Context, job *model.Job, execErr error) {
	if job.Attempts >= job.MaxAttempts {
		// ha esaurito i tentativi → dead
		w.store.UpdateJobStatus(ctx, job.Id, model.StatusDead)
		return
	}

	// calcola il backoff: 2^attempt * baseDelay con jitter ±10%
	attempt := float64(job.Attempts)
	baseDelay := 20 * time.Second
	backoff := time.Duration(math.Pow(2, attempt)) * baseDelay
	jitter := time.Duration(rand.Int63n(int64(backoff / 10)))
	nextScheduledAt := time.Now().Add(backoff + jitter)

	// rimetti in pending con il nuovo scheduled_at
	w.store.RescheduleJob(ctx, job.Id, nextScheduledAt)
}

// func NewWorkerPool(size int, store *store.Store) *WorkerPool {
// 	workers := make([]*Worker, size)
// 	for i := range size {
// 		workers[i] = NewWorker(
// 			fmt.Sprintf("worker-%d", i),
// 			store,
// 		)
// 	}
// 	return &WorkerPool{workers: workers, store: store}
// }

func NewWorker(id string, store *store.Store, jobCh <-chan *model.Job) *Worker {
	return &Worker{
		id:       id,
		store:    store,
		handlers: make(map[string]HandlerFunc),
		jobCh:    jobCh,
	}
}
