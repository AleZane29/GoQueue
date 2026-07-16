package dispatcher

import (
	"context"
	"fmt"
	"time"

	"github.com/AleZane29/GoQueue/internal/model"
	"github.com/AleZane29/GoQueue/internal/store"
	"github.com/AleZane29/GoQueue/internal/worker"
)

type Dispatcher struct {
	jobCh   chan *model.Job
	store   *store.Store
	workers []*worker.Worker
	started bool
}

func (d *Dispatcher) RegisterHandler(jobType string, handler worker.HandlerFunc) {
	if d.started {
		panic("RegisterHandler must be called before Start")
	}
	for _, w := range d.workers {
		w.RegisterHandler(jobType, handler)
	}
}

func NewDispatcher(store *store.Store, workerCount int) *Dispatcher {
	jobCh := make(chan *model.Job, workerCount) // buffer = numero di worker

	workers := make([]*worker.Worker, workerCount)
	for i := 0; i < workerCount; i++ {
		workers[i] = worker.NewWorker(
			fmt.Sprintf("worker-%d", i),
			store,
			jobCh,
		)
	}

	return &Dispatcher{
		store:   store,
		jobCh:   jobCh,
		workers: workers,
		started: false,
	}
}

func (d *Dispatcher) dispatch(ctx context.Context) {
	// ciclo weighted polling: 5 pull da high, 3 da medium, 1 da low
	weights := []struct {
		queueId int
		pulls   int
	}{
		{1, 5}, // high
		{2, 3}, // medium
		{3, 1}, // low
	}

	for _, w := range weights {
		for range w.pulls {
			job, err := d.store.FetchNextJob(ctx, w.queueId)
			if err != nil || job == nil {
				continue
			}
			// passa il job a un worker libero
			d.jobCh <- job
		}
	}
}

func (d *Dispatcher) Start(ctx context.Context) {
	d.started = true
	fmt.Println("Starting", len(d.workers), "workers")
	// avvia tutti i worker
	for _, w := range d.workers {
		w.Start(ctx)
	}

	// avvia il loop di polling in una goroutine separata
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				d.dispatch(ctx)
				// piccola pausa per non martellare il db quando le code sono vuote
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
}
