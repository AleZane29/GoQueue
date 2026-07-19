package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/AleZane29/GoQueue/internal/dispatcher"
	"github.com/AleZane29/GoQueue/internal/model"
	"github.com/AleZane29/GoQueue/internal/store"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/joho/godotenv/autoload"
)

// func main() {
// 	ctx := context.Background()

// 	pool, err := pgxpool.New(ctx, "postgres://postgres:@localhost:5432/GoQueue?sslmode=disable")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer pool.Close()

// 	if err := pool.Ping(ctx); err != nil {
// 		log.Fatal(err)
// 	}

// 	store := store.NewStore(pool)
// 	_ = store // per ora, finché non sviluppo dispatcher
// }

func baseJob(queueId int, name string, jobType string) *model.Job {
	return &model.Job{
		QueueId:          queueId,
		Name:             name,
		Status:           model.StatusPending,
		Type:             jobType,
		Payload:          `{"key": "value"}`,
		MaxTimeToExecute: 5 * time.Minute,
		MaxAttempts:      3,
		CreatedAt:        time.Now(),
		ScheduledAt:      time.Now(),
	}
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 1. connessione al db
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)

	}
	defer pool.Close()

	// 2. store
	s := store.NewStore(pool)

	// 3. dispatcher con 10 worker
	dispatcher := dispatcher.NewDispatcher(s, 10)

	// 4. registri gli handler
	dispatcher.RegisterHandler("email", func(ctx context.Context, job *model.Job) error {
		// logica invio email
		fmt.Println("Sending email for job:", job.Id)
		return nil
	})
	dispatcher.RegisterHandler("resize_image", func(ctx context.Context, job *model.Job) error {
		// logica resize

		fmt.Println("Resizing image for job:", job.Id)
		return nil
	})

	s.InsertJob(ctx, baseJob(1, "test_job1", "email"))
	s.InsertJob(ctx, baseJob(1, "test_job2", "document"))

	s.InsertJob(ctx, baseJob(1, "test_job3", "resize_image"))
	s.InsertJob(ctx, baseJob(1, "test_job4", "document"))
	s.InsertJob(ctx, baseJob(2, "test_job5", "email"))
	s.InsertJob(ctx, baseJob(3, "test_job6", "email"))
	s.InsertJob(ctx, baseJob(3, "test_job7", "document"))

	s.InsertJob(ctx, baseJob(1, "test_job8", "API"))
	s.InsertJob(ctx, baseJob(1, "test_job9", "document"))
	s.InsertJob(ctx, baseJob(2, "test_job10", "notification"))
	s.InsertJob(ctx, baseJob(3, "test_job11", "email"))
	s.InsertJob(ctx, baseJob(3, "test_job12", "resize_image"))
	// 5. avvii il dispatcher
	dispatcher.Start(ctx)
	log.Println("GoQueue started, press CTRL+C to stop")

	// blocca qui finché non arriva CTRL+C o SIGTERM
	<-ctx.Done()

	log.Println("shutting down...")
}
