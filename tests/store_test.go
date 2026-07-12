package store_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/AleZane29/GoQueue/internal/model"
	"github.com/AleZane29/GoQueue/internal/store"
)

// pool condiviso tra tutti i test — il container parte una sola volta
var testPool *pgxpool.Pool

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := tcpostgres.RunContainer(ctx,
		tcpostgres.WithDatabase("goqueue_test"),
		tcpostgres.WithUsername("postgres"),
		tcpostgres.WithPassword("postgres"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2),
		),
	)
	if err != nil {
		panic(err)
	}
	defer container.Terminate(ctx)

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		panic(err)
	}

	testPool, err = pgxpool.New(ctx, connStr)
	if err != nil {
		panic(err)
	}
	defer testPool.Close()

	// applica le migration
	if err := applyMigrations(ctx, testPool); err != nil {
		panic(err)
	}

	os.Exit(m.Run())
}

// applyMigrations esegue lo schema sul db di test
func applyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	schema := `
        CREATE TABLE IF NOT EXISTS queues (
            id     SERIAL PRIMARY KEY,
            name   TEXT NOT NULL UNIQUE,
            weight INT  NOT NULL DEFAULT 1
        );
        INSERT INTO queues (id, name, weight) VALUES
            (1, 'high', 5), (2, 'medium', 3), (3, 'low', 1)
        ON CONFLICT DO NOTHING;

        CREATE TABLE IF NOT EXISTS jobs (
            id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
            queue_id            INT         NOT NULL REFERENCES queues(id),
            name                TEXT        NOT NULL,
            status              TEXT        NOT NULL DEFAULT 'pending',
            type                TEXT        NOT NULL,
            payload             JSONB       NOT NULL DEFAULT '{}',
            max_time_to_execute INTERVAL    NOT NULL DEFAULT '5 minutes',
            max_attempts        INT         NOT NULL DEFAULT 3,
						attempts            INT         NOT NULL DEFAULT 0,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
            scheduled_at        TIMESTAMPTZ NOT NULL DEFAULT now()
        );

				CREATE TABLE IF NOT EXISTS executions (
					id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
					job_id       UUID        NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
					worker_id    TEXT        NOT NULL,
					attempt      INT         NOT NULL,
					started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
					completed_at TIMESTAMPTZ,
					status       TEXT        NOT NULL,  
					error        TEXT  
				);                
		`

	_, err := pool.Exec(ctx, schema)
	return err
}

// helper: crea uno store e pulisce la tabella jobs dopo ogni test
func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	t.Cleanup(func() {
		_, err := testPool.Exec(context.Background(), "DELETE FROM jobs")
		require.NoError(t, err)
	})
	return store.NewStore(testPool)
}

// helper: job con valori di default sensati
func baseJob(queueId int) *model.Job {
	return &model.Job{
		QueueId:          queueId,
		Name:             "test_job",
		Status:           model.StatusPending,
		Type:             "test",
		Payload:          `{"key": "value"}`,
		MaxTimeToExecute: 5 * time.Minute,
		MaxAttempts:      3,
		CreatedAt:        time.Now(),
		ScheduledAt:      time.Now(),
	}
}

// --- test cases ---

func TestFetchNextJob_EmptyQueue(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	job, err := s.FetchNextJob(ctx, 1)
	require.NoError(t, err)
	assert.Nil(t, job) // coda vuota → nil, non errore
}

func TestFetchNextJob_ReturnsPendingJob(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_, err := s.InsertJob(ctx, baseJob(1))
	require.NoError(t, err)

	job, err := s.FetchNextJob(ctx, 1)

	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, "test_job", job.Name)
	assert.Equal(t, model.StatusRunning, job.Status) // deve essere già in running
}

func TestFetchNextJob_IgnoresFutureJobs(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	future := baseJob(1)
	future.ScheduledAt = time.Now().Add(1 * time.Hour) // schedulato nel futuro
	_, err := s.InsertJob(ctx, future)
	require.NoError(t, err)

	job, err := s.FetchNextJob(ctx, 1)

	require.NoError(t, err)
	assert.Nil(t, job) // non deve essere prelevato
}

func TestFetchNextJob_FIFOOrder(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	first := baseJob(1)
	first.Name = "first_job"
	first.ScheduledAt = time.Now().Add(-2 * time.Minute) // più vecchio

	second := baseJob(1)
	second.Name = "second_job"
	second.ScheduledAt = time.Now().Add(-1 * time.Minute)

	_, err := s.InsertJob(ctx, first)
	require.NoError(t, err)
	_, err = s.InsertJob(ctx, second)
	require.NoError(t, err)

	job, err := s.FetchNextJob(ctx, 1)

	require.NoError(t, err)
	require.NotNil(t, job)
	assert.Equal(t, "first_job", job.Name) // deve prendere il più vecchio
}

func TestFetchNextJob_IgnoresRunningJobs(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_, err := s.InsertJob(ctx, baseJob(1))
	require.NoError(t, err)
	_, err = s.InsertJob(ctx, baseJob(1))
	require.NoError(t, err)

	// primo fetch: prende job_1 e lo mette in running
	first, err := s.FetchNextJob(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, first)

	// secondo fetch: deve prendere job_2, non job_1 che è running
	second, err := s.FetchNextJob(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, second)

	assert.NotEqual(t, first.Id, second.Id)
}

func TestFetchNextJob_IgnoresOtherQueues(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_, err := s.InsertJob(ctx, baseJob(2)) // queue medium
	require.NoError(t, err)

	job, err := s.FetchNextJob(ctx, 1) // chiedo dalla queue high

	require.NoError(t, err)
	assert.Nil(t, job) // non deve vedere il job della coda medium
}

// il test più importante: verifica che SKIP LOCKED funzioni
func TestFetchNextJob_NoConcurrentDuplicates(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_, err := s.InsertJob(ctx, baseJob(1))
	require.NoError(t, err)

	results := make(chan *model.Job, 2)

	for range 2 {
		go func() {
			job, _ := s.FetchNextJob(ctx, 1)
			results <- job
		}()
	}

	first := <-results
	second := <-results

	// esattamente uno dei due deve aver preso il job
	if first == nil {
		assert.NotNil(t, second)
	} else {
		assert.Nil(t, second)
	}
}

func TestFetchOrphanedJobs_RetriveOnlyOrphaned(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()
	var err error

	noOrphan := baseJob(1)
	noOrphan.Name = "no_orphan_job"
	noOrphan.MaxTimeToExecute = 5 * time.Minute
	noOrphan.Id, err = s.InsertJob(ctx, noOrphan)
	require.NoError(t, err)
	_, err = s.FetchNextJob(ctx, 1)
	require.NoError(t, err)
	s.InsertExecution(ctx, noOrphan.Id, "worker1", 1)

	orphan := baseJob(1)
	orphan.Name = "orphan_job"
	orphan.MaxTimeToExecute = -1 * time.Minute
	orphan.Id, err = s.InsertJob(ctx, orphan)
	require.NoError(t, err)

	_, err = s.FetchNextJob(ctx, 1)
	require.NoError(t, err)
	s.InsertExecution(ctx, orphan.Id, "worker1", 1)

	jobs, err := s.FetchOrphanedJobs(ctx)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	assert.Equal(t, "orphan_job", jobs[0].Name)
}

func TestTerminateExecution_InsertExecution(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	jobId, err := s.InsertJob(ctx, baseJob(1))
	require.NoError(t, err)

	err = s.TerminateExecution(ctx, "", model.StatusCompleted, "")
	require.Error(t, err)

	executionId, err := s.InsertExecution(ctx, jobId, "worker1", 1)
	require.NoError(t, err)

	err = s.TerminateExecution(ctx, executionId, model.StatusCompleted, "")
	require.NoError(t, err)
}
