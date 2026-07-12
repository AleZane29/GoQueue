package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/AleZane29/GoQueue/internal/model"
)

type Store struct {
	db *pgxpool.Pool
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// To use when you want to fetch the next job from a specific queue and mark it as running. It uses a transaction to ensure that the job is locked for processing and updates its status atomically and attempts. If no pending jobs are available, it returns nil without an error.
func (s *Store) FetchNextJob(ctx context.Context, queueId int) (*model.Job, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("FetchNextJob begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	job := &model.Job{}
	query := `
        SELECT id, queue_id, name, status, type, payload,
               max_time_to_execute, attempts, max_attempts, created_at, scheduled_at
        FROM jobs
        WHERE status = 'pending'
          AND queue_id = $1
          AND scheduled_at <= NOW()
        ORDER BY scheduled_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED`

	err = tx.QueryRow(ctx, query, queueId).Scan(
		&job.Id,
		&job.QueueId,
		&job.Name,
		&job.Status,
		&job.Type,
		&job.Payload,
		&job.MaxTimeToExecute,
		&job.Attempts,
		&job.MaxAttempts,
		&job.CreatedAt,
		&job.ScheduledAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("FetchNextJob select: %w", err)
	}

	// UPDATE status to 'running'
	_, err = tx.Exec(ctx, `UPDATE jobs SET status = $1, attempts = $2 WHERE id = $3`, model.StatusRunning, job.Attempts+1, job.Id)
	if err != nil {
		return nil, fmt.Errorf("FetchNextJob update status: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("FetchNextJob commit: %w", err)
	}

	job.Status = model.StatusRunning
	job.Attempts = job.Attempts + 1
	return job, nil
}

func (s *Store) UpdateJobStatus(ctx context.Context, jobId string, status model.StatusValues) (string, error) {
	query := `
				UPDATE jobs SET status = $1 WHERE id = $2
        RETURNING id`

	err := s.db.QueryRow(ctx, query, status, jobId).Scan(&jobId)
	if err != nil {
		return "", fmt.Errorf("UpdateJobStatus: %w", err)
	}
	return jobId, nil
}

// Creates a new job in the database and returns its ID. It takes a Job struct as input, the value created_at is set to the current timestamp from the DB, which contains all the necessary fields for the job. The function uses a SQL INSERT statement with a RETURNING clause to get the generated job ID after insertion. If the insertion fails, it returns an error.
func (s *Store) InsertJob(ctx context.Context, job *model.Job) (string, error) {
	var jobId string
	query := `
        INSERT INTO jobs (queue_id, name, status, type, payload, max_time_to_execute, max_attempts, scheduled_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id`

	err := s.db.QueryRow(ctx, query,
		job.QueueId,
		job.Name,
		job.Status,
		job.Type,
		job.Payload,
		job.MaxTimeToExecute,
		job.MaxAttempts,
		job.ScheduledAt,
	).Scan(&jobId)
	if err != nil {
		return "", fmt.Errorf("InsertJob: %w", err)
	}
	return jobId, nil
}

func (s *Store) RescheduleJob(ctx context.Context, jobId string, newScheduledAt time.Time) error {
	query := `
				UPDATE jobs SET scheduled_at = $1, status = 'pending' WHERE id = $2`
	_, err := s.db.Exec(ctx, query, newScheduledAt, jobId)
	if err != nil {
		return fmt.Errorf("RescheduleJob: %w", err)
	}
	return nil
}

// Inserts a new execution record for a job. It takes the job ID, worker ID, and attempt number as input, and sets the status to 'running' with the current timestamp. The function returns the generated execution ID after insertion. If the insertion fails, it returns an error.
func (s *Store) InsertExecution(ctx context.Context, jobId string, workerId string, attempt int) (string, error) {
	var executionId string
	query := `
        INSERT INTO executions (job_id, worker_id, attempt, status, started_at)
        VALUES ($1, $2, $3, 'running', NOW())
        RETURNING id`

	err := s.db.QueryRow(ctx, query, jobId, workerId, attempt).Scan(&executionId)
	if err != nil {
		return "", fmt.Errorf("InsertExecution: %w", err)
	}
	return executionId, nil
}

func (s *Store) TerminateExecution(ctx context.Context, executionId string, status model.StatusValues, execError string) error {
	// execError = "" → errValue = nil
	var errValue *string
	if execError != "" {
		errValue = &execError
	}

	query := `
        UPDATE executions 
        SET status = $1, completed_at = NOW(), error = $2
        WHERE id = $3`

	_, err := s.db.Exec(ctx, query, status, errValue, executionId)
	if err != nil {
		return fmt.Errorf("UpdateExecution: %w", err)
	}
	return nil
}

func (s *Store) FetchOrphanedJobs(ctx context.Context) ([]*model.Job, error) {
	query := `
        SELECT j.id, j.queue_id, j.name, j.status, j.type, j.payload,
               j.max_time_to_execute, j.max_attempts, j.created_at, j.scheduled_at
        FROM jobs j
        JOIN executions e ON e.job_id = j.id
        WHERE j.status = 'running'
          AND e.status = 'running'
          AND e.started_at < NOW() - j.max_time_to_execute`

	rows, err := s.db.Query(ctx, query)
	if err != nil {
		return []*model.Job{}, fmt.Errorf("FetchOrphanedJobs: %w", err)
	}
	defer rows.Close()

	var jobs []*model.Job
	for rows.Next() {
		job := &model.Job{}
		err := rows.Scan(
			&job.Id,
			&job.QueueId,
			&job.Name,
			&job.Status,
			&job.Type,
			&job.Payload,
			&job.MaxTimeToExecute,
			&job.MaxAttempts,
			&job.CreatedAt,
			&job.ScheduledAt,
		)
		if err != nil {
			return jobs, fmt.Errorf("FetchOrphanedJobs scan: %w", err)
		}
		jobs = append(jobs, job)
	}

	if err = rows.Err(); err != nil {
		return jobs, fmt.Errorf("FetchOrphanedJobs rows error: %w", err)
	}

	return jobs, nil
}
