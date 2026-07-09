package store

import (
	"context"
	"errors"
	"fmt"

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

func (s *Store) FetchNextJob(ctx context.Context, queueId int) (*model.Job, error) {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("FetchNextJob begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	job := &model.Job{}
	query := `
        SELECT id, queue_id, name, status, type, payload,
               max_time_to_execute, max_attempts, created_at, scheduled_at
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
	_, err = tx.Exec(ctx, `UPDATE jobs SET status = $1 WHERE id = $2`, model.StatusRunning, job.Id)
	if err != nil {
		return nil, fmt.Errorf("FetchNextJob update status: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("FetchNextJob commit: %w", err)
	}

	job.Status = model.StatusRunning
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

func (s *Store) InsertJob(ctx context.Context, job *model.Job) (string, error) {
	var jobId string
	query := `
        INSERT INTO jobs (queue_id, name, status, type, payload, max_time_to_execute, max_attempts, created_at, scheduled_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        RETURNING id`

	err := s.db.QueryRow(ctx, query,
		job.QueueId,
		job.Name,
		job.Status,
		job.Type,
		job.Payload,
		job.MaxTimeToExecute,
		job.MaxAttempts,
		job.CreatedAt,
		job.ScheduledAt,
	).Scan(&jobId)
	if err != nil {
		return "", fmt.Errorf("InsertJob: %w", err)
	}
	return jobId, nil
}

//Multi row
// func albumsByArtist(artist string) ([]Album, error) {
//     rows, err := db.Query("SELECT * FROM album WHERE artist = ?", artist)
//     if err != nil {
//         return nil, err
//     }
//     defer rows.Close()

//     // An album slice to hold data from returned rows.
//     var albums []Album

//     // Loop through rows, using Scan to assign column data to struct fields.
//     for rows.Next() {
//         var alb Album
//         if err := rows.Scan(&alb.ID, &alb.Title, &alb.Artist,
//             &alb.Price, &alb.Quantity); err != nil {
//             return albums, err
//         }
//         albums = append(albums, alb)
//     }
//     if err = rows.Err(); err != nil {
//         return albums, err
//     }
//     return albums, nil
// }
