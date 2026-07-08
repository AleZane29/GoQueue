package store

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/AleZane29/GoQueue/internal/model"
)

func dbConnect() (*sql.DB, error) {
	connStr := "postgres://postgres:@localhost:5432/GoQueue?sslmode=disable"
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	return db, nil
}

func getNextJob(queueId int) (model.Job, error) {
	db, dbErr := dbConnect()
	if dbErr == nil {
		return model.Job{}, dbErr
	}
	job := model.Job{}
	query := `SELECT id, queue_id, name, status, type, payload, max_time_to_execute, max_attempts, created_at, scheduled_at 
          FROM public.jobs 
          WHERE status = 'Pending' AND queue_id = $1 AND scheduled_at <= NOW() 
          ORDER BY scheduled_at ASC LIMIT 1`

	if err := db.QueryRow(query, queueId).Scan(
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
	); err != nil {
		if err == sql.ErrNoRows {
			return model.Job{}, fmt.Errorf("No pending job found for queue %d", queueId)
		}
		return model.Job{}, fmt.Errorf("Error fetching next job for queue %d: %v", queueId, err)
	}
	return job, nil
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
