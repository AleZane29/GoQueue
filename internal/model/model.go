package model

import "time"

type StatusValues string

const (
	StatusPending   StatusValues = "pending"
	StatusRunning   StatusValues = "running"
	StatusCompleted StatusValues = "completed"
	StatusFailed    StatusValues = "failed"
	StatusDead      StatusValues = "dead"
)

type Queue struct {
	Id     int
	Name   string
	Weight int
}

type QueueStats struct {
	NameQueue  string
	JobsStatus StatusValues
	NJob       int
}

type Job struct {
	Id               string
	QueueId          int
	Name             string
	Status           StatusValues
	Type             string
	Payload          string
	MaxTimeToExecute time.Duration
	Attempts         int
	MaxAttempts      int
	CreatedAt        time.Time
	ScheduledAt      time.Time
}

type Execution struct {
	Id          string
	JobId       string
	WorkerId    string
	Attempt     int
	StartedAt   time.Time
	CompletedAt time.Time
	Status      StatusValues
	ExError     string
}

type JobResponse struct {
	Id               string `json:"id"`
	QueueId          int    `json:"queue_id"`
	Name             string `json:"name"`
	Status           string `json:"status"`
	Type             string `json:"type"`
	Payload          string `json:"payload"`
	MaxTimeToExecute string `json:"max_time_to_execute"` // "5m0s"
	MaxAttempts      int    `json:"max_attempts"`
	CreatedAt        string `json:"created_at"`
	ScheduledAt      string `json:"scheduled_at"`
}
