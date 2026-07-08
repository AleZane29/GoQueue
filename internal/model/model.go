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

type Job struct {
	Id               string
	QueueId          int
	Name             string
	Status           StatusValues
	Type             string
	Payload          string
	MaxTimeToExecute time.Duration
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
