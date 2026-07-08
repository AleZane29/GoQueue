package model

type statusValues string

const (
	Pending   statusValues = "Pending"
	Running   statusValues = "Running"
	Completed statusValues = "Completed"
	Failed    statusValues = "Failed"
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
	Status           statusValues
	Type             string
	Payload          string
	MaxTimeToExecute string
	MaxAttempts      int
	CreatedAt        string
	ScheduledAt      string
}

type Execution struct {
	Id          string
	JobId       string
	WorkerId    string
	Attempt     int
	StartedAt   string
	CompletedAt string
	Status      statusValues
	ExError     string
}