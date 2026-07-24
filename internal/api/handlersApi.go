package api

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/AleZane29/GoQueue/internal/model"
)

func readJSON(r *http.Request, dst any) error {
	return json.NewDecoder(r.Body).Decode(dst)
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func errorResponse(msg string) map[string]string {
	return map[string]string{"error": msg}
}

// -----------------------------------------------------------------------
// Handlers
// -----------------------------------------------------------------------

// GET /health
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// POST /jobs
// Body: { "queue_id": 1, "name": "send_email", "type": "email",
//
//	"payload": "{}", "max_time_to_execute": "5m", "max_attempts": 3 }
func (s *Server) handleInsertJob(w http.ResponseWriter, r *http.Request) {
	var body struct {
		QueueId          int    `json:"queue_id"`
		Name             string `json:"name"`
		Type             string `json:"type"`
		Payload          string `json:"payload"`
		MaxTimeToExecute string `json:"max_time_to_execute"`
		MaxAttempts      int    `json:"max_attempts"`
	}

	if err := readJSON(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse("invalid request body"))
		return
	}

	// validazione minima
	if body.QueueId == 0 || body.Name == "" || body.Type == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse("queue_id, name and type are required"))
		return
	}

	// default sensati
	if body.MaxAttempts == 0 {
		body.MaxAttempts = 3
	}
	if body.MaxTimeToExecute == "" {
		body.MaxTimeToExecute = "5m"
	}
	if body.Payload == "" {
		body.Payload = "{}"
	}

	duration, err := time.ParseDuration(body.MaxTimeToExecute)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorResponse("invalid max_time_to_execute format, use Go duration (e.g. 5m, 30s)"))
		return
	}

	job := &model.Job{
		QueueId:          body.QueueId,
		Name:             body.Name,
		Type:             body.Type,
		Payload:          body.Payload,
		MaxTimeToExecute: duration,
		MaxAttempts:      body.MaxAttempts,
		Status:           model.StatusPending,
		ScheduledAt:      time.Now(),
	}

	id, err := s.store.InsertJob(r.Context(), job)
	if err != nil {
		log.Printf("handleInsertJob: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse("failed to insert job"))
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{"id": id})
}

// GET /jobs/{id}
func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse("missing job id"))
		return
	}

	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		log.Printf("handleGetJob: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse("failed to get job"))
		return
	}
	if job == nil {
		writeJSON(w, http.StatusNotFound, errorResponse("job not found"))
		return
	}

	writeJSON(w, http.StatusOK, job)
}

// GET /jobs?queue=high&status=pending&page=1&page_size=20
func (s *Server) handleListJobs(w http.ResponseWriter, r *http.Request) {
	queue := r.URL.Query().Get("queue")   // es. "high", "medium", "low"
	status := r.URL.Query().Get("status") // es. "pending", "running", "failed"

	jobs, err := s.store.ListJobs(r.Context(), queue, status)
	if err != nil {
		log.Printf("handleListJobs: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse("failed to list jobs"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"jobs":  jobs,
		"count": len(jobs),
	})
}

// POST /jobs/{id}/retry
// Rimette in pending un job dead, azzerando il contatore dei tentativi
func (s *Server) handleRetryJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse("missing job id"))
		return
	}

	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		log.Printf("handleRetryJob: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse("failed to get job"))
		return
	}
	if job == nil {
		writeJSON(w, http.StatusNotFound, errorResponse("job not found"))
		return
	}
	if job.Status != model.StatusDead && job.Status != model.StatusFailed {
		writeJSON(w, http.StatusBadRequest, errorResponse("only dead or failed jobs can be retried"))
		return
	}

	if err := s.store.RescheduleJob(r.Context(), id, time.Now()); err != nil {
		log.Printf("handleRetryJob reschedule: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse("failed to retry job"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"id": id, "status": string(model.StatusPending)})
}

// DELETE /jobs/{id}
// Cancella un job solo se è in stato pending
func (s *Server) handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if id == "" {
		writeJSON(w, http.StatusBadRequest, errorResponse("missing job id"))
		return
	}

	job, err := s.store.GetJob(r.Context(), id)
	if err != nil {
		log.Printf("handleDeleteJob: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse("failed to get job"))
		return
	}
	if job == nil {
		writeJSON(w, http.StatusNotFound, errorResponse("job not found"))
		return
	}
	if job.Status != model.StatusPending {
		writeJSON(w, http.StatusBadRequest, errorResponse("only pending jobs can be deleted"))
		return
	}

	if err := s.store.DeleteJob(r.Context(), id); err != nil {
		log.Printf("handleDeleteJob delete: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse("failed to delete job"))
		return
	}

	w.WriteHeader(http.StatusNoContent) // 204 — successo senza body
}

// GET /queues
// Ritorna le code con statistiche: quanti job pending, running, dead, completed
func (s *Server) handleGetQueues(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.FetchQueueStats(r.Context())
	if err != nil {
		log.Printf("handleGetQueues: %v", err)
		writeJSON(w, http.StatusInternalServerError, errorResponse("failed to get queue stats"))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"queues": stats,
	})
}
