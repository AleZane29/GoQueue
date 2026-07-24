package api

import (
	"log"
	"net/http"
	"os"
	"time"

	"github.com/AleZane29/GoQueue/internal/store"
)

// -----------------------------------------------------------------------
// Server
// -----------------------------------------------------------------------

type Server struct {
	store      *store.Store
	httpServer *http.Server
}

func NewServer(store *store.Store) *Server {
	s := &Server{store: store}

	// router pubblico — nessuna autenticazione
	publicMux := http.NewServeMux()
	publicMux.HandleFunc("GET /health", s.handleHealth)

	// router protetto — richiede API key
	protectedMux := http.NewServeMux()
	protectedMux.HandleFunc("POST /jobs", s.handleInsertJob)
	protectedMux.HandleFunc("GET /jobs/{id}", s.handleGetJob)
	protectedMux.HandleFunc("GET /jobs", s.handleListJobs)
	protectedMux.HandleFunc("POST /jobs/{id}/retry", s.handleRetryJob)
	protectedMux.HandleFunc("DELETE /jobs/{id}", s.handleDeleteJob)
	protectedMux.HandleFunc("GET /queues", s.handleGetQueues)

	// combina i due router
	mainMux := http.NewServeMux()
	mainMux.Handle("/health", publicMux)
	mainMux.Handle("/", authMiddleware(os.Getenv("API_KEY"))(protectedMux))

	// applica il logger su tutto
	handler := loggerMiddleware(mainMux)

	s.httpServer = &http.Server{
		Addr:         "127.0.0.1:8080",
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return s
}

func (s *Server) Start() error {
	log.Printf("Server started at: %s", s.httpServer.Addr)
	return s.httpServer.ListenAndServe()
}

func (s *Server) Shutdown(ctx interface{ Done() <-chan struct{} }) error {
	// usa un context con timeout per lo shutdown
	return nil
}
