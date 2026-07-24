package api

import (
	"log"
	"net/http"
	"time"
)

// responseWriter wrappa http.ResponseWriter per catturare lo status code
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}

// loggerMiddleware logga ogni richiesta con metodo, path, status e durata
func loggerMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := &responseWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(ww, r)
		log.Printf("%s %s %d %s", r.Method, r.URL.Path, ww.status, time.Since(start))
	})
}

// authMiddleware verifica che la richiesta abbia una API key valida
func authMiddleware(apiKey string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("Authorization")
			if key != "Bearer "+apiKey {
				writeJSON(w, http.StatusUnauthorized, errorResponse("unauthorized"))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
