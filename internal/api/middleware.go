package api

/*
Three middleware functions: RequestID (stamps every request with a unique ID),
Logger (logs method/path/status/duration for every request),
Recoverer (catches panics and returns 500 instead of crashing the server).
Why needed: without RequestID you cannot trace a single request through your logs. Without Recoverer one bad request can kill the entire server.
*/

/*
Read Logger and Recoverer. Understand the responseWriter wrapper — why we need it to capture the status code.
*/

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/Durgendra-kumar/NotificationSystem/pkg/metrics"
	"github.com/google/uuid"
)

// RequestID injects a unique request ID into every request.
// The ID appears in all log lines for that request — essential for tracing
// a single request through your logs when debugging production issues.

// http.Handler -> Anything that implements ServeHTTP is a handler
// ServeHTTP -> main method that handles an HTTP request in Go
func RequestID(next http.Handler) http.Handler {
	//HandlerFunc -> Converts a function into a handler
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", id)
		// store in context so every handler and log line can read it
		ctx := contextWithRequestID(r.Context(), id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// Logger logs every request: method, path, status, duration.
// Uses a responseWriter wrapper to capture the status code.
// middleware.go — updated Logger middleware
func Logger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}

			// count request in flight
			metrics.HTTPRequestsInFlight.Inc()
			defer metrics.HTTPRequestsInFlight.Dec()

			next.ServeHTTP(rw, r)

			duration := time.Since(start).Seconds()
			status := strconv.Itoa(rw.status)

			// count every request with method + path + status
			metrics.HTTPRequestsTotal.
				WithLabelValues(r.Method, r.URL.Path, status).
				Inc()

			// record how long it took
			metrics.HTTPRequestDuration.
				WithLabelValues(r.Method, r.URL.Path).
				Observe(duration)

			logger.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rw.status,
				"duration_ms", time.Since(start).Milliseconds(),
				"request_id", requestIDFromContext(r.Context()),
			)
		})
	}
}

// Recoverer catches panics in handlers and returns 500 instead of crashing.
// Always add this — a panic in one handler should not take down the server.
func Recoverer(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					logger.Error("panic recovered",
						"panic", rec,
						"path", r.URL.Path,
						"request_id", requestIDFromContext(r.Context()),
					)
					writeError(w, "internal server error", "INTERNAL_ERROR", http.StatusInternalServerError)
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}

// contextKeyRequestID is an unexported type used as a context key.
// Using a custom type prevents collisions with other packages
// that might also store a string under the key "request_id".
type contextKeyRequestID struct{}

// contextWithRequestID stores the request ID in the context.
// Every handler can then retrieve it for logging.
func contextWithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, contextKeyRequestID{}, id)
}

// requestIDFromContext retrieves the request ID from context.
// Returns empty string if none was set — never panics.
func requestIDFromContext(ctx context.Context) string {
	id, _ := ctx.Value(contextKeyRequestID{}).(string)
	return id
}
