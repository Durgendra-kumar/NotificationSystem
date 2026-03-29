package api

/*
Wires all routes (POST /notify, GET /notifications, GET /health, GET /metrics) and attaches middleware. Uses the chi router.
Why needed: separates routing from handler logic. handler.go stays focused on business logic, router.go stays focused on URL mapping.
*/

/*
Read it — it is short. Just wires routes to handlers with middleware applied.
*/

import (
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewRouter creates the HTTP router with all routes and middleware registered.
// Chi is used because it is stdlib-compatible and lightweight.
func NewRouter(h *Handler, logger *slog.Logger) http.Handler {
	r := chi.NewRouter()

	// Global middleware — applied to every request
	// Order matters: RequestID first so logger can include it.
	r.Use(requestIDMiddleware)
	r.Use(Logger(logger))
	r.Use(Recoverer(logger))

	// Health and metrics — no auth required
	r.Get("/health", h.HealthCheck)
	r.Handle("/metrics", promhttp.Handler()) // Prometheus scrape endpoint

	// API v1
	r.Route("/api/v1", func(r chi.Router) {
		// POST /api/v1/notify — called by upstream microservices
		r.Post("/notify", h.SendNotification)

		// GET /api/v1/notifications?id={id} — check delivery status
		r.Get("/notifications", h.GetNotificationStatus)
	})

	return r
}

// requestIDMiddleware wraps RequestID to match chi's middleware signature.
func requestIDMiddleware(next http.Handler) http.Handler {
	return RequestID(next)
}
