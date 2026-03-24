package api

/*
Contains SendNotification (POST /api/v1/notify) and GetNotificationStatus (GET /api/v1/notifications).
Validates request, checks rate limit, resolves user, checks preferences, saves to DB, publishes to Kafka, returns 202.
Why needed: this is the door into your system. Every upstream service (order, auth, payment) calls this endpoint to trigger a notification.
*/

import (
	"encoding/json"
	"net/http"
	"time"

	"log/slog"

	"github.com/google/uuid"
	"github.com/yourname/notification-platform/internal/domain"
	"github.com/yourname/notification-platform/internal/kafka"
	"github.com/yourname/notification-platform/internal/ratelimit"
	"github.com/yourname/notification-platform/internal/store"
	"github.com/yourname/notification-platform/pkg/metrics"
)

// Handler holds all dependencies for HTTP handlers.
// Every handler method is on this struct — no global state, fully testable.
type Handler struct {
	producer  *kafka.Producer
	userCache *store.UserCache
	notifRepo domain.NotificationRepository
	limiter   *ratelimit.Limiter
	logger    *slog.Logger
}

// NewHandler creates a Handler with all required dependencies injected.
func NewHandler(
	producer *kafka.Producer,
	userCache *store.UserCache,
	notifRepo domain.NotificationRepository,
	limiter *ratelimit.Limiter,
	logger *slog.Logger,
) *Handler {
	return &Handler{
		producer:  producer,
		userCache: userCache,
		notifRepo: notifRepo,
		limiter:   limiter,
		logger:    logger,
	}
}

// Defining Request / Response DTOs
// DTOs (Data Transfer Objects) are separate from domain types.
// This way the API contract can evolve independently of internal structs.

// SendRequest is the body of POST /api/v1/notify.
type SendRequest struct {
	UserID     string            `json:"user_id"`
	Channel    string            `json:"channel"`
	TemplateID string            `json:"template_id"`
	Payload    map[string]string `json:"payload"`
}

// SendResponse is returned on successful publish to Kafka.
type SendResponse struct {
	NotificationID string `json:"notification_id"`
	Status         string `json:"status"`
	Message        string `json:"message"`
}

// StatusResponse is returned by GET /api/v1/notifications/:id.
type StatusResponse struct {
	NotificationID string `json:"notification_id"`
	Channel        string `json:"channel"`
	Status         string `json:"status"`
	CreatedAt      string `json:"created_at"`
}

// ErrorResponse is the standard error envelope for all 4xx/5xx responses.
type ErrorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// defining Handlers -> entry point in the system

// SendNotification handles POST /api/v1/notify.
//
// Flow:
//  1. Decode and validate request
//  2. Check rate limit for this user+channel
//  3. Resolve user data (email, phone, device token) from cache/DB
//  4. Check user preferences (has this channel opted in?)
//  5. Build Notification struct and save to DB
//  6. Publish to Kafka
//  7. Return 202 Accepted immediately — never wait for delivery
func (h *Handler) SendNotification(w http.ResponseWriter, r *http.Request) {
	// Here context carries Request life cycle
	// handles timeout cancellation and request-scope value.
	ctx := r.Context()
	log := h.logger.With("handler", "SendNotification")

	// 1. Decode request body
	var req SendRequest
	// create new decoder that reads stream data and decode it -> converd it into Go struct SendRequest
	// Here JSON -> DTO
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, "invalid request body", "BAD_REQUEST", http.StatusBadRequest)
		return
	}

	// 2. Validate sendRequest required fields
	// null check
	if req.UserID == "" || req.Channel == "" || req.TemplateID == "" {
		writeError(w, "user_id, channel, and template_id are required", "VALIDATION_ERROR", http.StatusBadRequest)
		return
	}
	// channel check
	channel := domain.Channel(req.Channel)
	validChannels := map[domain.Channel]bool{
		domain.ChannelIOS:     true,
		domain.ChannelAndroid: true,
		domain.ChannelSMS:     true,
		domain.ChannelEmail:   true,
	}
	if !validChannels[channel] {
		writeError(w, "invalid channel: must be ios, android, sms, or email", "VALIDATION_ERROR", http.StatusBadRequest)
		return
	}

	// 3. Rate limit check -> Redis sliding window
	allowed, err := h.limiter.Allow(ctx, req.UserID, req.Channel)
	if err != nil {
		// Rate limiter error  log and fail open (allow the notification)
		log.Error("rate limiter error, failing open", "error", err)
	}
	if !allowed {
		metrics.NotificationsSkippedTotal.WithLabelValues(req.Channel, "rate_limited").Inc()
		writeError(w, "rate limit exceeded for this channel", "RATE_LIMITED", http.StatusTooManyRequests)
		return
	}

	// 4. Resolve user info from cache (hits Redis first, falls back to Postgres)
	user, err := h.userCache.FindByID(ctx, req.UserID)
	if err != nil {
		log.Error("failed to resolve user", "user_id", req.UserID, "error", err)
		writeError(w, "user not found", "USER_NOT_FOUND", http.StatusNotFound)
		return
	}

	// 5. Check user preferences
	prefs, err := h.userCache.FindPreferences(ctx, req.UserID)
	if err != nil {
		log.Error("failed to load preferences", "user_id", req.UserID, "error", err)
		writeError(w, "internal server error", "INTERNAL_ERROR", http.StatusInternalServerError)
		return
	}

	if !isChannelEnabled(channel, prefs) {
		metrics.NotificationsSkippedTotal.WithLabelValues(req.Channel, "user_opted_out").Inc()
		// Return 200 not 4xx — the caller did nothing wrong, the user just opted out
		writeJSON(w, http.StatusOK, SendResponse{
			Status:  "skipped",
			Message: "user has disabled this notification channel",
		})
		return
	}

	// 6. Build the Notification — enrich with resolved user data
	// Workers will use RecipientEmail/Phone/Token directly
	// so they don't need to hit the DB/cache again
	notif := &domain.Notification{
		ID:             uuid.NewString(),
		UserID:         req.UserID,
		Channel:        channel,
		TemplateID:     req.TemplateID,
		Payload:        req.Payload,
		Status:         domain.StatusPending,
		CreatedAt:      time.Now(),
		RecipientEmail: user.Email,
		RecipientPhone: user.Phone,
		RecipientToken: user.DeviceToken,
	}

	// 7. Persist notification record before publishing
	if err := h.notifRepo.Save(ctx, notif); err != nil {
		log.Error("failed to save notification", "error", err)
		writeError(w, "internal server error", "INTERNAL_ERROR", http.StatusInternalServerError)
		return
	}

	// 8. Publish to Kafka — this is async, returns immediately
	if err := h.producer.Publish(ctx, notif); err != nil {
		log.Error("failed to publish to kafka",
			"notification_id", notif.ID,
			"channel", channel,
			"error", err,
		)
		metrics.KafkaPublishTotal.WithLabelValues(req.Channel, "failure").Inc()
		writeError(w, "failed to queue notification", "INTERNAL_ERROR", http.StatusInternalServerError)
		return
	}

	metrics.KafkaPublishTotal.WithLabelValues(req.Channel, "success").Inc()
	log.Info("notification queued",
		"notification_id", notif.ID,
		"user_id", req.UserID,
		"channel", channel,
	)

	// 9. Return 202 Accepted — delivery happens asynchronously
	writeJSON(w, http.StatusAccepted, SendResponse{
		NotificationID: notif.ID,
		Status:         "queued",
		Message:        "notification accepted for delivery",
	})
}

// GetNotificationStatus handles GET /api/v1/notifications/{id}.
func (h *Handler) GetNotificationStatus(w http.ResponseWriter, r *http.Request) {
	// In chi, use chi.URLParam(r, "id")
	// For simplicity here we read from the query string
	id := r.URL.Query().Get("id")
	if id == "" {
		writeError(w, "notification id is required", "BAD_REQUEST", http.StatusBadRequest)
		return
	}

	notif, err := h.notifRepo.FindByID(r.Context(), id)
	if err != nil {
		writeError(w, "notification not found", "NOT_FOUND", http.StatusNotFound)
		return
	}

	writeJSON(w, http.StatusOK, StatusResponse{
		NotificationID: notif.ID,
		Channel:        string(notif.Channel),
		Status:         string(notif.Status),
		CreatedAt:      notif.CreatedAt.Format(time.RFC3339),
	})
}

// HealthCheck handles GET /health — used by Docker and Kubernetes probes.
func (h *Handler) HealthCheck(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// ── Helpers ───────────────────────────────────────────────────────────────────

func isChannelEnabled(channel domain.Channel, prefs *domain.UserPreferences) bool {
	switch channel {
	case domain.ChannelEmail:
		return prefs.EmailEnabled
	case domain.ChannelSMS:
		return prefs.SMSEnabled
	case domain.ChannelIOS, domain.ChannelAndroid:
		return prefs.PushEnabled
	default:
		return false
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, msg, code string, status int) {
	writeJSON(w, status, ErrorResponse{Error: msg, Code: code})
}
