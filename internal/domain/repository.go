package domain

/*
Defines three interfaces: NotificationRepository (Save, FindByID, UpdateStatus), DeliveryLogRepository (Insert, UpdateStatus, ExistsSent), UserRepository (FindByID, FindPreferences).
Why needed: the worker and handler depend on these interfaces, not on *pgxpool.Pool. This means you can pass a mock in tests without a real database running.
*/

import "context"

// NotificationRepository defines persistence operations for notifications.
// The worker depends on this interface, not on *pgx.Conn directly.
// This makes the worker testable with a mock — no real DB needed in tests.
type NotificationRepository interface {
	// Save inserts a new notification record.
	Save(ctx context.Context, n *Notification) error

	// FindByID retrieves a notification by its ID.
	FindByID(ctx context.Context, id string) (*Notification, error)

	// UpdateStatus updates the delivery status of a notification.
	UpdateStatus(ctx context.Context, id string, status Status) error
}

// DeliveryLogRepository defines persistence operations for delivery logs.
// A log is written BEFORE each send attempt — if the worker crashes,
// the log shows status=pending and the retry job can recover.
type DeliveryLogRepository interface {
	// Insert writes a new delivery attempt record with status=pending.
	// Called before the third-party API call.
	Insert(ctx context.Context, log *DeliveryLog) error

	// UpdateStatus updates a delivery log after the send attempt resolves.
	UpdateStatus(ctx context.Context, id string, status Status, errMsg string) error

	// ExistsSent checks if a notification was already successfully delivered.
	// Used for idempotency — Kafka at-least-once means we may see the same
	// message twice. If ExistsSent returns true, skip the send entirely.
	ExistsSent(ctx context.Context, notificationID string) (bool, error)
}

// UserRepository defines read operations for user data.
type UserRepository interface {
	// FindByID retrieves a user by ID.
	// Used by the resolver to enrich notifications before publishing to Kafka.
	FindByID(ctx context.Context, id string) (*User, error)

	// FindPreferences retrieves a user's notification preferences.
	FindPreferences(ctx context.Context, userID string) (*UserPreferences, error)
}
