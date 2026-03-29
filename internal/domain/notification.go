package domain

/*
Defines every core struct: Notification, User, UserPreferences, DeliveryLog. Also defines Channel type (ios/android/sms/email) and Status type (pending/sent/failed).
Why needed: every file in the project uses these types. Putting them here — with zero imports from other internal packages — means nothing in domain can cause a circular import.
domain
*/

//Read every struct. Ask: what is a Notification? what is a DeliveryLog? what is a Channel? Draw them on paper.

import "time"

// Channel represents a notification delivery channel.
// Adding a new channel = add a constant here, a partition in config, a worker.
type Channel string

const (
	ChannelIOS     Channel = "ios"
	ChannelAndroid Channel = "android"
	ChannelSMS     Channel = "sms"
	ChannelEmail   Channel = "email"
)

// Status tracks a notification through its lifecycle.
type Status string

const (
	StatusPending Status = "pending"
	StatusSent    Status = "sent"
	StatusFailed  Status = "failed"
	StatusSkipped Status = "skipped" // user opted out or rate limited
)

// Notification is the single struct that flows through the entire system.
// API receives it -> publishes to Kafka -> worker reads it -> sender delivers it.
type Notification struct {
	ID          string            `json:"id"`
	UserID      string            `json:"user_id"`
	Channel     Channel           `json:"channel"`
	TemplateID  string            `json:"template_id"`
	Payload     map[string]string `json:"payload"`
	Status      Status            `json:"status"`
	CreatedAt   time.Time         `json:"created_at"`
	ScheduledAt *time.Time        `json:"scheduled_at,omitempty"`

	// Resolved fields — populated by UserResolver before publishing to Kafka.
	// Workers use these to call third-party APIs without hitting the DB again.
	RecipientEmail string `json:"recipient_email,omitempty"`
	RecipientPhone string `json:"recipient_phone,omitempty"`
	RecipientToken string `json:"recipient_token,omitempty"` // APNs / FCM device token
}

// User holds information fetched from the DB/cache by the resolver.
type User struct {
	ID          string
	Email       string
	Phone       string
	DeviceToken string // APNs or FCM token
	Platform    string // "ios" or "android"
}

// UserPreferences controls which channels a user has opted into.
type UserPreferences struct {
	UserID       string
	EmailEnabled bool
	SMSEnabled   bool
	PushEnabled  bool
}

// DeliveryLog records every delivery attempt.
// Written to DB BEFORE the send — this is the crash-recovery record.
type DeliveryLog struct {
	ID             string
	NotificationID string
	Channel        Channel
	Attempt        int
	Status         Status
	ErrorMessage   string
	AttemptedAt    time.Time
}
