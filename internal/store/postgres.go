package store

/*
Implements all three repository interfaces (NotificationRepository, DeliveryLogRepository, UserRepository)
using a single pgxpool connection pool.
Also contains Migrate() which creates all tables on startup.
*/

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Durgendra-kumar/NotificationSystem/internal/config"
	"github.com/Durgendra-kumar/NotificationSystem/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store implements all repository interfaces using PostgreSQL.
// One struct, one connection pool, all DB operations in one place.
type Store struct {
	pool *pgxpool.Pool
}

// New creates a new Store with a connection pool.
func New(ctx context.Context, cfg config.PostgresConfig) (*Store, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("store: parse DSN: %w", err)
	}

	poolCfg.MaxConns = int32(cfg.MaxOpenConns)
	poolCfg.MinConns = int32(cfg.MaxIdleConns)
	poolCfg.MaxConnLifetime = cfg.ConnMaxLifetime

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("store: create pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("store: ping database: %w", err)
	}

	return &Store{pool: pool}, nil
}

// Close releases all connections in the pool.
func (s *Store) Close() {
	s.pool.Close()
}

// NotificationRepository
// it save Notificaiton into the notification respository
func (s *Store) Save(ctx context.Context, n *domain.Notification) error {
	//$1 is a placeholder (parameter binding)
	//$1 = id
	query := `
		INSERT INTO notifications (id, user_id, channel, template_id, payload, status, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	//Exec NSERT UPDATE DELETE execute sql query in Prostgres and resturn result
	_, err := s.pool.Exec(ctx, query,
		n.ID, n.UserID, n.Channel, n.TemplateID, n.Payload, n.Status, n.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("store.Save: %w", err)
	}
	return nil
}

// find Notification BY ID
func (s *Store) FindByID(ctx context.Context, id string) (*domain.Notification, error) {
	q := `
		SELECT id, user_id, channel, template_id, payload, status, created_at
		FROM notifications WHERE id = $1
	`
	//QueryRow-> return exactly ONE row
	// row is a wrapper that holds the result
	row := s.pool.QueryRow(ctx, q, id)

	//extract data using .Scan()
	var n domain.Notification
	err := row.Scan(&n.ID, &n.UserID, &n.Channel, &n.TemplateID, &n.Payload, &n.Status, &n.CreatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("store.FindByID: not found: %s", id)
		}
		return nil, fmt.Errorf("store.FindByID: %w", err)
	}
	return &n, nil
}

// upade notification status
func (s *Store) UpdateStatus(ctx context.Context, id string, status domain.Status) error {
	q := `UPDATE notifications SET status = $1 WHERE id = $2`
	_, err := s.pool.Exec(ctx, q, status, id)
	if err != nil {
		return fmt.Errorf("store.UpdateStatus: %w", err)
	}
	return nil
}

// DeliveryLogRepository
// insert Notification that is goint to deliver thirdparty into Delivery log
func (s *Store) Insert(ctx context.Context, log *domain.DeliveryLog) error {
	q := `
		INSERT INTO delivery_logs (id, notification_id, channel, attempt, status, error_message, attempted_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	_, err := s.pool.Exec(ctx, q,
		log.ID, log.NotificationID, log.Channel, log.Attempt,
		log.Status, log.ErrorMessage, log.AttemptedAt,
	)
	if err != nil {
		return fmt.Errorf("store.Insert delivery log: %w", err)
	}
	return nil
}

func (s *Store) UpdateDeliveryStatus(ctx context.Context, id string, status domain.Status, errMsg string) error {
	q := `UPDATE delivery_logs SET status = $1, error_message = $2 WHERE id = $3`
	_, err := s.pool.Exec(ctx, q, status, errMsg, id)
	if err != nil {
		return fmt.Errorf("store.UpdateDeliveryStatus: %w", err)
	}
	return nil
}

// ExistsSent checks if a notification was already successfully delivered. Used for idempotency
func (s *Store) ExistsSent(ctx context.Context, notificationID string) (bool, error) {
	q := `
		SELECT EXISTS (
			SELECT 1 FROM delivery_logs
			WHERE notification_id = $1 AND status = 'sent'
		)
	`
	var exists bool
	err := s.pool.QueryRow(ctx, q, notificationID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("store.ExistsSent: %w", err)
	}
	return exists, nil
}

// UserRepository
// find user by ID
func (s *Store) FindUserByID(ctx context.Context, id string) (*domain.User, error) {
	q := `SELECT id, email, phone, device_token, platform FROM users WHERE id = $1`
	row := s.pool.QueryRow(ctx, q, id)

	var u domain.User
	err := row.Scan(&u.ID, &u.Email, &u.Phone, &u.DeviceToken, &u.Platform)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("store.FindUserByID: not found: %s", id)
		}
		return nil, fmt.Errorf("store.FindUserByID: %w", err)
	}
	return &u, nil
}

// finding notificartion preferences by user
func (s *Store) FindPreferences(ctx context.Context, userID string) (*domain.UserPreferences, error) {
	q := `
		SELECT user_id, email_enabled, sms_enabled, push_enabled
		FROM user_preferences WHERE user_id = $1
	`
	row := s.pool.QueryRow(ctx, q, userID)

	var p domain.UserPreferences
	err := row.Scan(&p.UserID, &p.EmailEnabled, &p.SMSEnabled, &p.PushEnabled)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// No preferences row = all channels enabled by default
			return &domain.UserPreferences{
				UserID:       userID,
				EmailEnabled: true,
				SMSEnabled:   true,
				PushEnabled:  true,
			}, nil
		}
		return nil, fmt.Errorf("store.FindPreferences: %w", err)
	}
	return &p, nil
}

// DB migrations create and manage the structure of your database
// the tables, columns, and indexes themselves.
// Migration = a controlled way to change your database schema over time.
func (s *Store) Migrate(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS users (
		id           UUID PRIMARY KEY,
		email        TEXT NOT NULL,
		phone        TEXT,
		device_token TEXT,
		platform     TEXT,
		created_at   TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE TABLE IF NOT EXISTS user_preferences (
		user_id       UUID PRIMARY KEY REFERENCES users(id),
		email_enabled BOOLEAN DEFAULT TRUE,
		sms_enabled   BOOLEAN DEFAULT TRUE,
		push_enabled  BOOLEAN DEFAULT TRUE
	);

	CREATE TABLE IF NOT EXISTS notifications (
		id          UUID PRIMARY KEY,
		user_id     UUID NOT NULL REFERENCES users(id),
		channel     TEXT NOT NULL,
		template_id TEXT NOT NULL,
		payload     JSONB,
		status      TEXT NOT NULL DEFAULT 'pending',
		created_at  TIMESTAMPTZ DEFAULT NOW()
	);

	-- Composite index on the most common query: "show me all notifications for user X, newest first"
	CREATE INDEX IF NOT EXISTS idx_notifications_user_created
		ON notifications(user_id, created_at DESC);

	CREATE TABLE IF NOT EXISTS delivery_logs (
		id              UUID PRIMARY KEY,
		notification_id UUID NOT NULL REFERENCES notifications(id),
		channel         TEXT NOT NULL,
		attempt         INT  NOT NULL DEFAULT 1,
		status          TEXT NOT NULL,
		error_message   TEXT,
		attempted_at    TIMESTAMPTZ DEFAULT NOW()
	);

	-- Index for idempotency check: "was this notification already sent?"
	CREATE INDEX IF NOT EXISTS idx_delivery_logs_notification_status
		ON delivery_logs(notification_id, status);
	`

	_, err := s.pool.Exec(ctx, schema)
	if err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	_ = time.Now() // suppress unused import in minimal builds
	return nil
}
