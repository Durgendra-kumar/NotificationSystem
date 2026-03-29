package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/Durgendra-kumar/NotificationSystem/internal/domain"
	"github.com/Durgendra-kumar/NotificationSystem/internal/kafka"
	"github.com/Durgendra-kumar/NotificationSystem/internal/retry"
	"github.com/Durgendra-kumar/NotificationSystem/pkg/metrics"
	"github.com/google/uuid"
)

// Worker pulls notifications from Kafka for one channel, persists them,
// and delivers them via the channel's Sender.
// Delivery order:
//  1. Idempotency check  — skip if already delivered (handles Kafka redelivery)
//  2. Persist to DB      — write delivery_log with status=pending BEFORE sending
//  3. Call sender        — call third-party API (APNS, FCM, Twilio, SendGrid)
//  4. Update DB status   — mark as sent or failed
type Worker struct {
	consumer *kafka.Consumer
	sender   domain.Sender
	logRepo  domain.DeliveryLogRepository
	logger   *slog.Logger
	retryCfg retry.Config
}

// New creates a Worker for one notification channel.
func New(
	consumer *kafka.Consumer,
	sender domain.Sender,
	logRepo domain.DeliveryLogRepository,
	logger *slog.Logger,
) *Worker {
	return &Worker{
		consumer: consumer,
		sender:   sender,
		logRepo:  logRepo,
		logger:   logger.With("channel", sender.Channel()),
		retryCfg: retry.DefaultConfig,
	}
}

// Run starts the worker loop. Blocks until ctx is cancelled.
// Call this in a goroutine from cmd/worker/main.go.
// reading Notification form kafka
func (w *Worker) Run(ctx context.Context) {
	w.logger.Info("worker started")

	for {
		// ReadOne blocks until a message arrives or ctx is cancelled.
		n, err := w.consumer.ReadOne(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				w.logger.Info("worker shutting down")
				return
			}
			w.logger.Error("failed to read from kafka", "error", err)
			continue
		}

		if err := w.process(ctx, n); err != nil {
			w.logger.Error("failed to process notification",
				"notification_id", n.ID,
				"error", err,
			)
		}
	}
}

// process handles one notification end-to-end.
// This method is the core of the entire worker — every design decision lives here.
func (w *Worker) process(ctx context.Context, n *domain.Notification) error {
	start := time.Now()
	log := w.logger.With("notification_id", n.ID, "user_id", n.UserID)

	// Step 1: Idempotency check
	// Kafka delivers at-least-once. If the worker crashes after sending but
	// before committing the offset, Kafka will redeliver the same message.
	// This check prevents the user receiving the same notification twice.
	alreadySent, err := w.logRepo.ExistsSent(ctx, n.ID)
	if err != nil {
		return fmt.Errorf("idempotency check: %w", err)
	}
	if alreadySent {
		log.Info("notification already delivered, skipping (idempotent)")
		metrics.NotificationsSkippedTotal.WithLabelValues(string(w.sender.Channel()), "duplicate").Inc()
		return nil
	}

	// Step 2: Persist delivery log BEFORE sending
	// Writing the log first means: if the worker crashes between step 2 and
	// step 3, the log shows status=pending. A recovery job can find all
	// pending logs and retry them. Without this, a crash between send and
	// persist creates a "ghost" — sent but unrecorded.
	logID := uuid.NewString()
	deliveryLog := &domain.DeliveryLog{
		ID:             logID,
		NotificationID: n.ID,
		Channel:        w.sender.Channel(),
		Attempt:        1,
		Status:         domain.StatusPending,
		AttemptedAt:    time.Now(),
	}

	if err := w.logRepo.Insert(ctx, deliveryLog); err != nil {
		return fmt.Errorf("persist delivery log: %w", err)
	}

	log.Info("delivery log persisted, attempting send")

	// Step 3: Send via third-party provider (with retry)
	var sendErr error
	retryErr := retry.Do(ctx, w.retryCfg, func(attempt int) error {
		if attempt > 1 {
			log.Info("retrying send", "attempt", attempt)
		}
		sendErr = w.sender.Send(ctx, n)
		return sendErr
	})

	// Step 4: Update delivery log with final status
	if retryErr != nil {
		errMsg := retryErr.Error()
		if updateErr := w.logRepo.UpdateDeliveryStatus(ctx, logID, domain.StatusFailed, errMsg); updateErr != nil {
			log.Error("failed to update delivery log to failed", "error", updateErr)
		}
		metrics.NotificationsFailedTotal.WithLabelValues(string(w.sender.Channel())).Inc()
		return fmt.Errorf("send failed after retries: %w", retryErr)
	}

	if updateErr := w.logRepo.UpdateDeliveryStatus(ctx, logID, domain.StatusSent, ""); updateErr != nil {
		// Non-fatal: the notification was delivered. Log the failure but don't
		// return an error — returning an error would cause Kafka to redeliver.
		log.Error("notification sent but failed to update delivery log", "error", updateErr)
	}

	metrics.NotificationsSentTotal.WithLabelValues(string(w.sender.Channel())).Inc()
	metrics.DeliveryDuration.WithLabelValues(string(w.sender.Channel())).Observe(time.Since(start).Seconds())

	log.Info("notification delivered successfully",
		"duration_ms", time.Since(start).Milliseconds(),
	)

	return nil
}

// Close shuts down the Kafka consumer cleanly.
func (w *Worker) Close() error {
	return w.consumer.Close()
}
