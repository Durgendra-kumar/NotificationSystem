package retry

/*
Do(ctx, config, fn) calls fn up to MaxAttempts times.
Waits 100ms, 200ms, 400ms between attempts (exponential backoff).
 Stops early if ctx is cancelled. Returns ErrMaxRetriesExceeded after all attempts fail.
Why needed: third-party APIs (APNs, Twilio) fail transiently — network blip, brief outage.
 Without retry, one bad second causes permanent failures. With retry, transient errors self-heal.
*/
/*
Read the Do() function. Trace through: attempt 1 fails -> wait 100ms -> attempt 2 fails -> wait 200ms -> attempt 3 fails -> return error. Then retype it.
Goal: these files have zero dependencies on the rest of the project. Perfect starting point for writing code.
*/

import (
	"context"
	"fmt"
	"math"
	"time"
)

// ErrMaxRetriesExceeded is returned when all retry attempts are exhausted.
var ErrMaxRetriesExceeded = fmt.Errorf("max retries exceeded")

// Config controls retry behaviour.
type Config struct {
	MaxAttempts int           // total attempts including the first
	BaseDelay   time.Duration // delay before the second attempt
	MaxDelay    time.Duration // cap on delay growth
}

// DefaultConfig is the standard retry config for third-party provider calls.
var DefaultConfig = Config{
	MaxAttempts: 3,
	BaseDelay:   100 * time.Millisecond,
	MaxDelay:    5 * time.Second,
}

// Do calls fn up to cfg.MaxAttempts times with exponential backoff.
// Stops early if ctx is cancelled.
// Returns nil on first success, ErrMaxRetriesExceeded after all attempts fail.
func Do(ctx context.Context, cfg Config, fn func(attempt int) error) error {
	var lastErr error

	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		lastErr = fn(attempt)
		if lastErr == nil {
			return nil // success
		}

		// Last attempt failed — don't wait, just return
		if attempt == cfg.MaxAttempts {
			break
		}

		// Calculate backoff: 100ms, 200ms, 400ms... capped at MaxDelay
		delay := time.Duration(math.Pow(2, float64(attempt-1))) * cfg.BaseDelay
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("retry cancelled after attempt %d: %w", attempt, ctx.Err())
		case <-time.After(delay):
			// continue to next attempt
		}
	}

	return fmt.Errorf("%w after %d attempts: %v", ErrMaxRetriesExceeded, cfg.MaxAttempts, lastErr)
}
