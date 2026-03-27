package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

/*
Redis-based sliding window rate limiter. Allow(ctx, userID, channel) increments a Redis counter (key: rate:{userID}:{channel}),
sets TTL on first call. Returns false if count exceeds 100 per hour.
Rate limiter protects both the user and your third-party API budget.
*/

// Limiter enforces per-user, per-channel rate limits using a Redis counter.
// Algorithm: sliding window approximation via INCR + EXPIRE.
//
// Key pattern: rate:{userID}:{channel}
// On first call in a window: INCR (returns 1) + EXPIRE sets TTL.
// On subsequent calls: INCR only (TTL already set).
// If count > limit: reject.
type Limiter struct {
	redis  *redis.Client
	limit  int           // max notifications per window
	window time.Duration // window size (e.g. 1 hour)
}

// NewLimiter creates a rate limiter with the given limit per window.
func NewLimiter(rdb *redis.Client, limit int, window time.Duration) *Limiter {
	return &Limiter{
		redis:  rdb,
		limit:  limit,
		window: window,
	}
}

// Allow checks if the notification is within the rate limit.
// Returns true if allowed, false if the limit is exceeded.
// Side effect: increments the counter if allowed.

// working: for every user+channel we set TTL and increse count by 1, afte count increase beyond lmit. notification block.
// after TTL expires after 1 hr, count reset (by expire delete the key)
func (l *Limiter) Allow(ctx context.Context, userID, channel string) (bool, error) {
	//unique Redis key per user + channel
	key := fmt.Sprintf("rate:%s:%s", userID, channel)

	// Increment the counter atomically for Key
	//INCR is atomic, safe for Concurrency
	count, err := l.redis.Incr(ctx, key).Result()
	if err != nil {
		// If Redis is down, allow the notification — fail open.
		// Prefer sending a duplicate over dropping a valid notification.
		return true, fmt.Errorf("rate limit check: redis incr: %w", err)
	}

	// On the first increment in this window, set the expiry.
	// On subsequent increments the TTL is already set — EXPIRE would reset it,
	// so we only call it when count == 1.
	if count == 1 {
		if err := l.redis.Expire(ctx, key, l.window).Err(); err != nil {
			return true, fmt.Errorf("rate limit check: redis expire: %w", err)
		}
	}
	// for every user+channel we set TTL and increse count by 1, afte count increase beyond lmit. notification block.
	// after TTL expires after 1 hr, count reset (by expire delete the key)
	return count <= int64(l.limit), nil
}
