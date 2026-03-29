package store

/*
Wraps UserRepository with a Redis cache layer.
FindByID checks Redis first (key: user:{id}), falls back to Postgres on miss,
 then populates Redis for next time. Same pattern for FindPreferences.
*/

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Durgendra-kumar/NotificationSystem/internal/domain"
	"github.com/redis/go-redis/v9"
)

// UserCache sits in front of the UserRepository.
// Cache-aside pattern: check Redis -> on miss, hit Postgres and populate cache.
//
// This is the key optimisation your design described:
//   - Without cache: every POST /notify hits Postgres for user data -> DB bottleneck
//   - With cache:    ~95% of requests hit Redis (sub-ms) -> DB only sees cache misses
type UserCache struct {
	rdb   *redis.Client
	store *Store
	ttl   time.Duration
}

// NewUserCache creates a caching wrapper around the Store.
func NewUserCache(rdb *redis.Client, store *Store, ttl time.Duration) *UserCache {
	return &UserCache{rdb: rdb, store: store, ttl: ttl}
}

// FindByID returns a user, served from cache when possible.
func (c *UserCache) FindByID(ctx context.Context, id string) (*domain.User, error) {
	key := fmt.Sprintf("user:%s", id)

	// 1. Try cache first
	data, err := c.rdb.Get(ctx, key).Bytes()
	if err == nil {
		var u domain.User
		if err := json.Unmarshal(data, &u); err == nil {
			return &u, nil
		}
	}

	if !errors.Is(err, redis.Nil) {
		// Redis error (not a cache miss) — log and fall through to DB
		// We fail open: a Redis outage should not break notification delivery
	}

	// 2. Cache miss — hit the database
	u, err := c.store.FindUserByID(ctx, id)
	if err != nil {
		return nil, err
	}

	// 3. Populate cache for future requests
	if data, err := json.Marshal(u); err == nil {
		_ = c.rdb.Set(ctx, key, data, c.ttl).Err()
	}

	return u, nil
}

// FindPreferences returns user preferences, served from cache when possible.
func (c *UserCache) FindPreferences(ctx context.Context, userID string) (*domain.UserPreferences, error) {
	key := fmt.Sprintf("prefs:%s", userID)

	data, err := c.rdb.Get(ctx, key).Bytes()
	if err == nil {
		var p domain.UserPreferences
		if err := json.Unmarshal(data, &p); err == nil {
			return &p, nil
		}
	}

	p, err := c.store.FindPreferences(ctx, userID)
	if err != nil {
		return nil, err
	}

	if data, err := json.Marshal(p); err == nil {
		_ = c.rdb.Set(ctx, key, data, c.ttl).Err()
	}

	return p, nil
}
