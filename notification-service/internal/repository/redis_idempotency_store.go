package repository

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisIdempotencyStore uses Redis to track which payment events have already
// been processed by the notification worker.
//
// Why Redis instead of PostgreSQL here?
//   - Speed: Redis lookups are O(1) in memory vs. a DB round-trip
//   - TTL: Keys expire automatically (no manual cleanup needed)
//   - Atomicity: SET NX is atomic, preventing race conditions between workers
//
// Key format: "notif:processed:<paymentID>"
// TTL: configurable (default 48h) — long enough to prevent duplicates
// within a reasonable window even if a message is redelivered much later.
type RedisIdempotencyStore struct {
	client *redis.Client
	ttl    time.Duration
}

// NewRedisIdempotencyStore connects to Redis and returns an idempotency store.
func NewRedisIdempotencyStore(redisURL string, ttlHours int) (*RedisIdempotencyStore, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL: %w", err)
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err = client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	ttl := time.Duration(ttlHours) * time.Hour
	log.Printf("[RedisIdempotency] Connected to Redis (TTL=%s)", ttl)
	return &RedisIdempotencyStore{client: client, ttl: ttl}, nil
}

func (s *RedisIdempotencyStore) key(paymentID string) string {
	return fmt.Sprintf("notif:processed:%s", paymentID)
}

// IsDuplicate checks whether the paymentID has already been processed.
// Uses GET — safe for concurrent workers (multiple baristas checking the same order).
func (s *RedisIdempotencyStore) IsDuplicate(ctx context.Context, paymentID string) (bool, error) {
	exists, err := s.client.Exists(ctx, s.key(paymentID)).Result()
	if err != nil {
		return false, fmt.Errorf("redis EXISTS: %w", err)
	}
	return exists > 0, nil
}

// MarkProcessed atomically records the paymentID as processed using SET NX
// (set if not exists). This prevents a race condition where two workers
// process the same event simultaneously — only one will win the SET NX.
func (s *RedisIdempotencyStore) MarkProcessed(ctx context.Context, paymentID string) error {
	ok, err := s.client.SetNX(ctx, s.key(paymentID), "1", s.ttl).Result()
	if err != nil {
		return fmt.Errorf("redis SET NX: %w", err)
	}
	if !ok {
		// Another worker already marked this — treat as success (idempotent)
		log.Printf("[RedisIdempotency] Key already set for %s (race condition handled)", paymentID)
	}
	return nil
}

// Close closes the Redis connection.
func (s *RedisIdempotencyStore) Close() error {
	return s.client.Close()
}
