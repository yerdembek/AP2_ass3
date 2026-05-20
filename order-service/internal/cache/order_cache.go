package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// Order is a lightweight copy of repository.Order used for caching.
// We keep it here to avoid import cycles between cache and repository.
type Order struct {
	ID            int32   `json:"id"`
	CustomerEmail string  `json:"customer_email"`
	Amount        float64 `json:"amount"`
	Status        string  `json:"status"`
}

// OrderCache implements the Cache-aside pattern for orders using Redis.
// It is intentionally decoupled from the database layer — the handler
// coordinates between cache and repository.
type OrderCache struct {
	client *redis.Client
	ttl    time.Duration
}

// NewOrderCache creates a Redis-backed order cache.
// redisURL format: "redis://host:port/db"
func NewOrderCache(redisURL string, ttl time.Duration) (*OrderCache, error) {
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

	log.Printf("[OrderCache] Connected to Redis (TTL=%s)", ttl)
	return &OrderCache{client: client, ttl: ttl}, nil
}

// cacheKey returns the Redis key for a given order ID.
func (c *OrderCache) cacheKey(orderID int32) string {
	return fmt.Sprintf("order:%d", orderID)
}

// Get retrieves an order from cache.
// Returns (nil, nil) on cache miss so the caller can fall through to the DB.
func (c *OrderCache) Get(ctx context.Context, orderID int32) (*Order, error) {
	key := c.cacheKey(orderID)
	val, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		// Cache miss — not an error, caller should query DB
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("redis GET %s: %w", key, err)
	}

	var order Order
	if err = json.Unmarshal([]byte(val), &order); err != nil {
		return nil, fmt.Errorf("unmarshal cached order: %w", err)
	}
	return &order, nil
}

// Set stores an order in cache with the configured TTL.
func (c *OrderCache) Set(ctx context.Context, order *Order) error {
	key := c.cacheKey(order.ID)
	data, err := json.Marshal(order)
	if err != nil {
		return fmt.Errorf("marshal order: %w", err)
	}

	if err = c.client.Set(ctx, key, data, c.ttl).Err(); err != nil {
		return fmt.Errorf("redis SET %s: %w", key, err)
	}
	log.Printf("[OrderCache] Cached order #%d (TTL=%s)", order.ID, c.ttl)
	return nil
}

// Delete removes an order from cache (call this after status changes).
// This is the atomic invalidation step to prevent stale reads.
func (c *OrderCache) Delete(ctx context.Context, orderID int32) error {
	key := c.cacheKey(orderID)
	if err := c.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("redis DEL %s: %w", key, err)
	}
	log.Printf("[OrderCache] Invalidated cache for order #%d", orderID)
	return nil
}

// Close closes the Redis connection.
func (c *OrderCache) Close() error {
	return c.client.Close()
}
