package middleware

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

// RateLimiter is an HTTP middleware that enforces per-IP request limits using Redis.
// It uses the "fixed window" algorithm:
//   - On first request from an IP: SET counter=1 with EXPIRE=windowSeconds
//   - On subsequent requests:      INCR counter; if counter > limit → HTTP 429
//
// This demonstrates Redis as a fast, shared state store for distributed systems.
type RateLimiter struct {
	client        *redis.Client
	maxRequests   int
	windowSeconds int
	next          http.Handler
}

// NewRateLimiter creates a Redis-backed rate-limiter middleware.
func NewRateLimiter(redisURL string, maxRequests, windowSeconds int) (*RateLimiter, error) {
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse redis URL: %w", err)
	}

	client := redis.NewClient(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err = client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("ping redis for rate limiter: %w", err)
	}

	log.Printf("[RateLimiter] Ready: %d requests per %ds", maxRequests, windowSeconds)
	return &RateLimiter{
		client:        client,
		maxRequests:   maxRequests,
		windowSeconds: windowSeconds,
	}, nil
}

// Wrap wraps the given handler with rate-limiting logic.
func (rl *RateLimiter) Wrap(next http.Handler) http.Handler {
	rl.next = next
	return rl
}

// ServeHTTP is the middleware entry point.
func (rl *RateLimiter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Identify the client by IP address
	clientIP := r.RemoteAddr
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		clientIP = forwarded
	}

	key := fmt.Sprintf("rate:%s", clientIP)
	ctx := r.Context()

	// Atomically increment and get the new counter value.
	// If the key does not exist, Redis creates it with value 1.
	count, err := rl.client.Incr(ctx, key).Result()
	if err != nil {
		// Redis failure → fail open (allow request) to avoid taking down the service
		log.Printf("[RateLimiter] Redis error for %s: %v — allowing request", clientIP, err)
		rl.next.ServeHTTP(w, r)
		return
	}

	// On first request, set the expiry window
	if count == 1 {
		rl.client.Expire(ctx, key, time.Duration(rl.windowSeconds)*time.Second)
	}

	if int(count) > rl.maxRequests {
		log.Printf("[RateLimiter] Rate limit exceeded for %s (count=%d, limit=%d)", clientIP, count, rl.maxRequests)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", fmt.Sprintf("%d", rl.windowSeconds))
		w.WriteHeader(http.StatusTooManyRequests)
		fmt.Fprintf(w, `{"error":"rate limit exceeded — max %d requests per %ds"}`, rl.maxRequests, rl.windowSeconds)
		return
	}

	rl.next.ServeHTTP(w, r)
}

// Close closes the Redis connection used by the rate limiter.
func (rl *RateLimiter) Close() error {
	return rl.client.Close()
}
