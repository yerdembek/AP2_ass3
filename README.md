# AP2 Microservices — Assignment 4: Performance Optimization & External Integrations

A production-ready microservices system built with Go, PostgreSQL, RabbitMQ, and Redis.
Implements Redis Caching, the Adapter Pattern, Parallel Background Workers with Exponential Backoff, and a Redis Rate Limiter.

---

## Architecture Overview

```
 ┌─────────────────────────────────────────────────────────────────┐
 │  Client (Postman / curl)                                        │
 └───────────────────────┬─────────────────────────────────────────┘
                         │ HTTP REST
                         ▼
 ┌───────────────────────────────────────────────────────────────┐
 │  Order Service  (:8080 HTTP / :50051 gRPC)                    │
 │                                                               │
 │  ┌──────────────────┐   ┌──────────────────────────────────┐  │
 │  │  Rate Limiter    │   │  Cache-Aside (Redis)             │  │
 │  │  Middleware      │   │  GET /orders/:id                 │  │
 │  │  (Redis INCR)    │   │  → Redis HIT → return fast       │  │
 │  │  HTTP 429 on     │   │  → Redis MISS → DB → cache.Set   │  │
 │  │  limit exceeded  │   │  On status change → cache.Delete │  │
 │  └──────────────────┘   └──────────────────────────────────┘  │
 └────────────────────────────┬──────────────────────────────────┘
                              │ gRPC
                              ▼
 ┌──────────────────────────────────────────────────────────────┐
 │  Payment Service  (:50052 gRPC)                              │
 │  Processes payment → publishes PaymentEvent to RabbitMQ      │
 └────────────────────────────┬─────────────────────────────────┘
                              │ AMQP (payment.completed queue)
                              ▼
 ┌──────────────────────────────────────────────────────────────┐
 │  Notification Service (Background Worker Pool)               │
 │                                                              │
 │  ┌───────────┐  ┌───────────┐  ┌───────────┐                │
 │  │ Worker 1  │  │ Worker 2  │  │ Worker 3  │  ← parallel    │
 │  │ (barista) │  │ (barista) │  │ (barista) │    goroutines  │
 │  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘               │
 │        └──────────────┼──────────────┘                      │
 │                       │                                      │
 │  ┌──────────────────────────────────────────────────┐        │
 │  │ Redis Idempotency (SET NX)                       │        │
 │  │ → if already processed: ACK & skip               │        │
 │  └──────────────────────────────────────────────────┘        │
 │                       │                                      │
 │  ┌──────────────────────────────────────────────────┐        │
 │  │ sendWithRetry (Exponential Backoff)               │        │
 │  │   attempt 1 → fail → sleep 2s                    │        │
 │  │   attempt 2 → fail → sleep 4s                    │        │
 │  │   attempt 3 → success → ACK                      │        │
 │  │   all failed → NACK → DLQ                        │        │
 │  └──────────────────────────────────────────────────┘        │
 │                       │                                      │
 │  ┌──────────────────────────────────────────────────┐        │
 │  │ EmailSender interface (Adapter Pattern)           │        │
 │  │   SIMULATED: random latency + 30% failure rate   │        │
 │  │   REAL:      SMTP via net/smtp + STARTTLS         │        │
 │  └──────────────────────────────────────────────────┘        │
 └──────────────────────────────────────────────────────────────┘

Shared Infrastructure: Redis | PostgreSQL | RabbitMQ
```

---

## Quick Start

```bash
docker-compose up --build
```

Services start in order: PostgreSQL → RabbitMQ → Redis → payment-service → order-service → notification-service.

### Test the API

```bash
# Create an order (triggers payment + notification pipeline)
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_email":"test@example.com","amount":99.99}'

# Get order — first call hits DB and populates Redis cache
curl http://localhost:8080/orders/1

# Second call — served from Redis cache (no DB query)
curl http://localhost:8080/orders/1
```

### Rate Limiter Test (Bonus)

```bash
# Send 11+ requests — the 11th returns HTTP 429
for i in $(seq 1 12); do curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/orders/1; done
```

---

## Assignment 4 Features

### 1. Redis Cache-Aside Pattern (Order Service)

**Strategy:** Cache-Aside (Lazy Loading)

- **Read path (`GET /orders/:id`):**
  1. Check Redis key `order:<id>`
  2. **Cache HIT** → return cached JSON (no DB query, low latency)
  3. **Cache MISS** → query PostgreSQL → populate Redis in background goroutine → return result

- **Invalidation (atomic):** After `UpdateOrderStatus()` is called in the DB, the handler immediately calls `cache.Delete(orderID)`. This prevents serving stale data (e.g., showing "pending" status after a successful payment).

- **TTL:** Configurable via `CACHE_TTL_SECONDS` (default: 300s / 5 minutes). Even if invalidation fails, the key auto-expires.

**Redis key format:** `order:<id>`

### 2. Email Adapter Pattern (Notification Service)

The `EmailSender` interface decouples the worker from vendor-specific code:

```go
type EmailSender interface {
    Send(to, subject, body string) error
}
```

| `PROVIDER_MODE` | Implementation | Behavior |
|---|---|---|
| `SIMULATED` (default) | `SimulatedEmailSender` | 100–500ms latency + 30% random failure rate |
| `REAL` | `SMTPEmailSender` | Real SMTP via `net/smtp` with STARTTLS |

The consumer never changes — only the implementation injected at startup changes. This is the **Adapter Pattern**.

### 3. Parallel Background Workers (Barista Model)

The notification service runs **N parallel goroutines** (configured via `NUM_WORKERS`, default: 3). Each goroutine is an independent worker that:

1. Reads messages from the shared RabbitMQ queue concurrently
2. Checks Redis for duplicate detection (`EXISTS notif:processed:<eventID>`)
3. Calls `sendWithRetry()` with **exponential backoff**
4. Marks as processed with `SET NX` (atomic — prevents two workers processing the same event)
5. Sends `ACK` or `NACK` to RabbitMQ

This is analogous to multiple baristas at a coffee shop: they all take orders from the same queue simultaneously, each handles their own order independently, and they retry if the coffee machine fails.

### 4. Exponential Backoff Retry

```
attempt 1 → fail → sleep 2s   (baseDelay * 2^0)
attempt 2 → fail → sleep 4s   (baseDelay * 2^1)
attempt 3 → fail → sleep 8s   (baseDelay * 2^2)
attempt N → all failed → NACK → Dead Letter Queue
```

Configured via: `MAX_RETRIES=3`, `RETRY_BASE_SECONDS=2`.

### 5. Redis Idempotency (Notification Service)

Before sending any notification, each worker checks:
```
EXISTS notif:processed:<eventID>
```
- **Found** → ACK and skip (no duplicate email sent)
- **Not found** → process, then `SET NX notif:processed:<eventID> 1 EX <48h>`

`SET NX` (Set if Not eXists) is atomic — if two workers race, only one wins and the other safely skips.

**TTL:** 48h (`IDEMPOTENCY_TTL_HOURS=48`) — keys auto-expire to prevent unbounded growth.

### 6. Redis Rate Limiter — Bonus (+10%)

HTTP middleware on the Order Service using the **Fixed Window** algorithm:

```
INCR rate:<clientIP>   → atomically increment counter
EXPIRE (on first hit)  → set window expiry
if counter > limit     → HTTP 429 Too Many Requests
```

Configured via: `RATE_LIMIT_REQUESTS=10`, `RATE_LIMIT_WINDOW_SECONDS=60`.
Returns `Retry-After` header with the window duration.

---

## Configuration (`.env`)

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection URL |
| `CACHE_TTL_SECONDS` | `300` | Order cache TTL (5 min) |
| `RATE_LIMIT_REQUESTS` | `10` | Max requests per window |
| `RATE_LIMIT_WINDOW_SECONDS` | `60` | Rate limit window size |
| `PROVIDER_MODE` | `SIMULATED` | `SIMULATED` or `REAL` |
| `NUM_WORKERS` | `3` | Parallel notification workers |
| `MAX_RETRIES` | `3` | Max retry attempts per notification |
| `RETRY_BASE_SECONDS` | `2` | Base delay for exponential backoff |
| `IDEMPOTENCY_TTL_HOURS` | `48` | Redis idempotency key lifetime |
| `SMTP_HOST` / `SMTP_PORT` / `SMTP_USER` / `SMTP_PASS` | — | SMTP config (only for `REAL` mode) |

---

## Inspect Redis State

```bash
# List all cached orders
docker exec -it redis redis-cli KEYS "order:*"

# Inspect a cached order
docker exec -it redis redis-cli GET "order:1"

# List processed notification events
docker exec -it redis redis-cli KEYS "notif:processed:*"

# Inspect rate limiter counters
docker exec -it redis redis-cli KEYS "rate:*"
docker exec -it redis redis-cli GET "rate:172.17.0.1"
```

---

## Project Structure

```
ap2_ass3/
├── docker-compose.yml          # Infrastructure: PG + RabbitMQ + Redis + services
├── .env                        # All configuration
├── init.sql                    # DB schema
├── order-service/
│   ├── internal/
│   │   ├── cache/
│   │   │   └── order_cache.go          # Redis Cache-aside implementation
│   │   ├── handler/
│   │   │   ├── order_handler.go        # gRPC handler with cache integration
│   │   │   └── http_handler.go         # REST gateway
│   │   ├── middleware/
│   │   │   └── rate_limiter.go         # Redis rate limiter (Bonus)
│   │   └── repository/
│   │       └── order_repo.go           # PostgreSQL repository
│   └── cmd/main.go
├── payment-service/
│   ├── internal/
│   │   ├── handler/                    # gRPC payment handler
│   │   ├── messaging/                  # RabbitMQ publisher
│   │   └── repository/                 # PostgreSQL repository
│   └── cmd/main.go
└── notification-service/
    ├── internal/
    │   ├── provider/
    │   │   ├── email_sender.go         # EmailSender interface (Adapter)
    │   │   ├── simulated_provider.go   # Mock with latency + random failures
    │   │   └── smtp_provider.go        # Real SMTP adapter
    │   ├── consumer/
    │   │   └── notification_consumer.go # Parallel worker pool + retry + backoff
    │   └── repository/
    │       ├── idempotency_store.go         # PostgreSQL idempotency (fallback)
    │       └── redis_idempotency_store.go   # Redis idempotency (primary)
    └── cmd/main.go
```
