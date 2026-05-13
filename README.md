# AP2 Assignment 3 – Event-Driven Architecture with Message Queues

**Student:** Beknur Erdembek  
**Group:** SE-2406  
**Course:** Advanced Programming 2  
**Repository:** https://github.com/yerdembek/AP2_ass3

---

## Architecture Overview

This project implements an **event-driven microservices architecture** using gRPC for synchronous service-to-service communication and RabbitMQ for asynchronous event publishing. A REST HTTP gateway is exposed on port `8080` for external clients (e.g., Postman).

```
[Client / Postman]
       │
       │  HTTP REST  (POST /orders, GET /orders/{id})
       ▼
[Order Service  :8080 / :50051]
       │
       │  gRPC (synchronous call)
       ▼
[Payment Service  :50052]
       │
       ├─── DB Transaction → PostgreSQL (payments table)
       │
       └─── Publish JSON Event → RabbitMQ
                Exchange : payments        (direct, durable)
                Queue    : payment.completed (durable, DLX-backed)
                DLX      : payments.dlx
                DLQ      : payment.dead_letter
                       │
                       ▼
            [Notification Service]
            (RabbitMQ consumer – fully decoupled)
                       │
                       ├─── Idempotency Check  →  processed_events (PostgreSQL)
                       ├─── Log email simulation
                       └─── Manual ACK  →  RabbitMQ
```

---

## Services

| Service | Port(s) | Protocol | Role |
|---|---|---|---|
| **Order Service** | `8080` (HTTP), `50051` (gRPC) | REST + gRPC | Receives orders, persists to DB, calls Payment Service |
| **Payment Service** | `50052` | gRPC | Processes payment, commits to DB, publishes RabbitMQ event |
| **Notification Service** | — | RabbitMQ Consumer | Listens on `payment.completed`, deduplicates, logs email |
| **PostgreSQL** | `5432` | — | Shared database (`orders`, `payments`, `processed_events`) |
| **RabbitMQ** | `5672` (AMQP), `15672` (UI) | AMQP 0-9-1 | Message broker with DLX/DLQ support |

### Project Structure

```
AP2_ass3/
├── docker-compose.yml
├── init.sql                          # DB schema (orders, payments, processed_events)
├── proto/                            # Shared .proto definitions
├── order-service/
│   ├── cmd/main.go                   # Starts gRPC + HTTP servers
│   └── internal/
│       ├── handler/
│       │   ├── order_handler.go      # gRPC handler – CreateOrder, GetOrder
│       │   └── http_handler.go       # REST gateway – POST/GET /orders
│       └── repository/order_repo.go  # PostgreSQL queries
├── payment-service/
│   ├── cmd/main.go
│   └── internal/
│       ├── handler/                  # gRPC handler – ProcessPayment
│       ├── messaging/publisher.go    # RabbitMQ publisher + topology declaration
│       └── repository/               # PostgreSQL queries
└── notification-service/
    ├── cmd/main.go
    └── internal/
        ├── consumer/
        │   └── notification_consumer.go  # RabbitMQ consumer + ACK logic
        └── repository/                   # Idempotency store (processed_events)
```

---

## How to Run

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running

### Start all services

```bash
docker compose up --build -d
```

### Verify all containers are healthy

```bash
docker ps
```

Expected output – all five containers should show `Up`:

```
NAMES                  STATUS           PORTS
order-service          Up X seconds     0.0.0.0:8080->8080/tcp, 0.0.0.0:50051->50051/tcp
payment-service        Up X seconds     0.0.0.0:50052->50052/tcp
notification-service   Up X seconds
postgres               Up X seconds (healthy)   0.0.0.0:5432->5432/tcp
rabbitmq               Up X seconds (healthy)   0.0.0.0:5672->5672/tcp, 0.0.0.0:15672->15672/tcp
```

### Stop all services

```bash
docker compose down
```

### RabbitMQ Management UI

Open in browser: **http://localhost:15672**  
Credentials: `guest` / `guest`

---

## Idempotency Strategy

**Problem:** RabbitMQ guarantees *at-least-once* delivery. If the Notification Service crashes after processing a message but before sending the ACK, RabbitMQ redelivers the same message. Without a guard, the same notification would be logged (or sent) twice.

**Solution – Database-backed deduplication via `processed_events` table:**

```sql
-- Created by init.sql
CREATE TABLE IF NOT EXISTS processed_events (
    event_id     VARCHAR(255) PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT NOW()
);
```

Processing flow for every incoming message:

1. Every `PaymentEvent` published by the Payment Service includes a unique `event_id` (UUID v4).
2. Notification Service queries the table before doing any work:
   ```sql
   SELECT event_id FROM processed_events WHERE event_id = $1
   ```
3. **Duplicate found** → silently `ACK` the message and skip — no side effects.
4. **Not a duplicate** → execute business logic (log email) → insert `event_id` into `processed_events` → `ACK`.

This guarantees **exactly-once effect** even under at-least-once delivery semantics.

---

## ACK Logic

Manual acknowledgment is enabled by setting `autoAck = false` when the consumer registers:

```go
// notification-service/internal/consumer/notification_consumer.go
deliveries, err := c.channel.Consume(
    QueueName,
    "notification-consumer", // consumer tag
    false,                   // autoAck = false  ← Manual ACK
    false, false, false, nil,
)
```

The `ACK` is sent **only after all of the following succeed**:

| Step | Action |
|---|---|
| 1 | Message body parsed successfully (JSON → `PaymentEvent`) |
| 2 | Idempotency check passes (no duplicate in `processed_events`) |
| 3 | Business logic executed (email log printed) |
| 4 | `event_id` inserted into `processed_events` |
| 5 | `d.Ack(false)` called |

If the service crashes at **any step before step 5**, RabbitMQ redelivers the message — preserving the at-least-once guarantee without data loss.

**NACK behaviour:**

| Scenario | Action | Result |
|---|---|---|
| JSON parse error (unrecoverable) | `Nack(requeue=false)` | Message routed to DLQ |
| DB / idempotency error (transient) | `Nack(requeue=true)` | Message requeued for retry |

---

## Reliability Features

| Feature | Implementation |
|---|---|
| **Durable queues** | `QueueDeclare(..., durable=true, ...)` – messages survive broker restart |
| **Persistent messages** | `DeliveryMode: amqp.Persistent` set in publisher |
| **Manual ACKs** | `autoAck=false`; ACK sent only after successful end-to-end processing |
| **Idempotency** | `processed_events` table; duplicate `event_id` → silent skip |
| **Prefetch limit** | `ch.Qos(1, 0, false)` – processes one message at a time (fair dispatch) |
| **Graceful Shutdown** | `os/signal` in all services; gRPC `GracefulStop()` on SIGINT/SIGTERM |
| **Retry on startup** | All services retry connecting to Postgres/RabbitMQ up to 10 times (3 s apart) |
| **Dead Letter Queue** | Messages nacked with `requeue=false` are automatically moved to `payment.dead_letter` |

---

## Dead Letter Queue (Bonus)

The main queue `payment.completed` is declared with a Dead-Letter Exchange (DLX):

```go
// payment-service/internal/messaging/publisher.go
args := amqp.Table{
    "x-dead-letter-exchange":    "payments.dlx",
    "x-dead-letter-routing-key": "payment.completed",
}
ch.QueueDeclare("payment.completed", true, false, false, false, args)
```

The DLX (`payments.dlx`) routes failed messages to the DLQ (`payment.dead_letter`):

```go
ch.ExchangeDeclare("payments.dlx", "direct", true, false, false, false, nil)
ch.QueueDeclare("payment.dead_letter", true, false, false, false, nil)
ch.QueueBind("payment.dead_letter", "payment.completed", "payments.dlx", false, nil)
```

**When does a message go to the DLQ?**
- The consumer calls `Nack(false, false)` — for example on an unrecoverable JSON parse error.
- RabbitMQ automatically routes it via DLX to `payment.dead_letter`.

**Monitoring:** Open **http://localhost:15672 → Queues** to see message counts in `payment.completed` and `payment.dead_letter` in real time.

---

## Event Payload (JSON)

The `PaymentEvent` struct is serialized to JSON and published to the `payments` exchange:

```json
{
  "event_id":       "550e8400-e29b-41d4-a716-446655440000",
  "order_id":       1,
  "amount":         99.99,
  "customer_email": "user@example.com",
  "status":         "completed",
  "occurred_at":    "2026-05-02T10:00:00Z"
}
```

| Field | Type | Description |
|---|---|---|
| `event_id` | `string` (UUID v4) | Unique message ID used for idempotency deduplication |
| `order_id` | `int32` | References the `orders` table |
| `amount` | `float64` | Payment amount |
| `customer_email` | `string` | Recipient email address |
| `status` | `string` | Always `"completed"` for successfully processed payments |
| `occurred_at` | `string` (RFC3339) | Timestamp of the payment event |

---

## Expected Console Output

After sending a `POST /orders` request, all three services produce logs showing the full event-driven chain:

**Order Service:**
```
[OrderService] Connected to PostgreSQL
[OrderService] HTTP REST server listening on :8080
[OrderService] gRPC server listening on :50051
[OrderService] CreateOrder: email=user@example.com amount=99.99
[OrderService] Order #1 created in DB
[OrderService] Order #1 status updated to 'completed'
```

**Payment Service:**
```
[PaymentService] Connected to PostgreSQL
[Publisher] RabbitMQ topology declared (queue, DLX, DLQ)
[PaymentService] gRPC server listening on :50052
[PaymentService] ProcessPayment: order_id=1 amount=99.99 email=user@example.com
[PaymentService] Payment for order #1 committed to DB
[Publisher] Event published: event_id=c5576dbc-ac2a-4e0b-937c-10805e0037a8 order_id=1
```

**Notification Service:**
```
[NotificationService] Connected to PostgreSQL (idempotency store)
[Consumer] Connected to RabbitMQ, queue ready
[NotificationService] Starting consumer...
[Consumer] Waiting for messages on queue: payment.completed
[Notification] Sent email to user@example.com for Order #1. Amount: $99.99
```

To view logs in real time:

```bash
docker logs order-service --follow
docker logs payment-service --follow
docker logs notification-service --follow
```
