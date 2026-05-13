# AP2 Assignment 3 – Event-Driven Architecture with Message Queues

**Student:** Adildabek Nurassyl  
**Group:** SE-2408  
**Course:** Advanced Programming 2

---

## Architecture Overview

```
[Client]
   │  HTTP/gRPC
   ▼
[Order Service :50051]  ──gRPC──►  [Payment Service :50052]
                                           │
                                     DB Transaction
                                           │
                                    Publish Event
                                           │
                                    [RabbitMQ]
                                    Exchange: payments
                                    Queue: payment.completed
                                    DLX: payments.dlx
                                    DLQ: payment.dead_letter
                                           │
                                           ▼
                               [Notification Service]
                               (Consumer – fully decoupled)
                                           │
                                    Check Idempotency
                                    (processed_events table)
                                           │
                                    Log email simulation
                                           │
                                    Manual ACK → RabbitMQ
```

---

## Services

| Service | Port | Role |
|---|---|---|
| Order Service | :50051 | gRPC server; receives orders, calls Payment |
| Payment Service | :50052 | gRPC server; commits payment to DB, publishes event |
| Notification Service | — | RabbitMQ consumer; decoupled email simulator |
| PostgreSQL | :5432 | Shared database |
| RabbitMQ | :5672 / :15672 | Message broker |

---

## How to Run

```bash
docker-compose up --build
```

RabbitMQ Management UI: http://localhost:15672 (guest / guest)

---

## Idempotency Strategy

**Problem:** RabbitMQ guarantees *at-least-once* delivery. If the Notification Service crashes after processing but before ACKing, RabbitMQ redelivers the message. Without a guard, the same email would be logged twice.

**Solution – Database-backed deduplication:**

1. Every `PaymentEvent` published by the Payment Service includes a unique `event_id` (UUID v4).
2. Before processing, the Notification Service queries the `processed_events` table:
   ```sql
   SELECT event_id FROM processed_events WHERE event_id = $1
   ```
3. If found → duplicate detected → message is silently **ACKed** and skipped.
4. If not found → message is processed → `event_id` is inserted into `processed_events` → message is **ACKed**.

This guarantees **exactly-once** *effect* even under at-least-once delivery.

---

## ACK Logic

Manual acknowledgment is enabled by setting `autoAck = false` in the consumer:

```go
ch.Consume(QueueName, "notification-consumer", false /* autoAck */, ...)
```

The ACK is sent **only after**:
1. The message is successfully parsed.
2. The idempotency check passes.
3. The notification log is printed.
4. The `event_id` is recorded in the DB.

If the service crashes at any step before the ACK, RabbitMQ redelivers the message to the next available consumer (at-least-once guarantee).

**NACK behaviour:**
- Parse error → `Nack(requeue=false)` → message moves to DLQ.
- DB/idempotency error → `Nack(requeue=true)` → message is retried up to 3 times before moving to DLQ.

---

## Reliability Features

| Feature | Implementation |
|---|---|
| **Durable queues** | `QueueDeclare(..., durable=true, ...)` – messages survive broker restart |
| **Persistent messages** | `DeliveryMode: amqp.Persistent` in publisher |
| **Manual ACKs** | `autoAck=false`; ACK sent only after successful processing |
| **Idempotency** | `processed_events` table; duplicate `event_id` → skip |
| **Graceful Shutdown** | `os/signal` in all services; gRPC `GracefulStop()` |
| **DLQ (Bonus)** | Messages exceeding 3 delivery attempts moved to `payment.dead_letter` |

---

## Dead Letter Queue (Bonus)

The main queue is declared with:
```go
"x-dead-letter-exchange":    "payments.dlx"
"x-dead-letter-routing-key": "payment.completed"
"x-delivery-limit":          3
```

To simulate a DLQ failure, temporarily modify `handleDelivery` in the consumer to call `Nack(requeue=false)` for a specific `order_id`. After 3 delivery attempts, RabbitMQ moves the message to `payment.dead_letter` automatically.

Monitor in the RabbitMQ UI at http://localhost:15672 → Queues.

---

## Event Payload (JSON)

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "order_id": 1,
  "amount": 99.99,
  "customer_email": "user@example.com",
  "status": "completed",
  "occurred_at": "2026-05-02T10:00:00Z"
}
```

---

## Expected Console Output

**Notification Service log:**
```
[Notification] Sent email to user@example.com for Order #1. Amount: $99.99
```
