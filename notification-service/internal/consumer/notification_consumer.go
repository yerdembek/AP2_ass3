// Package consumer implements a parallel notification worker pool.
//
// Architecture ("barista" analogy from the assignment):
//   - Each goroutine is an independent "barista" that picks up a job from the queue
//   - Baristas work in parallel (concurrent message processing)
//   - Each barista retries on failure using exponential backoff (2s, 4s, 8s, ...)
//   - Before serving, a barista checks the idempotency store so the same order
//     is never fulfilled twice, even if two baristas grab the same ticket
package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"notification-service/internal/provider"
	"notification-service/internal/repository"
)

const (
	QueueName = "payment.completed"
)

// PaymentEvent mirrors the struct published by the Payment Service.
type PaymentEvent struct {
	EventID       string  `json:"event_id"`
	OrderID       int32   `json:"order_id"`
	Amount        float64 `json:"amount"`
	CustomerEmail string  `json:"customer_email"`
	Status        string  `json:"status"`
	OccurredAt    string  `json:"occurred_at"`
}

// NotificationConsumer is a background worker pool.
// It spins up numWorkers goroutines, each reading from the RabbitMQ queue
// in parallel — like multiple baristas serving customers simultaneously.
type NotificationConsumer struct {
	conn       *amqp.Connection
	channel    *amqp.Channel
	redisStore *repository.RedisIdempotencyStore // primary: Redis-based idempotency
	pgStore    *repository.IdempotencyStore      // fallback: PostgreSQL-based idempotency
	sender     provider.EmailSender              // injected adapter (Simulated or SMTP)
	numWorkers int
	maxRetries int
	baseDelay  time.Duration
}

// NewNotificationConsumer creates the consumer and connects to RabbitMQ.
func NewNotificationConsumer(
	amqpURL string,
	redisStore *repository.RedisIdempotencyStore,
	pgStore *repository.IdempotencyStore,
	sender provider.EmailSender,
	numWorkers, maxRetries int,
	baseDelay time.Duration,
) (*NotificationConsumer, error) {
	var conn *amqp.Connection
	var err error

	for i := 0; i < 10; i++ {
		conn, err = amqp.Dial(amqpURL)
		if err == nil {
			break
		}
		log.Printf("[Consumer] Waiting for RabbitMQ... attempt %d/10: %v", i+1, err)
		time.Sleep(3 * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}

	// Prefetch = numWorkers so each worker has a message ready without starving others
	if err = ch.Qos(numWorkers, 0, false); err != nil {
		return nil, fmt.Errorf("set QoS: %w", err)
	}

	// Declare queue (idempotent)
	dlxArgs := amqp.Table{
		"x-dead-letter-exchange":    "payments.dlx",
		"x-dead-letter-routing-key": "payment.completed",
	}
	if _, err = ch.QueueDeclare(QueueName, true, false, false, false, dlxArgs); err != nil {
		return nil, fmt.Errorf("declare queue: %w", err)
	}

	log.Printf("[Consumer] Connected to RabbitMQ | workers=%d maxRetries=%d baseDelay=%s",
		numWorkers, maxRetries, baseDelay)

	return &NotificationConsumer{
		conn:       conn,
		channel:    ch,
		redisStore: redisStore,
		pgStore:    pgStore,
		sender:     sender,
		numWorkers: numWorkers,
		maxRetries: maxRetries,
		baseDelay:  baseDelay,
	}, nil
}

// Consume starts the worker pool and blocks until done is closed.
// Each worker is an independent goroutine reading from the shared deliveries channel —
// like multiple baristas taking tickets from the same order counter.
func (c *NotificationConsumer) Consume(done <-chan struct{}) error {
	deliveries, err := c.channel.Consume(
		QueueName,
		"notification-consumer",
		false, // autoAck = false (manual ACK after processing)
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("start consume: %w", err)
	}

	log.Printf("[Consumer] Starting %d parallel workers on queue: %s", c.numWorkers, QueueName)

	var wg sync.WaitGroup

	// ── Spin up worker goroutines ─────────────────────────────────────────────
	// Each goroutine is a "barista" that independently picks up jobs.
	for i := 0; i < c.numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			log.Printf("[Worker-%d] Started — ready to process notifications", workerID)

			for {
				select {
				case <-done:
					log.Printf("[Worker-%d] Shutting down", workerID)
					return
				case d, ok := <-deliveries:
					if !ok {
						log.Printf("[Worker-%d] Deliveries channel closed", workerID)
						return
					}
					c.handleDelivery(workerID, d)
				}
			}
		}(i + 1)
	}

	// Wait for shutdown signal, then wait for all workers to finish
	<-done
	log.Println("[Consumer] Shutdown signal received — draining workers...")
	wg.Wait()
	log.Println("[Consumer] All workers stopped")
	return nil
}

// handleDelivery processes a single message:
//  1. Parse event
//  2. Check Redis idempotency (skip if already processed)
//  3. Call sendWithRetry (exponential backoff on failure)
//  4. Mark as processed in Redis (SET NX)
//  5. ACK or NACK
func (c *NotificationConsumer) handleDelivery(workerID int, d amqp.Delivery) {
	prefix := fmt.Sprintf("[Worker-%d]", workerID)

	// ── 1. Parse ──────────────────────────────────────────────────────────────
	var event PaymentEvent
	if err := json.Unmarshal(d.Body, &event); err != nil {
		log.Printf("%s ERROR: cannot parse message: %v — sending to DLQ", prefix, err)
		_ = d.Nack(false, false) // unrecoverable → DLQ
		return
	}

	log.Printf("%s Processing event_id=%s order_id=%d email=%s",
		prefix, event.EventID, event.OrderID, event.CustomerEmail)

	ctx := context.Background()

	// ── 2. Idempotency Check (Redis primary) ──────────────────────────────────
	isDup, err := c.redisStore.IsDuplicate(ctx, event.EventID)
	if err != nil {
		log.Printf("%s Redis idempotency check failed: %v — requeuing", prefix, err)
		_ = d.Nack(false, true) // temporary error → requeue
		return
	}
	if isDup {
		log.Printf("%s DUPLICATE detected (Redis), skipping event_id=%s", prefix, event.EventID)
		_ = d.Ack(false)
		return
	}

	// ── 3. Send with Exponential Backoff Retry ────────────────────────────────
	subject := fmt.Sprintf("Payment Confirmation – Order #%d", event.OrderID)
	body := fmt.Sprintf(
		"Dear customer,\n\nYour payment of $%.2f for Order #%d has been %s.\n\nThank you!",
		event.Amount, event.OrderID, event.Status,
	)

	if err = c.sendWithRetry(prefix, event.CustomerEmail, subject, body); err != nil {
		// All retries exhausted → send to DLQ
		log.Printf("%s All retries exhausted for event_id=%s — sending to DLQ", prefix, event.EventID)
		_ = d.Nack(false, false)
		return
	}

	// ── 4. Mark as processed in Redis (SET NX — atomic) ──────────────────────
	if err = c.redisStore.MarkProcessed(ctx, event.EventID); err != nil {
		log.Printf("%s ERROR marking event_id=%s in Redis: %v", prefix, event.EventID, err)
		// Non-fatal: idempotency TTL will eventually expire.
		// We still ACK to avoid infinite redelivery.
	}

	// Also mark in PostgreSQL for backward compatibility
	if c.pgStore != nil {
		if err = c.pgStore.MarkProcessed(event.EventID); err != nil {
			log.Printf("%s WARNING: PostgreSQL idempotency mark failed: %v", prefix, err)
		}
	}

	// ── 5. ACK ────────────────────────────────────────────────────────────────
	if err = d.Ack(false); err != nil {
		log.Printf("%s ERROR sending ACK for event_id=%s: %v", prefix, event.EventID, err)
	}
	log.Printf("%s ✓ Notification delivered for event_id=%s", prefix, event.EventID)
}

// sendWithRetry calls the EmailSender with exponential backoff.
//
// Backoff schedule (baseDelay=2s):
//
//	attempt 1 → fail → wait 2s
//	attempt 2 → fail → wait 4s
//	attempt 3 → fail → wait 8s
//	attempt maxRetries → fail → return error
//
// This is the "barista keeps retrying when the coffee machine malfunctions"
// pattern — smart retries without hammering the provider.
func (c *NotificationConsumer) sendWithRetry(prefix, to, subject, body string) error {
	for attempt := 1; attempt <= c.maxRetries; attempt++ {
		log.Printf("%s Attempt %d/%d: sending email to %s", prefix, attempt, c.maxRetries, to)

		err := c.sender.Send(to, subject, body)
		if err == nil {
			return nil // success
		}

		log.Printf("%s Attempt %d failed: %v", prefix, attempt, err)

		if attempt < c.maxRetries {
			// Exponential backoff: baseDelay * 2^(attempt-1)
			backoff := time.Duration(float64(c.baseDelay) * math.Pow(2, float64(attempt-1)))
			log.Printf("%s Retrying in %s...", prefix, backoff)
			time.Sleep(backoff)
		}
	}
	return fmt.Errorf("all %d attempts failed", c.maxRetries)
}

// Close cleans up RabbitMQ connections.
func (c *NotificationConsumer) Close() {
	if c.channel != nil {
		_ = c.channel.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
	log.Println("[Consumer] RabbitMQ connection closed")
}
