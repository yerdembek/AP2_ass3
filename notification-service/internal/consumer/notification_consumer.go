package consumer

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

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

// NotificationConsumer listens on the payment.completed queue.
// It is fully decoupled – it does NOT import or depend on Order or Payment services.
type NotificationConsumer struct {
	conn    *amqp.Connection
	channel *amqp.Channel
	store   *repository.IdempotencyStore
}

func NewNotificationConsumer(amqpURL string, store *repository.IdempotencyStore) (*NotificationConsumer, error) {
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

	// Set prefetch = 1 so we process one message at a time (fair dispatch)
	if err = ch.Qos(1, 0, false); err != nil {
		return nil, fmt.Errorf("set QoS: %w", err)
	}

	// The queue must already exist (declared by the producer).
	// We declare it here too (idempotent) so the consumer can start independently.
	// Note: x-delivery-limit is only supported by Quorum queues, not Classic queues
	dlxArgs := amqp.Table{
		"x-dead-letter-exchange":    "payments.dlx",
		"x-dead-letter-routing-key": "payment.completed",
	}
	if _, err = ch.QueueDeclare(QueueName, true, false, false, false, dlxArgs); err != nil {
		return nil, fmt.Errorf("declare queue: %w", err)
	}

	log.Println("[Consumer] Connected to RabbitMQ, queue ready")
	return &NotificationConsumer{conn: conn, channel: ch, store: store}, nil
}

// Consume starts the blocking consume loop. It returns only when done channel is closed.
func (c *NotificationConsumer) Consume(done <-chan struct{}) error {
	// ── Manual ACK: autoAck = false ──────────────────────────────────────────
	deliveries, err := c.channel.Consume(
		QueueName,
		"notification-consumer", // consumer tag
		false,                   // autoAck = false  ← Manual ACK
		false,                   // exclusive
		false,                   // noLocal
		false,                   // noWait
		nil,
	)
	if err != nil {
		return fmt.Errorf("start consume: %w", err)
	}

	log.Println("[Consumer] Waiting for messages on queue:", QueueName)

	for {
		select {
		case <-done:
			log.Println("[Consumer] Stopping consume loop")
			return nil
		case d, ok := <-deliveries:
			if !ok {
				return fmt.Errorf("deliveries channel closed")
			}
			c.handleDelivery(d)
		}
	}
}

func (c *NotificationConsumer) handleDelivery(d amqp.Delivery) {
	// ── Parse message ────────────────────────────────────────────────────────
	var event PaymentEvent
	if err := json.Unmarshal(d.Body, &event); err != nil {
		log.Printf("[Consumer] ERROR: cannot parse message: %v – sending to DLQ", err)
		// Unrecoverable: nack without requeue → goes to DLX/DLQ
		_ = d.Nack(false, false)
		return
	}

	// ── Idempotency check ────────────────────────────────────────────────────
	// If we have already processed this event_id, just ACK and skip.
	isDup, err := c.store.IsDuplicate(event.EventID)
	if err != nil {
		log.Printf("[Consumer] ERROR checking idempotency for event_id=%s: %v", event.EventID, err)
		// Requeue so we can retry later
		_ = d.Nack(false, true)
		return
	}
	if isDup {
		log.Printf("[Consumer] DUPLICATE detected, skipping event_id=%s order_id=%d", event.EventID, event.OrderID)
		_ = d.Ack(false) // ACK to remove from queue
		return
	}

	// ── Business logic: simulate sending email ───────────────────────────────
	// This is the core notification logic required by the assignment.
	log.Printf("[Notification] Sent email to %s for Order #%d. Amount: $%.2f",
		event.CustomerEmail, event.OrderID, event.Amount)

	// ── Mark as processed (idempotency) ─────────────────────────────────────
	if err = c.store.MarkProcessed(event.EventID); err != nil {
		log.Printf("[Consumer] ERROR marking event_id=%s as processed: %v", event.EventID, err)
		_ = d.Nack(false, true)
		return
	}

	// ── Manual ACK: only after successful processing ─────────────────────────
	// If the service crashes BEFORE this line, the message is redelivered.
	// At-least-once delivery guarantee.
	if err = d.Ack(false); err != nil {
		log.Printf("[Consumer] ERROR sending ACK for event_id=%s: %v", event.EventID, err)
	}
}

func (c *NotificationConsumer) Close() {
	if c.channel != nil {
		_ = c.channel.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
	log.Println("[Consumer] RabbitMQ connection closed")
}
