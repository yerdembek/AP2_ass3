package messaging

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ExchangeName = "payments"
	QueueName    = "payment.completed"
	RoutingKey   = "payment.completed"
	DLXName      = "payments.dlx"
	DLQName      = "payment.dead_letter"
)

// PaymentEvent is the message published to RabbitMQ after a successful payment.
// It is serialized as JSON.
type PaymentEvent struct {
	EventID       string  `json:"event_id"`       // Unique ID for idempotency
	OrderID       int32   `json:"order_id"`
	Amount        float64 `json:"amount"`
	CustomerEmail string  `json:"customer_email"`
	Status        string  `json:"status"`
	OccurredAt    string  `json:"occurred_at"`
}

// Publisher wraps the RabbitMQ connection and provides a Publish method.
// The messaging logic is hidden behind this interface (Infrastructure layer).
type Publisher struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewPublisher(amqpURL string) (*Publisher, error) {
	var conn *amqp.Connection
	var err error

	// Retry loop – RabbitMQ may not be ready immediately
	for i := 0; i < 10; i++ {
		conn, err = amqp.Dial(amqpURL)
		if err == nil {
			break
		}
		log.Printf("[Publisher] Waiting for RabbitMQ... attempt %d/10: %v", i+1, err)
		time.Sleep(3 * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("connect to RabbitMQ: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}

	// ── Declare Dead-Letter Exchange (DLX) ──────────────────────────────────
	if err = ch.ExchangeDeclare(DLXName, "direct", true, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("declare DLX: %w", err)
	}

	// ── Declare Dead-Letter Queue (DLQ) ─────────────────────────────────────
	if _, err = ch.QueueDeclare(DLQName, true, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("declare DLQ: %w", err)
	}
	if err = ch.QueueBind(DLQName, RoutingKey, DLXName, false, nil); err != nil {
		return nil, fmt.Errorf("bind DLQ: %w", err)
	}

	// ── Declare Main Exchange ────────────────────────────────────────────────
	if err = ch.ExchangeDeclare(ExchangeName, "direct", true, false, false, false, nil); err != nil {
		return nil, fmt.Errorf("declare exchange: %w", err)
	}

	// ── Declare Durable Queue with DLX ─────────────────────────────────────
	// x-dead-letter-exchange: messages go to DLX when nacked with requeue=false
	// Note: x-delivery-limit is only supported by Quorum queues, not Classic queues
	args := amqp.Table{
		"x-dead-letter-exchange":    DLXName,
		"x-dead-letter-routing-key": RoutingKey,
	}
	if _, err = ch.QueueDeclare(QueueName, true, false, false, false, args); err != nil {
		return nil, fmt.Errorf("declare queue: %w", err)
	}
	if err = ch.QueueBind(QueueName, RoutingKey, ExchangeName, false, nil); err != nil {
		return nil, fmt.Errorf("bind queue: %w", err)
	}

	log.Println("[Publisher] RabbitMQ topology declared (queue, DLX, DLQ)")
	return &Publisher{conn: conn, channel: ch}, nil
}

// Publish serializes a PaymentEvent and sends it to the broker.
// Uses Mandatory=true so the broker returns an error if the queue is not bound.
func (p *Publisher) Publish(ctx context.Context, event PaymentEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	err = p.channel.PublishWithContext(ctx,
		ExchangeName, // exchange
		RoutingKey,   // routing key
		true,         // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent, // survive broker restart
			MessageId:    event.EventID,
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	log.Printf("[Publisher] Event published: event_id=%s order_id=%d", event.EventID, event.OrderID)
	return nil
}

func (p *Publisher) Close() {
	if p.channel != nil {
		_ = p.channel.Close()
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}
	log.Println("[Publisher] RabbitMQ connection closed")
}
