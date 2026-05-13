package handler

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"

	"payment-service/internal/messaging"
	"payment-service/internal/repository"
	pb "payment-service/proto"
)

type PaymentHandler struct {
	pb.UnimplementedPaymentServiceServer
	repo      *repository.PaymentRepository
	publisher *messaging.Publisher
}

func NewPaymentHandler(repo *repository.PaymentRepository, pub *messaging.Publisher) *PaymentHandler {
	return &PaymentHandler{repo: repo, publisher: pub}
}

func (h *PaymentHandler) ProcessPayment(ctx context.Context, req *pb.ProcessPaymentRequest) (*pb.ProcessPaymentResponse, error) {
	log.Printf("[PaymentService] ProcessPayment: order_id=%d amount=%.2f email=%s",
		req.OrderId, req.Amount, req.CustomerEmail)

	// 1. Persist payment record (DB transaction inside CreatePayment)
	_, err := h.repo.CreatePayment(req.OrderId, req.Amount)
	if err != nil {
		return nil, fmt.Errorf("persist payment: %w", err)
	}
	log.Printf("[PaymentService] Payment for order #%d committed to DB", req.OrderId)

	// 2. Publish event to RabbitMQ AFTER the DB transaction commits.
	//    At-least-once: if publish fails here, the caller can retry.
	event := messaging.PaymentEvent{
		EventID:       uuid.New().String(), // unique ID for idempotency on consumer side
		OrderID:       req.OrderId,
		Amount:        req.Amount,
		CustomerEmail: req.CustomerEmail,
		Status:        "completed",
		OccurredAt:    time.Now().UTC().Format(time.RFC3339),
	}
	if err = h.publisher.Publish(ctx, event); err != nil {
		// Log and continue – payment is already committed.
		// In production you'd use an outbox pattern here.
		log.Printf("[PaymentService] WARNING: failed to publish event for order #%d: %v", req.OrderId, err)
	}

	return &pb.ProcessPaymentResponse{
		Status:  "completed",
		Message: fmt.Sprintf("Payment for order #%d processed successfully", req.OrderId),
	}, nil
}
