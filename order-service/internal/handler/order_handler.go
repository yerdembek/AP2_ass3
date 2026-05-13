package handler

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"order-service/internal/repository"
	pb "order-service/proto"
)

type OrderHandler struct {
	pb.UnimplementedOrderServiceServer
	repo          *repository.OrderRepository
	paymentClient pb.PaymentServiceClient
}

func NewOrderHandler(repo *repository.OrderRepository, paymentAddr string) (*OrderHandler, error) {
	var conn *grpc.ClientConn
	var err error

	// Retry connecting to payment service (it might not be ready yet)
	for i := 0; i < 10; i++ {
		conn, err = grpc.Dial(paymentAddr, //nolint:staticcheck
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err == nil {
			break
		}
		log.Printf("[OrderService] Waiting for payment service... attempt %d/10", i+1)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("connect to payment service: %w", err)
	}

	return &OrderHandler{
		repo:          repo,
		paymentClient: pb.NewPaymentServiceClient(conn),
	}, nil
}

func (h *OrderHandler) CreateOrder(ctx context.Context, req *pb.CreateOrderRequest) (*pb.CreateOrderResponse, error) {
	log.Printf("[OrderService] CreateOrder: email=%s amount=%.2f", req.CustomerEmail, req.Amount)

	// 1. Persist order
	orderID, err := h.repo.CreateOrder(req.CustomerEmail, req.Amount)
	if err != nil {
		return nil, fmt.Errorf("create order in db: %w", err)
	}
	log.Printf("[OrderService] Order #%d created in DB", orderID)

	// 2. Call Payment Service synchronously (Assignment 2 pattern preserved)
	payResp, err := h.paymentClient.ProcessPayment(ctx, &pb.ProcessPaymentRequest{
		OrderId:       orderID,
		Amount:        req.Amount,
		CustomerEmail: req.CustomerEmail,
	})
	if err != nil {
		_ = h.repo.UpdateOrderStatus(orderID, "payment_failed")
		return nil, fmt.Errorf("payment service error: %w", err)
	}

	// 3. Update order status based on payment result
	_ = h.repo.UpdateOrderStatus(orderID, payResp.Status)
	log.Printf("[OrderService] Order #%d status updated to '%s'", orderID, payResp.Status)

	return &pb.CreateOrderResponse{
		OrderId: orderID,
		Status:  payResp.Status,
	}, nil
}

func (h *OrderHandler) GetOrder(ctx context.Context, req *pb.GetOrderRequest) (*pb.GetOrderResponse, error) {
	order, err := h.repo.GetOrder(req.OrderId)
	if err != nil {
		return nil, err
	}
	return &pb.GetOrderResponse{
		OrderId:       order.ID,
		CustomerEmail: order.CustomerEmail,
		Amount:        order.Amount,
		Status:        order.Status,
	}, nil
}
