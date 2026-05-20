package handler

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"order-service/internal/cache"
	"order-service/internal/repository"
	pb "order-service/proto"
)

// OrderHandler implements the gRPC OrderService and coordinates between
// the database repository and the Redis cache (Cache-aside pattern).
type OrderHandler struct {
	pb.UnimplementedOrderServiceServer
	repo          *repository.OrderRepository
	paymentClient pb.PaymentServiceClient
	cache         *cache.OrderCache // nil-safe: caching is best-effort
}

func NewOrderHandler(repo *repository.OrderRepository, paymentAddr string, c *cache.OrderCache) (*OrderHandler, error) {
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
		cache:         c,
	}, nil
}

// CreateOrder creates a new order, processes the payment, and atomically
// invalidates the cache entry after the status is updated.
func (h *OrderHandler) CreateOrder(ctx context.Context, req *pb.CreateOrderRequest) (*pb.CreateOrderResponse, error) {
	log.Printf("[OrderService] CreateOrder: email=%s amount=%.2f", req.CustomerEmail, req.Amount)

	// 1. Persist order
	orderID, err := h.repo.CreateOrder(req.CustomerEmail, req.Amount)
	if err != nil {
		return nil, fmt.Errorf("create order in db: %w", err)
	}
	log.Printf("[OrderService] Order #%d created in DB", orderID)

	// 2. Call Payment Service synchronously
	payResp, err := h.paymentClient.ProcessPayment(ctx, &pb.ProcessPaymentRequest{
		OrderId:       orderID,
		Amount:        req.Amount,
		CustomerEmail: req.CustomerEmail,
	})
	if err != nil {
		_ = h.repo.UpdateOrderStatus(orderID, "payment_failed")
		// Invalidate cache if an entry somehow exists (defensive)
		h.invalidateCache(ctx, orderID)
		return nil, fmt.Errorf("payment service error: %w", err)
	}

	// 3. Update order status in DB
	_ = h.repo.UpdateOrderStatus(orderID, payResp.Status)
	log.Printf("[OrderService] Order #%d status updated to '%s'", orderID, payResp.Status)

	// 4. ── Cache Invalidation ────────────────────────────────────────────────
	// Delete the cache key so the next GET reads fresh data from DB.
	// This prevents serving stale "pending" status after a successful payment.
	h.invalidateCache(ctx, orderID)

	return &pb.CreateOrderResponse{
		OrderId: orderID,
		Status:  payResp.Status,
	}, nil
}

// GetOrder implements the Cache-aside read pattern:
//  1. Check Redis cache
//  2. On HIT: return cached order (fast path, no DB query)
//  3. On MISS: query DB, populate cache, return order
func (h *OrderHandler) GetOrder(ctx context.Context, req *pb.GetOrderRequest) (*pb.GetOrderResponse, error) {
	// ── Step 1: Cache Lookup ──────────────────────────────────────────────────
	if h.cache != nil {
		cached, err := h.cache.Get(ctx, req.OrderId)
		if err != nil {
			// Cache error is non-fatal — log and fall through to DB
			log.Printf("[OrderService] Cache GET error for order #%d: %v", req.OrderId, err)
		} else if cached != nil {
			// ── Cache HIT ────────────────────────────────────────────────────
			log.Printf("[OrderService] Cache HIT for order #%d", req.OrderId)
			return &pb.GetOrderResponse{
				OrderId:       cached.ID,
				CustomerEmail: cached.CustomerEmail,
				Amount:        cached.Amount,
				Status:        cached.Status,
			}, nil
		}
	}

	// ── Step 2: Cache MISS → query DB ────────────────────────────────────────
	log.Printf("[OrderService] Cache MISS for order #%d — querying DB", req.OrderId)
	order, err := h.repo.GetOrder(req.OrderId)
	if err != nil {
		return nil, err
	}

	// ── Step 3: Populate cache (best-effort, non-blocking) ───────────────────
	if h.cache != nil {
		go func() {
			cacheOrder := &cache.Order{
				ID:            order.ID,
				CustomerEmail: order.CustomerEmail,
				Amount:        order.Amount,
				Status:        order.Status,
			}
			if setErr := h.cache.Set(context.Background(), cacheOrder); setErr != nil {
				log.Printf("[OrderService] Cache SET error for order #%d: %v", order.ID, setErr)
			}
		}()
	}

	return &pb.GetOrderResponse{
		OrderId:       order.ID,
		CustomerEmail: order.CustomerEmail,
		Amount:        order.Amount,
		Status:        order.Status,
	}, nil
}

// invalidateCache deletes the cache entry for an order.
// Errors are logged but not propagated — cache invalidation failure
// is handled at the TTL boundary.
func (h *OrderHandler) invalidateCache(ctx context.Context, orderID int32) {
	if h.cache == nil {
		return
	}
	if err := h.cache.Delete(ctx, orderID); err != nil {
		log.Printf("[OrderService] Cache invalidation error for order #%d: %v", orderID, err)
	}
}
