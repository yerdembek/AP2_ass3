package main

import (
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"order-service/internal/cache"
	"order-service/internal/handler"
	"order-service/internal/middleware"
	"order-service/internal/repository"
	pb "order-service/proto"
)

func main() {
	// ─── Configuration ────────────────────────────────────────────────────────
	dbURL := getEnv("DB_URL", "postgres://ap2user:ap2pass@localhost:5432/ap2db?sslmode=disable")
	paymentAddr := getEnv("PAYMENT_ADDR", "localhost:50052")
	redisURL := getEnv("REDIS_URL", "redis://localhost:6379/0")

	cacheTTLSecs, _ := strconv.Atoi(getEnv("CACHE_TTL_SECONDS", "300"))
	rateLimitReqs, _ := strconv.Atoi(getEnv("RATE_LIMIT_REQUESTS", "10"))
	rateLimitWindow, _ := strconv.Atoi(getEnv("RATE_LIMIT_WINDOW_SECONDS", "60"))

	grpcAddr := ":50051"
	httpAddr := ":8080"

	// ─── Repository ──────────────────────────────────────────────────────────
	repo, err := repository.NewOrderRepository(dbURL)
	if err != nil {
		log.Fatalf("[OrderService] DB connection failed: %v", err)
	}
	defer repo.Close()
	log.Println("[OrderService] Connected to PostgreSQL")

	// ─── Redis Cache ──────────────────────────────────────────────────────────
	ttl := time.Duration(cacheTTLSecs) * time.Second
	orderCache, err := cache.NewOrderCache(redisURL, ttl)
	if err != nil {
		// Non-fatal: service works without cache, just slower
		log.Printf("[OrderService] WARNING: Redis cache unavailable: %v — continuing without cache", err)
		orderCache = nil
	} else {
		defer orderCache.Close()
	}

	// ─── Handler ─────────────────────────────────────────────────────────────
	orderHandler, err := handler.NewOrderHandler(repo, paymentAddr, orderCache)
	if err != nil {
		log.Fatalf("[OrderService] Failed to init handler: %v", err)
	}

	// ─── gRPC Server ─────────────────────────────────────────────────────────
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("[OrderService] Failed to listen gRPC: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterOrderServiceServer(grpcServer, orderHandler)

	go func() {
		log.Printf("[OrderService] gRPC server listening on %s", grpcAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("[OrderService] gRPC serve error: %v", err)
		}
	}()

	// ─── HTTP REST Server with Rate Limiter ───────────────────────────────────
	httpHandler := handler.NewHTTPHandler(orderHandler)

	// Wrap handler with rate limiter middleware (Bonus)
	rateLimiter, err := middleware.NewRateLimiter(redisURL, rateLimitReqs, rateLimitWindow)
	if err != nil {
		log.Printf("[OrderService] WARNING: Rate limiter unavailable: %v — continuing without it", err)
	}

	var finalHandler http.Handler = httpHandler
	if rateLimiter != nil {
		finalHandler = rateLimiter.Wrap(httpHandler)
		defer rateLimiter.Close()
	}

	httpServer := &http.Server{
		Addr:    httpAddr,
		Handler: finalHandler,
	}

	go func() {
		log.Printf("[OrderService] HTTP REST server listening on %s", httpAddr)
		log.Printf("[OrderService] Postman: POST http://localhost:8080/orders")
		log.Printf("[OrderService] Postman: GET  http://localhost:8080/orders/{id}")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[OrderService] HTTP serve error: %v", err)
		}
	}()

	// ─── Graceful Shutdown ───────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("[OrderService] Shutting down gracefully...")
	grpcServer.GracefulStop()
	_ = httpServer.Close()
	log.Println("[OrderService] Shutdown complete")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
