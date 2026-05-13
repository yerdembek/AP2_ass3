package main

import (
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"order-service/internal/handler"
	"order-service/internal/repository"
	pb "order-service/proto"
)

func main() {
	dbURL := getEnv("DB_URL", "postgres://ap2user:ap2pass@localhost:5432/ap2db?sslmode=disable")
	paymentAddr := getEnv("PAYMENT_ADDR", "localhost:50052")
	grpcAddr := ":50051"
	httpAddr := ":8080"

	// ─── Repository ──────────────────────────────────────────────────────────
	repo, err := repository.NewOrderRepository(dbURL)
	if err != nil {
		log.Fatalf("[OrderService] DB connection failed: %v", err)
	}
	defer repo.Close()
	log.Println("[OrderService] Connected to PostgreSQL")

	// ─── Handler ─────────────────────────────────────────────────────────────
	orderHandler, err := handler.NewOrderHandler(repo, paymentAddr)
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

	// ─── HTTP REST Server (for Postman testing) ───────────────────────────────
	httpHandler := handler.NewHTTPHandler(orderHandler)
	httpServer := &http.Server{
		Addr:    httpAddr,
		Handler: httpHandler,
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
