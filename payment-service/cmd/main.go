package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"

	"payment-service/internal/handler"
	"payment-service/internal/messaging"
	"payment-service/internal/repository"
	pb "payment-service/proto"
)

func main() {
	dbURL := getEnv("DB_URL", "postgres://ap2user:ap2pass@localhost:5432/ap2db?sslmode=disable")
	rabbitURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	listenAddr := ":50052"

	// ─── Repository ──────────────────────────────────────────────────────────
	repo, err := repository.NewPaymentRepository(dbURL)
	if err != nil {
		log.Fatalf("[PaymentService] DB connection failed: %v", err)
	}
	defer repo.Close()
	log.Println("[PaymentService] Connected to PostgreSQL")

	// ─── Messaging (RabbitMQ Publisher) ──────────────────────────────────────
	publisher, err := messaging.NewPublisher(rabbitURL)
	if err != nil {
		log.Fatalf("[PaymentService] RabbitMQ connection failed: %v", err)
	}
	defer publisher.Close()

	// ─── gRPC Server ─────────────────────────────────────────────────────────
	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("[PaymentService] Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterPaymentServiceServer(grpcServer, handler.NewPaymentHandler(repo, publisher))

	// ─── Graceful Shutdown ───────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("[PaymentService] gRPC server listening on %s", listenAddr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("[PaymentService] Serve error: %v", err)
		}
	}()

	<-quit
	log.Println("[PaymentService] Shutting down gracefully...")
	grpcServer.GracefulStop()
	log.Println("[PaymentService] Shutdown complete")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
