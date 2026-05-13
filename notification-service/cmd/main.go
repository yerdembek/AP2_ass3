package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"notification-service/internal/consumer"
	"notification-service/internal/repository"
)

func main() {
	rabbitURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	dbURL := getEnv("DB_URL", "postgres://ap2user:ap2pass@localhost:5432/ap2db?sslmode=disable")

	// ─── Idempotency Store (PostgreSQL) ──────────────────────────────────────
	store, err := repository.NewIdempotencyStore(dbURL)
	if err != nil {
		log.Fatalf("[NotificationService] Failed to connect to DB: %v", err)
	}
	defer store.Close()
	log.Println("[NotificationService] Connected to PostgreSQL (idempotency store)")

	// ─── Consumer ────────────────────────────────────────────────────────────
	notifConsumer, err := consumer.NewNotificationConsumer(rabbitURL, store)
	if err != nil {
		log.Fatalf("[NotificationService] Failed to create consumer: %v", err)
	}
	defer notifConsumer.Close()

	// ─── Graceful Shutdown ───────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan struct{})

	go func() {
		<-quit
		log.Println("[NotificationService] Shutdown signal received")
		close(done)
	}()

	log.Println("[NotificationService] Starting consumer...")
	if err = notifConsumer.Consume(done); err != nil {
		log.Fatalf("[NotificationService] Consumer error: %v", err)
	}

	log.Println("[NotificationService] Shutdown complete")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
