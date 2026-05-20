package main

import (
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"notification-service/internal/consumer"
	"notification-service/internal/provider"
	"notification-service/internal/repository"
)

func main() {
	// ─── Configuration ────────────────────────────────────────────────────────
	rabbitURL := getEnv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/")
	dbURL := getEnv("DB_URL", "postgres://ap2user:ap2pass@localhost:5432/ap2db?sslmode=disable")
	redisURL := getEnv("REDIS_URL", "redis://localhost:6379/0")
	providerMode := getEnv("PROVIDER_MODE", "SIMULATED")

	numWorkers, _ := strconv.Atoi(getEnv("NUM_WORKERS", "3"))
	maxRetries, _ := strconv.Atoi(getEnv("MAX_RETRIES", "3"))
	baseDelaySecs, _ := strconv.Atoi(getEnv("RETRY_BASE_SECONDS", "2"))
	idempotencyTTLHours, _ := strconv.Atoi(getEnv("IDEMPOTENCY_TTL_HOURS", "48"))

	baseDelay := time.Duration(baseDelaySecs) * time.Second

	log.Printf("[NotificationService] Starting | provider=%s workers=%d maxRetries=%d baseDelay=%s",
		providerMode, numWorkers, maxRetries, baseDelay)

	// ─── PostgreSQL Idempotency Store (fallback / backward compat) ───────────
	pgStore, err := repository.NewIdempotencyStore(dbURL)
	if err != nil {
		log.Printf("[NotificationService] WARNING: PostgreSQL idempotency store unavailable: %v", err)
		pgStore = nil
	} else {
		defer pgStore.Close()
		log.Println("[NotificationService] Connected to PostgreSQL (idempotency fallback)")
	}

	// ─── Redis Idempotency Store (primary) ────────────────────────────────────
	redisStore, err := repository.NewRedisIdempotencyStore(redisURL, idempotencyTTLHours)
	if err != nil {
		log.Fatalf("[NotificationService] Failed to connect to Redis: %v", err)
	}
	defer redisStore.Close()

	// ─── Email Provider (Adapter Pattern) ─────────────────────────────────────
	// The provider is chosen at startup via PROVIDER_MODE env var.
	// The consumer only depends on the EmailSender interface — it doesn't know
	// which concrete implementation is used.
	var emailSender provider.EmailSender
	switch providerMode {
	case "REAL":
		smtpHost := getEnv("SMTP_HOST", "smtp.gmail.com")
		smtpPort := getEnv("SMTP_PORT", "587")
		smtpUser := getEnv("SMTP_USER", "")
		smtpPass := getEnv("SMTP_PASS", "")
		smtpFrom := getEnv("SMTP_FROM", smtpUser)

		if smtpUser == "" || smtpPass == "" {
			log.Println("[NotificationService] WARNING: SMTP credentials not set — falling back to SIMULATED")
			emailSender = provider.NewSimulatedEmailSender(0.3)
		} else {
			emailSender = provider.NewSMTPEmailSender(smtpHost, smtpPort, smtpUser, smtpPass, smtpFrom)
			log.Println("[NotificationService] Using REAL SMTP provider")
		}
	default: // SIMULATED
		emailSender = provider.NewSimulatedEmailSender(0.3) // 30% failure rate
		log.Println("[NotificationService] Using SIMULATED email provider (30% failure rate)")
	}

	// ─── Consumer (Worker Pool) ───────────────────────────────────────────────
	notifConsumer, err := consumer.NewNotificationConsumer(
		rabbitURL,
		redisStore,
		pgStore,
		emailSender,
		numWorkers,
		maxRetries,
		baseDelay,
	)
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

	log.Printf("[NotificationService] Starting %d worker goroutines...", numWorkers)
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
