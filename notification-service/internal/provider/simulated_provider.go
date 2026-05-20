package provider

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

// SimulatedEmailSender is a mock adapter that satisfies the EmailSender interface.
// It simulates real-world conditions to test the retry & backoff logic:
//   - Network latency: random sleep between 100ms and 500ms
//   - Transient failures: ~30% chance of returning an error
//
// This is analogous to a barista who is sometimes slow and sometimes makes mistakes
// — but the worker retries until it succeeds or exhausts its attempts.
type SimulatedEmailSender struct {
	failureRate float64 // e.g. 0.3 = 30% failure rate
}

// NewSimulatedEmailSender creates a SimulatedEmailSender with the given failure rate.
// failureRate should be between 0.0 (never fails) and 1.0 (always fails).
func NewSimulatedEmailSender(failureRate float64) *SimulatedEmailSender {
	log.Printf("[SimulatedProvider] Initialized (failure_rate=%.0f%%)", failureRate*100)
	return &SimulatedEmailSender{failureRate: failureRate}
}

// Send simulates sending an email by sleeping (latency) then randomly failing.
// Implements the EmailSender interface.
func (s *SimulatedEmailSender) Send(to, subject, body string) error {
	// Simulate network latency: 100ms – 500ms
	latency := time.Duration(100+rand.Intn(400)) * time.Millisecond
	time.Sleep(latency)

	// Simulate random transient failure
	if rand.Float64() < s.failureRate {
		log.Printf("[SimulatedProvider] ✗ FAILED to send email to %s (simulated error after %s)", to, latency)
		return fmt.Errorf("simulated provider error: transient network failure")
	}

	log.Printf("[SimulatedProvider] ✓ Email sent to=%s subject=%q (latency=%s)", to, subject, latency)
	return nil
}
