// Package provider defines the EmailSender interface and its implementations.
// The Adapter Pattern is used here: the consumer only depends on the EmailSender
// interface, not on any concrete email library or vendor SDK.
// The actual implementation (Simulated or SMTP) is chosen at startup via env var.
package provider

// EmailSender is the abstraction (port) that the notification worker uses.
// Any concrete email provider must implement this interface.
// This decouples business logic from vendor-specific details (SMTP, Mailjet, etc.).
type EmailSender interface {
	// Send sends an email to the given recipient.
	// Returns an error if the delivery failed (and should be retried).
	Send(to, subject, body string) error
}
