package provider

import (
	"crypto/tls"
	"fmt"
	"log"
	"net"
	"net/smtp"
	"strings"
)

// SMTPEmailSender is the real adapter for sending emails via SMTP.
// It implements the EmailSender interface using Go's standard net/smtp package.
// Activate it by setting PROVIDER_MODE=REAL in the environment.
type SMTPEmailSender struct {
	host     string
	port     string
	username string
	password string
	from     string
}

// NewSMTPEmailSender creates a real SMTP email sender.
// Configure via environment variables: SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, SMTP_FROM.
func NewSMTPEmailSender(host, port, username, password, from string) *SMTPEmailSender {
	log.Printf("[SMTPProvider] Initialized (host=%s:%s from=%s)", host, port, from)
	return &SMTPEmailSender{
		host:     host,
		port:     port,
		username: username,
		password: password,
		from:     from,
	}
}

// Send sends a real email via SMTP using STARTTLS.
// Implements the EmailSender interface.
func (s *SMTPEmailSender) Send(to, subject, body string) error {
	addr := net.JoinHostPort(s.host, s.port)

	// Build the raw email message
	msg := strings.NewReader(fmt.Sprintf(
		"From: %s\r\nTo: %s\r\nSubject: %s\r\nMIME-Version: 1.0\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s",
		s.from, to, subject, body,
	))

	// Connect to SMTP server
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return fmt.Errorf("smtp dial %s: %w", addr, err)
	}

	client, err := smtp.NewClient(conn, s.host)
	if err != nil {
		return fmt.Errorf("smtp new client: %w", err)
	}
	defer client.Quit() //nolint:errcheck

	// Upgrade to TLS (STARTTLS)
	tlsCfg := &tls.Config{ServerName: s.host}
	if err = client.StartTLS(tlsCfg); err != nil {
		return fmt.Errorf("starttls: %w", err)
	}

	// Authenticate
	auth := smtp.PlainAuth("", s.username, s.password, s.host)
	if err = client.Auth(auth); err != nil {
		return fmt.Errorf("smtp auth: %w", err)
	}

	// Send
	if err = client.Mail(s.from); err != nil {
		return fmt.Errorf("smtp MAIL FROM: %w", err)
	}
	if err = client.Rcpt(to); err != nil {
		return fmt.Errorf("smtp RCPT TO: %w", err)
	}

	wc, err := client.Data()
	if err != nil {
		return fmt.Errorf("smtp DATA: %w", err)
	}
	defer wc.Close() //nolint:errcheck

	buf := make([]byte, msg.Len())
	msg.Read(buf) //nolint:errcheck
	if _, err = wc.Write(buf); err != nil {
		return fmt.Errorf("smtp write body: %w", err)
	}

	log.Printf("[SMTPProvider] ✓ Email sent to=%s subject=%q", to, subject)
	return nil
}
