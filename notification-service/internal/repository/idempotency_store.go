package repository

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

// IdempotencyStore persists processed event IDs in PostgreSQL.
// Before processing any message, the consumer checks this store.
// If the event_id already exists, the message is a duplicate and is silently ACKed.
type IdempotencyStore struct {
	db *sql.DB
}

func NewIdempotencyStore(dsn string) (*IdempotencyStore, error) {
	var db *sql.DB
	var err error

	for i := 0; i < 10; i++ {
		db, err = sql.Open("postgres", dsn)
		if err == nil {
			if err = db.Ping(); err == nil {
				break
			}
		}
		log.Printf("[IdempotencyStore] Waiting for DB... attempt %d/10", i+1)
		time.Sleep(3 * time.Second)
	}
	if err != nil {
		return nil, fmt.Errorf("connect to db: %w", err)
	}
	return &IdempotencyStore{db: db}, nil
}

// IsDuplicate returns true if the eventID was already processed.
func (s *IdempotencyStore) IsDuplicate(eventID string) (bool, error) {
	var existing string
	err := s.db.QueryRow(
		`SELECT event_id FROM processed_events WHERE event_id = $1`,
		eventID,
	).Scan(&existing)
	if err == nil {
		return true, nil // found → duplicate
	}
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil // not found → new event
	}
	return false, fmt.Errorf("idempotency check: %w", err)
}

// MarkProcessed records the eventID so future duplicates are detected.
func (s *IdempotencyStore) MarkProcessed(eventID string) error {
	_, err := s.db.Exec(
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`,
		eventID,
	)
	return err
}

func (s *IdempotencyStore) Close() error {
	return s.db.Close()
}
