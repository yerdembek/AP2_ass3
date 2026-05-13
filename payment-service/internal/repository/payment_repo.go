package repository

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type Payment struct {
	ID      int32
	OrderID int32
	Amount  float64
	Status  string
}

type PaymentRepository struct {
	db *sql.DB
}

func NewPaymentRepository(dsn string) (*PaymentRepository, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return &PaymentRepository{db: db}, nil
}

// CreatePayment inserts a payment record and returns its ID.
// Wrapped in a DB transaction to ensure atomicity.
func (r *PaymentRepository) CreatePayment(orderID int32, amount float64) (int32, error) {
	tx, err := r.db.Begin()
	if err != nil {
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	var id int32
	err = tx.QueryRow(
		`INSERT INTO payments (order_id, amount, status) VALUES ($1, $2, 'completed') RETURNING id`,
		orderID, amount,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert payment: %w", err)
	}

	if err = tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit tx: %w", err)
	}
	return id, nil
}

func (r *PaymentRepository) Close() error {
	return r.db.Close()
}
