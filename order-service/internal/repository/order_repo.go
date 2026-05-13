package repository

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

type Order struct {
	ID            int32
	CustomerEmail string
	Amount        float64
	Status        string
}

type OrderRepository struct {
	db *sql.DB
}

func NewOrderRepository(dsn string) (*OrderRepository, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	if err = db.Ping(); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return &OrderRepository{db: db}, nil
}

func (r *OrderRepository) CreateOrder(email string, amount float64) (int32, error) {
	var id int32
	err := r.db.QueryRow(
		`INSERT INTO orders (customer_email, amount, status) VALUES ($1, $2, 'pending') RETURNING id`,
		email, amount,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert order: %w", err)
	}
	return id, nil
}

func (r *OrderRepository) UpdateOrderStatus(orderID int32, status string) error {
	_, err := r.db.Exec(
		`UPDATE orders SET status = $1 WHERE id = $2`,
		status, orderID,
	)
	return err
}

func (r *OrderRepository) GetOrder(orderID int32) (*Order, error) {
	row := r.db.QueryRow(
		`SELECT id, customer_email, amount, status FROM orders WHERE id = $1`,
		orderID,
	)
	o := &Order{}
	if err := row.Scan(&o.ID, &o.CustomerEmail, &o.Amount, &o.Status); err != nil {
		return nil, fmt.Errorf("get order: %w", err)
	}
	return o, nil
}

func (r *OrderRepository) Close() error {
	return r.db.Close()
}
