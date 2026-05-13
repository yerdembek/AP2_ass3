-- Orders table
CREATE TABLE IF NOT EXISTS orders (
    id          SERIAL PRIMARY KEY,
    customer_email VARCHAR(255) NOT NULL,
    amount      NUMERIC(10,2) NOT NULL,
    status      VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMP DEFAULT NOW()
);

-- Payments table
CREATE TABLE IF NOT EXISTS payments (
    id         SERIAL PRIMARY KEY,
    order_id   INT NOT NULL,
    amount     NUMERIC(10,2) NOT NULL,
    status     VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT NOW()
);

-- Idempotency store for notification service
-- Stores processed message IDs to prevent duplicate notifications
CREATE TABLE IF NOT EXISTS processed_events (
    event_id   VARCHAR(255) PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT NOW()
);
