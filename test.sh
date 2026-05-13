#!/bin/bash
# test.sh – Quick end-to-end test after docker-compose up

set -e

echo "=== AP2 Assignment 3 - Test Script ==="
echo ""

# Wait for services to be ready
echo "[1] Waiting 5s for services to stabilize..."
sleep 5

echo ""
echo "[2] Checking RabbitMQ Management UI..."
curl -s -u guest:guest http://localhost:15672/api/queues/%2F/payment.completed | python3 -m json.tool 2>/dev/null | grep -E '"name"|"messages"' || echo "  RabbitMQ not ready yet"

echo ""
echo "[3] Sending test order via grpcurl (requires grpcurl installed)..."
echo "    grpcurl -plaintext -d '{\"customer_email\":\"user@example.com\",\"amount\":99.99}' localhost:50051 ap2.OrderService/CreateOrder"

echo ""
echo "[4] Expected Notification Service output:"
echo "    [Notification] Sent email to user@example.com for Order #1. Amount: \$99.99"

echo ""
echo "[5] To check for duplicate handling, run the same order twice."
echo "    The second time, the notification should NOT be printed again for the same event_id."

echo ""
echo "[6] To view DLQ (Dead Letter Queue):"
echo "    curl -u guest:guest http://localhost:15672/api/queues/%2F/payment.dead_letter"
