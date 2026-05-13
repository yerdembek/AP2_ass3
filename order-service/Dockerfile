FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY . .
RUN go mod tidy
RUN go build -o order-service ./cmd/main.go

FROM alpine:3.18
WORKDIR /app
COPY --from=builder /app/order-service .
EXPOSE 50051
CMD ["./order-service"]
