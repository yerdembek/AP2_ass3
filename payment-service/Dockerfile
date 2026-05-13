FROM golang:1.21-alpine AS builder

WORKDIR /app
COPY . .
RUN go mod tidy
RUN go build -o payment-service ./cmd/main.go

FROM alpine:3.18
WORKDIR /app
COPY --from=builder /app/payment-service .
EXPOSE 50052
CMD ["./payment-service"]
