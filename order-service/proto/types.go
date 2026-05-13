package proto

// Shared message types for Order and Payment services

type CreateOrderRequest struct {
	CustomerEmail string  `json:"customer_email"`
	Amount        float64 `json:"amount"`
}

type CreateOrderResponse struct {
	OrderId int32  `json:"order_id"`
	Status  string `json:"status"`
}

type GetOrderRequest struct {
	OrderId int32 `json:"order_id"`
}

type GetOrderResponse struct {
	OrderId       int32   `json:"order_id"`
	CustomerEmail string  `json:"customer_email"`
	Amount        float64 `json:"amount"`
	Status        string  `json:"status"`
}

type ProcessPaymentRequest struct {
	OrderId       int32   `json:"order_id"`
	Amount        float64 `json:"amount"`
	CustomerEmail string  `json:"customer_email"`
}

type ProcessPaymentResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}
