package handler

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"

	pb "order-service/proto"
)

// HTTPHandler wraps OrderHandler and exposes REST endpoints for Postman testing.
type HTTPHandler struct {
	order *OrderHandler
}

func NewHTTPHandler(order *OrderHandler) *HTTPHandler {
	return &HTTPHandler{order: order}
}

// ServeHTTP routes requests:
//
//	POST /orders        → CreateOrder
//	GET  /orders/{id}   → GetOrder
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Allow CORS for easy browser/Postman access
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)

	if parts[0] != "orders" {
		writeError(w, http.StatusNotFound, "endpoint not found — use POST /orders or GET /orders/{id}")
		return
	}

	switch {
	// POST /orders — Create a new order
	case r.Method == http.MethodPost && len(parts) == 1:
		h.createOrder(w, r)

	// GET /orders/{id} — Get an existing order
	case r.Method == http.MethodGet && len(parts) == 2:
		h.getOrder(w, r, parts[1])

	default:
		writeError(w, http.StatusMethodNotAllowed, "use POST /orders or GET /orders/{id}")
	}
}

func (h *HTTPHandler) createOrder(w http.ResponseWriter, r *http.Request) {
	var req pb.CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	if req.CustomerEmail == "" || req.Amount <= 0 {
		writeError(w, http.StatusBadRequest, "customer_email and amount (>0) are required")
		return
	}

	resp, err := h.order.CreateOrder(context.Background(), &req)
	if err != nil {
		log.Printf("[HTTP] CreateOrder error: %v", err)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(resp)
}

func (h *HTTPHandler) getOrder(w http.ResponseWriter, r *http.Request, idStr string) {
	id, err := strconv.Atoi(idStr)
	if err != nil || id <= 0 {
		writeError(w, http.StatusBadRequest, "order_id must be a positive integer")
		return
	}

	resp, err := h.order.GetOrder(context.Background(), &pb.GetOrderRequest{OrderId: int32(id)})
	if err != nil {
		log.Printf("[HTTP] GetOrder error: %v", err)
		writeError(w, http.StatusNotFound, err.Error())
		return
	}

	_ = json.NewEncoder(w).Encode(resp)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
