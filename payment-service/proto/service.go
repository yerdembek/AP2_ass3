package proto

import (
	"context"
	"encoding/json"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

func init() {
	encoding.RegisterCodec(JSONCodec{})
}

type JSONCodec struct{}

func (JSONCodec) Marshal(v interface{}) ([]byte, error)        { return json.Marshal(v) }
func (JSONCodec) Unmarshal(data []byte, v interface{}) error   { return json.Unmarshal(data, v) }
func (JSONCodec) Name() string                                  { return "proto" }

// ─── Message Types ────────────────────────────────────────────────────────────

type ProcessPaymentRequest struct {
	OrderId       int32   `json:"order_id"`
	Amount        float64 `json:"amount"`
	CustomerEmail string  `json:"customer_email"`
}

type ProcessPaymentResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func (r *ProcessPaymentRequest) Reset()         {}
func (r *ProcessPaymentRequest) String() string { return fmt.Sprintf("%+v", *r) }
func (r *ProcessPaymentRequest) ProtoMessage()  {}

func (r *ProcessPaymentResponse) Reset()         {}
func (r *ProcessPaymentResponse) String() string { return fmt.Sprintf("%+v", *r) }
func (r *ProcessPaymentResponse) ProtoMessage()  {}

// ─── Server Interface ─────────────────────────────────────────────────────────

type PaymentServiceServer interface {
	ProcessPayment(context.Context, *ProcessPaymentRequest) (*ProcessPaymentResponse, error)
	mustEmbedUnimplementedPaymentServiceServer()
}

type UnimplementedPaymentServiceServer struct{}

func (UnimplementedPaymentServiceServer) ProcessPayment(_ context.Context, _ *ProcessPaymentRequest) (*ProcessPaymentResponse, error) {
	return nil, nil
}
func (UnimplementedPaymentServiceServer) mustEmbedUnimplementedPaymentServiceServer() {}

var _PaymentService_serviceDesc = grpc.ServiceDesc{
	ServiceName: "ap2.PaymentService",
	HandlerType: (*PaymentServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "ProcessPayment",
			Handler:    _PaymentService_ProcessPayment_Handler,
		},
	},
	Streams: []grpc.StreamDesc{},
}

func RegisterPaymentServiceServer(s *grpc.Server, srv PaymentServiceServer) {
	s.RegisterService(&_PaymentService_serviceDesc, srv)
}

func _PaymentService_ProcessPayment_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ProcessPaymentRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(PaymentServiceServer).ProcessPayment(ctx, in)
	}
	info := &grpc.UnaryServerInfo{Server: srv, FullMethod: "/ap2.PaymentService/ProcessPayment"}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(PaymentServiceServer).ProcessPayment(ctx, req.(*ProcessPaymentRequest))
	}
	return interceptor(ctx, in, info, handler)
}
