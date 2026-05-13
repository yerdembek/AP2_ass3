package proto

import (
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/encoding"
)

func init() {
	encoding.RegisterCodec(JSONCodec{})
}

// JSONCodec is a gRPC codec that uses JSON encoding instead of protobuf.
// This lets us avoid the protoc code-gen toolchain.
type JSONCodec struct{}

func (JSONCodec) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (JSONCodec) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

func (JSONCodec) Name() string { return "proto" }

// Reset / String / ProtoMessage stubs so structs satisfy proto.Message interface
// when used with grpc encoding. Not needed for JSON codec but kept for clarity.
func (r *CreateOrderRequest) Reset()         {}
func (r *CreateOrderRequest) String() string { return fmt.Sprintf("%+v", *r) }
func (r *CreateOrderRequest) ProtoMessage()  {}

func (r *CreateOrderResponse) Reset()         {}
func (r *CreateOrderResponse) String() string { return fmt.Sprintf("%+v", *r) }
func (r *CreateOrderResponse) ProtoMessage()  {}

func (r *GetOrderRequest) Reset()         {}
func (r *GetOrderRequest) String() string { return fmt.Sprintf("%+v", *r) }
func (r *GetOrderRequest) ProtoMessage()  {}

func (r *GetOrderResponse) Reset()         {}
func (r *GetOrderResponse) String() string { return fmt.Sprintf("%+v", *r) }
func (r *GetOrderResponse) ProtoMessage()  {}

func (r *ProcessPaymentRequest) Reset()         {}
func (r *ProcessPaymentRequest) String() string { return fmt.Sprintf("%+v", *r) }
func (r *ProcessPaymentRequest) ProtoMessage()  {}

func (r *ProcessPaymentResponse) Reset()         {}
func (r *ProcessPaymentResponse) String() string { return fmt.Sprintf("%+v", *r) }
func (r *ProcessPaymentResponse) ProtoMessage()  {}
