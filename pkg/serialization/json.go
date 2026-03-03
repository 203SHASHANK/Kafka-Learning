// Package serialization provides message encoding/decoding utilities.
//
// Kafka messages are raw bytes. Serialization is the process of converting
// your Go structs into bytes for the wire, and back again on the other side.
//
// Format comparison:
//
//	JSON      – Human-readable, self-describing, larger payload, slower
//	Protobuf  – Binary, schema-required, smaller payload, faster, backward compatible
//	Avro      – Binary, schema in registry, best for schema evolution at scale
//	MessagePack – Binary JSON alternative, no schema, smaller than JSON
//
// For most Go microservices starting with Kafka, JSON is fine.
// Switch to Protobuf when payload size or throughput becomes a concern.
package serialization

import (
	"encoding/json"
	"fmt"
)

// JSONSerializer provides type-safe JSON encode/decode using Go generics.
// The type parameter T is your domain event type (e.g., OrderEvent, PaymentEvent).
//
// Usage:
//
//	type OrderEvent struct {
//	    OrderID string `json:"order_id"`
//	    Amount  float64 `json:"amount"`
//	}
//
//	s := serialization.NewJSON[OrderEvent]()
//	bytes, _ := s.Marshal(OrderEvent{OrderID: "123", Amount: 99.99})
//	event, _  := s.Unmarshal(bytes)
type JSONSerializer[T any] struct{}

// NewJSON creates a new JSON serializer for the given type.
func NewJSON[T any]() *JSONSerializer[T] {
	return &JSONSerializer[T]{}
}

// Marshal serializes a value to JSON bytes.
func (s *JSONSerializer[T]) Marshal(v T) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json marshal %T: %w", v, err)
	}
	return b, nil
}

// Unmarshal deserializes JSON bytes into a value of type T.
func (s *JSONSerializer[T]) Unmarshal(data []byte) (T, error) {
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return v, fmt.Errorf("json unmarshal into %T: %w", v, err)
	}
	return v, nil
}

// ─── Envelope Pattern ────────────────────────────────────────────────────────

// Envelope wraps an event payload with metadata needed for routing, versioning,
// and distributed tracing.
//
// WHY use an envelope?
//   - Version field: allows consumers to handle schema migrations gracefully.
//     Consumer sees version "2.0" → uses new logic; version "1.0" → uses old logic.
//   - EventType field: a single topic can carry multiple event types
//     (OrderPlaced, OrderCancelled, OrderShipped). Consumers filter by type.
//   - TraceID field: propagates a distributed trace across service boundaries.
//     Without this, correlating logs across producer and consumer is painful.
//
// Common pattern for event-driven microservices.
type Envelope[T any] struct {
	Version   string `json:"version"`
	EventType string `json:"event_type"`
	TraceID   string `json:"trace_id,omitempty"`
	Source    string `json:"source,omitempty"`
	Payload   T      `json:"payload"`
}

// Wrap creates an envelope around a payload.
func Wrap[T any](eventType, version, source string, payload T) *Envelope[T] {
	return &Envelope[T]{
		Version:   version,
		EventType: eventType,
		Source:    source,
		Payload:   payload,
	}
}

// MarshalEnvelope serializes an envelope to JSON bytes.
func MarshalEnvelope[T any](env *Envelope[T]) ([]byte, error) {
	b, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("marshaling envelope: %w", err)
	}
	return b, nil
}

// UnmarshalEnvelope deserializes JSON bytes into a typed Envelope.
func UnmarshalEnvelope[T any](data []byte) (*Envelope[T], error) {
	var env Envelope[T]
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("unmarshaling envelope: %w", err)
	}
	return &env, nil
}

// ─── Convenience Functions ───────────────────────────────────────────────────

// Marshal serializes any value to JSON bytes.
func Marshal(v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json marshal: %w", err)
	}
	return b, nil
}

// Unmarshal deserializes JSON bytes into a value of type T.
func Unmarshal[T any](data []byte) (T, error) {
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return v, fmt.Errorf("json unmarshal: %w", err)
	}
	return v, nil
}
