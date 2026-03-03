// Example 05 — JSON Serialization with Typed Messages
//
// WHAT: Use Go structs (not raw strings) as Kafka message payloads with JSON encoding.
//
// WHY TYPED MESSAGES:
//
// In the basic examples, we sent raw JSON strings. In production, you want:
//   1. Compile-time type safety (no more typos in field names)
//   2. Automatic serialization/deserialization
//   3. Schema versioning (handle old and new message formats gracefully)
//   4. A standard envelope for trace IDs and event metadata
//
// SCHEMA EVOLUTION STRATEGY (JSON):
//   - Adding new fields with omitempty: backward compatible
//   - Removing fields: NOT backward compatible (old consumers expect them)
//   - Renaming fields: NOT backward compatible
//   - Use a "version" field in your envelope so consumers can branch on it
//
// FOR PRODUCTION AT SCALE:
//   Consider Apache Avro with Confluent Schema Registry:
//   - Schemas stored centrally, consumers auto-download them
//   - Built-in backward/forward compatibility checking
//   - Much smaller payloads than JSON (binary encoding)
//
// HOW TO RUN:
//   make docker-up && make run-serialization
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// ── Domain Types ─────────────────────────────────────────────────────────────

// OrderEvent represents an event in the order lifecycle.
// Field tags control JSON serialization:
//   `json:"field_name"`           → custom field name
//   `json:"field_name,omitempty"` → omit if zero value (nil, "", 0)
type OrderEvent struct {
	OrderID    string    `json:"order_id"`
	CustomerID string    `json:"customer_id"`
	EventType  string    `json:"event_type"` // placed, updated, shipped, delivered, cancelled
	Amount     float64   `json:"amount"`
	Currency   string    `json:"currency"`
	Items      []Item    `json:"items"`
	Metadata   *Metadata `json:"metadata,omitempty"`
	CreatedAt  time.Time `json:"created_at"`
}

// Item is a line item in the order.
type Item struct {
	SKU      string  `json:"sku"`
	Quantity int     `json:"quantity"`
	Price    float64 `json:"price"`
}

// Metadata carries optional context (shipping address, promo codes, etc.)
type Metadata struct {
	ShippingAddress string `json:"shipping_address,omitempty"`
	PromoCode       string `json:"promo_code,omitempty"`
	Notes           string `json:"notes,omitempty"`
}

// Envelope wraps any event payload with versioning and tracing metadata.
// This is a critical pattern for long-lived event-driven systems:
//   - "version" allows consumers to handle schema migrations gracefully
//   - "event_type" lets consumers filter events on a multi-type topic
//   - "trace_id" propagates across service boundaries for observability
type Envelope struct {
	Version   string          `json:"version"`
	EventType string          `json:"event_type"`
	Source    string          `json:"source"`
	TraceID   string          `json:"trace_id,omitempty"`
	Payload   json.RawMessage `json:"payload"` // raw JSON, defer decoding to consumer
}

func main() {
	brokers := []string{"localhost:9092"}
	topic := "orders"

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("Creating producer: %v", err)
	}

	// ── Produce typed messages ────────────────────────────────────────────────
	events := []OrderEvent{
		{
			OrderID:    "ord-001",
			CustomerID: "cust-42",
			EventType:  "placed",
			Amount:     149.99,
			Currency:   "USD",
			Items: []Item{
				{SKU: "LAPTOP-PRO", Quantity: 1, Price: 129.99},
				{SKU: "USB-HUB-4K", Quantity: 1, Price: 20.00},
			},
			Metadata:  &Metadata{ShippingAddress: "123 Main St, NY", PromoCode: "SAVE10"},
			CreatedAt: time.Now().UTC(),
		},
		{
			OrderID:    "ord-002",
			CustomerID: "cust-17",
			EventType:  "shipped",
			Amount:     49.99,
			Currency:   "USD",
			Items:      []Item{{SKU: "KEYBOARD-MECH", Quantity: 1, Price: 49.99}},
			CreatedAt:  time.Now().UTC(),
		},
	}

	fmt.Println("Producing typed OrderEvent messages...\n")
	for _, event := range events {
		if err := produceEvent(producer, topic, event); err != nil {
			log.Printf("Failed to produce event: %v", err)
		}
	}
	producer.Close()

	// ── Consume and deserialize ───────────────────────────────────────────────
	fmt.Println("\nConsuming and deserializing messages...\n")
	consumeAndDeserialize(brokers, topic)
}

// produceEvent serializes an event into an envelope and sends it.
func produceEvent(producer sarama.SyncProducer, topic string, event OrderEvent) error {
	// Step 1: Serialize the payload to JSON
	payloadBytes, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshaling event: %w", err)
	}

	// Step 2: Wrap in envelope with metadata
	envelope := Envelope{
		Version:   "1.0",
		EventType: event.EventType,
		Source:    "order-service",
		TraceID:   fmt.Sprintf("trace-%d", time.Now().UnixNano()),
		Payload:   json.RawMessage(payloadBytes),
	}

	// Step 3: Serialize the whole envelope
	envelopeBytes, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshaling envelope: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		// Key: CustomerID ensures all events for a customer are in the same partition
		Key:   sarama.StringEncoder(event.CustomerID),
		Value: sarama.ByteEncoder(envelopeBytes),
		Headers: []sarama.RecordHeader{
			{Key: []byte("content-type"), Value: []byte("application/json")},
			{Key: []byte("event-version"), Value: []byte(envelope.Version)},
		},
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("sending message: %w", err)
	}

	fmt.Printf("Produced: order_id=%-8s event_type=%-12s partition=%d offset=%d\n",
		event.OrderID, event.EventType, partition, offset)
	return nil
}

// consumeAndDeserialize reads messages back and deserializes them into typed structs.
func consumeAndDeserialize(brokers []string, topic string) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	cfg.Consumer.Offsets.AutoCommit.Enable = false
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest // replay from beginning for demo
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}

	client, err := sarama.NewConsumerGroup(brokers, "serialization-demo-group", cfg)
	if err != nil {
		log.Fatalf("Creating consumer group: %v", err)
	}
	defer client.Close()

	handler := &deserializeHandler{}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			_ = client.Consume(ctx, []string{topic}, handler)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
	case <-ctx.Done():
		fmt.Println("\nDemo complete.")
	}
	cancel()
	wg.Wait()
}

// deserializeHandler reads envelopes and unpacks them into typed OrderEvents.
type deserializeHandler struct {
	ready chan struct{}
	once  sync.Once
}

func (h *deserializeHandler) Setup(s sarama.ConsumerGroupSession) error {
	if h.ready == nil {
		h.ready = make(chan struct{})
	}
	h.once.Do(func() { close(h.ready) })
	return nil
}

func (h *deserializeHandler) Cleanup(s sarama.ConsumerGroupSession) error { return nil }

func (h *deserializeHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		// Step 1: Decode the outer envelope (don't decode payload yet)
		var env Envelope
		if err := json.Unmarshal(msg.Value, &env); err != nil {
			fmt.Printf("  ERROR: invalid envelope: %v\n", err)
			session.MarkMessage(msg, "")
			continue
		}

		// Step 2: Route by event type, decode payload accordingly
		switch env.EventType {
		case "placed", "shipped", "delivered", "cancelled", "updated":
			var order OrderEvent
			if err := json.Unmarshal(env.Payload, &order); err != nil {
				fmt.Printf("  ERROR: invalid OrderEvent payload: %v\n", err)
				session.MarkMessage(msg, "")
				continue
			}

			fmt.Printf("  Received [v%s] order_id=%-8s event=%-10s customer=%-10s amount=$%.2f trace=%s\n",
				env.Version,
				order.OrderID,
				order.EventType,
				order.CustomerID,
				order.Amount,
				env.TraceID,
			)

			if order.Metadata != nil && order.Metadata.PromoCode != "" {
				fmt.Printf("           promo_code=%s\n", order.Metadata.PromoCode)
			}

		default:
			fmt.Printf("  WARNING: unknown event_type=%q, skipping\n", env.EventType)
		}

		session.MarkMessage(msg, "")
	}
	return nil
}
