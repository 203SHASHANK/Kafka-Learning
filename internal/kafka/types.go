// Package kafka provides production-grade Kafka client wrappers built on sarama.
//
// WHY wrap sarama?
// sarama is excellent but low-level. Our wrappers add:
//   - Consistent structured logging on every operation
//   - Enforced production-safe defaults (acks, retries, compression)
//   - Uniform error types with context
//   - Hooks for metrics (Prometheus counters/histograms)
//   - Clean interfaces that are easy to mock in tests
//
// The goal: callers think in domain terms (send an order event),
// not Kafka protocol details (partition, sarama.ProducerMessage).
package kafka

import "time"

// Message is our domain-level Kafka message type.
// It abstracts away sarama.ProducerMessage (for producing) and
// sarama.ConsumerMessage (for consuming), giving one unified type.
type Message struct {
	// Topic is the Kafka topic name.
	Topic string

	// Key determines partition routing. Messages with the same key always land
	// on the same partition, guaranteeing ordering for that key.
	// A nil key results in round-robin partition assignment.
	Key []byte

	// Value is the raw message payload (JSON bytes, Protobuf bytes, etc.)
	Value []byte

	// Headers carry metadata without touching the payload.
	// Common uses: content-type, trace-id, schema-version, correlation-id.
	Headers map[string][]byte

	// Partition is populated after produce / during consume.
	Partition int32

	// Offset is the message's unique position within its partition.
	// Offsets are sequential and never re-used within a partition.
	Offset int64

	// Timestamp is the producer-set time. Used for time-based operations.
	Timestamp time.Time
}

// ProduceResult is returned by synchronous produce operations.
type ProduceResult struct {
	Topic     string
	Partition int32
	Offset    int64
}

// TopicConfig defines the parameters for creating a new Kafka topic.
type TopicConfig struct {
	Name string

	// Partitions determines throughput ceiling and parallelism.
	// More partitions = more consumers can run in parallel.
	// Rule of thumb: target throughput / (throughput per partition).
	Partitions int32

	// ReplicationFactor is how many broker copies exist for each partition.
	// Production minimum: 3 (survive 1 broker failure).
	// Must be <= number of brokers.
	ReplicationFactor int16

	// RetentionMs: how long messages are kept. -1 = forever (for compacted topics).
	// Default Kafka retention is 7 days (604800000 ms).
	RetentionMs int64

	// CleanupPolicy: "delete" (default, expire old segments) or
	// "compact" (keep only the latest value per key—for changelog/CDC patterns).
	CleanupPolicy string
}

// ConsumerGroupInfo holds metadata about a consumer group.
type ConsumerGroupInfo struct {
	GroupID string
	State   string // Empty, PreparingRebalance, CompletingRebalance, Stable, Dead
	Members []ConsumerGroupMember
}

// ConsumerGroupMember describes one consumer instance in the group.
type ConsumerGroupMember struct {
	ClientID   string
	ClientHost string
	Topics     []string
}

// MessageHandler is the callback signature for processing consumed messages.
//
// Implementations must be:
//   - Idempotent: Kafka delivers "at least once", so duplicates can occur.
//   - Context-aware: check ctx.Err() before expensive operations.
//   - Non-blocking for short messages: long processing blocks partition consumption.
//
// Return a non-nil error to signal processing failure (triggers retry or DLQ logic).
type MessageHandler func(ctx interface{ Err() error }, msg *Message) error
