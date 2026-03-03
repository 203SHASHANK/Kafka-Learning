package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	"kafka-learning/internal/config"
)

// ─── Sync Producer ───────────────────────────────────────────────────────────

// SyncProducer wraps sarama's SyncProducer.
//
// WHEN to use SyncProducer:
//   - You need delivery confirmation before proceeding (e.g., HTTP handler returning 200)
//   - Throughput is secondary to reliability
//   - Simple, sequential message sending
//
// TRADE-OFF: Each send blocks until Kafka responds. For 1000 msg/s with 5ms latency,
// you're limited to ~200 msg/s per goroutine. For higher throughput, use AsyncProducer.
type SyncProducer struct {
	producer sarama.SyncProducer
	logger   *zap.Logger
}

// NewSyncProducer creates a synchronous Kafka producer with production defaults.
func NewSyncProducer(cfg *config.Config) (*SyncProducer, error) {
	saramaCfg := buildBaseProducerConfig(cfg)

	// SyncProducer REQUIRES Return.Successes = true.
	// sarama uses this to know it should wait for and return the result.
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer(cfg.Kafka.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("creating sync producer (brokers=%v): %w", cfg.Kafka.Brokers, err)
	}

	log := zap.L().With(zap.String("component", "sync-producer"))
	log.Info("sync producer connected", zap.Strings("brokers", cfg.Kafka.Brokers))

	return &SyncProducer{producer: producer, logger: log}, nil
}

// Send publishes a message and blocks until Kafka acknowledges it.
// Returns the partition and offset on success.
func (p *SyncProducer) Send(ctx context.Context, msg *Message) (*ProduceResult, error) {
	if ctx.Err() != nil {
		return nil, fmt.Errorf("context already cancelled: %w", ctx.Err())
	}

	saramaMsg := toSaramaMessage(msg)

	start := time.Now()
	partition, offset, err := p.producer.SendMessage(saramaMsg)
	elapsed := time.Since(start)

	if err != nil {
		p.logger.Error("produce failed",
			zap.String("topic", msg.Topic),
			zap.Error(err),
			zap.Duration("elapsed", elapsed),
		)
		return nil, fmt.Errorf("producing to topic %q: %w", msg.Topic, err)
	}

	p.logger.Debug("message produced",
		zap.String("topic", msg.Topic),
		zap.Int32("partition", partition),
		zap.Int64("offset", offset),
		zap.Duration("elapsed", elapsed),
	)

	return &ProduceResult{Topic: msg.Topic, Partition: partition, Offset: offset}, nil
}

// SendBatch sends multiple messages in one network round-trip.
// More efficient than calling Send() in a loop because sarama can optimize the batch.
func (p *SyncProducer) SendBatch(ctx context.Context, msgs []*Message) error {
	if ctx.Err() != nil {
		return fmt.Errorf("context already cancelled: %w", ctx.Err())
	}

	saramaMessages := make([]*sarama.ProducerMessage, len(msgs))
	for i, m := range msgs {
		saramaMessages[i] = toSaramaMessage(m)
	}

	if err := p.producer.SendMessages(saramaMessages); err != nil {
		// sarama returns ProducerErrors (a slice) for batch failures.
		// Each entry tells us which individual message failed.
		if prodErrs, ok := err.(sarama.ProducerErrors); ok {
			for _, pe := range prodErrs {
				p.logger.Error("batch message failed",
					zap.String("topic", pe.Msg.Topic),
					zap.Error(pe.Err),
				)
			}
		}
		return fmt.Errorf("sending batch of %d messages: %w", len(msgs), err)
	}

	return nil
}

// Close flushes pending messages and releases the producer connection.
// Always call Close() before your program exits.
func (p *SyncProducer) Close() error {
	p.logger.Info("closing sync producer")
	if err := p.producer.Close(); err != nil {
		return fmt.Errorf("closing sync producer: %w", err)
	}
	return nil
}

// ─── Async Producer ──────────────────────────────────────────────────────────

// AsyncProducer wraps sarama's AsyncProducer for high-throughput scenarios.
//
// WHEN to use AsyncProducer:
//   - You need maximum throughput (millions of messages per second)
//   - You can handle successes/errors out-of-band (via goroutines)
//   - You're OK with buffering (messages are batched before sending)
//
// TRADE-OFF: More complex error handling. You MUST drain the Errors() channel
// or the producer will deadlock when the error buffer is full.
type AsyncProducer struct {
	producer sarama.AsyncProducer
	logger   *zap.Logger
}

// NewAsyncProducer creates a high-throughput async Kafka producer.
func NewAsyncProducer(cfg *config.Config) (*AsyncProducer, error) {
	saramaCfg := buildBaseProducerConfig(cfg)
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Return.Errors = true

	// Flush when buffer reaches 64 KB OR after FlushFrequency, whichever comes first.
	saramaCfg.Producer.Flush.Frequency = cfg.Kafka.Producer.FlushFrequency
	saramaCfg.Producer.Flush.Bytes = 64 * 1024

	producer, err := sarama.NewAsyncProducer(cfg.Kafka.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("creating async producer: %w", err)
	}

	ap := &AsyncProducer{
		producer: producer,
		logger:   zap.L().With(zap.String("component", "async-producer")),
	}

	// These goroutines MUST run. Without them, the internal channels fill up
	// and the producer stalls. This is a common mistake with async producers.
	go ap.drainSuccesses()
	go ap.drainErrors()

	return ap, nil
}

// Send enqueues a message for async delivery. Never blocks on Kafka I/O.
func (ap *AsyncProducer) Send(msg *Message) {
	ap.producer.Input() <- toSaramaMessage(msg)
}

func (ap *AsyncProducer) drainSuccesses() {
	for msg := range ap.producer.Successes() {
		ap.logger.Debug("async message delivered",
			zap.String("topic", msg.Topic),
			zap.Int32("partition", msg.Partition),
			zap.Int64("offset", msg.Offset),
		)
	}
}

func (ap *AsyncProducer) drainErrors() {
	for err := range ap.producer.Errors() {
		ap.logger.Error("async delivery failed",
			zap.String("topic", err.Msg.Topic),
			zap.Error(err.Err),
		)
	}
}

// Close signals the async producer to stop after flushing buffered messages.
// This is non-blocking; use AsyncClose() and wait for the channel drains.
func (ap *AsyncProducer) Close() {
	ap.logger.Info("closing async producer")
	ap.producer.AsyncClose()
}

// ─── Shared Helpers ──────────────────────────────────────────────────────────

// buildBaseProducerConfig creates a sarama config with production-safe defaults.
// Examples and cmd packages override specific fields as needed.
func buildBaseProducerConfig(cfg *config.Config) *sarama.Config {
	c := sarama.NewConfig()
	c.Version = cfg.Kafka.Version

	c.Producer.Retry.Max = cfg.Kafka.Producer.RetryMax
	c.Producer.Retry.Backoff = 250 * time.Millisecond

	c.Producer.RequiredAcks = cfg.Kafka.Producer.RequiredAcks
	c.Producer.Compression = cfg.Kafka.Producer.Compression

	// MaxMessageBytes should match broker's message.max.bytes.
	// Default 1MB is generous for most workloads.
	c.Producer.MaxMessageBytes = 1024 * 1024

	if cfg.Kafka.Producer.Idempotent {
		c.Producer.Idempotent = true
		// MaxOpenRequests=1 is REQUIRED for idempotency.
		// Multiple in-flight requests could arrive out of order at the broker,
		// which would violate the sequence numbering that idempotency relies on.
		c.Net.MaxOpenRequests = 1
		// Idempotent producer requires WaitForAll—the broker needs ISR acknowledgment
		// to safely assign sequence numbers and detect duplicates.
		c.Producer.RequiredAcks = sarama.WaitForAll
	}

	if cfg.Kafka.Producer.TransactionID != "" {
		c.Producer.Transaction.ID = cfg.Kafka.Producer.TransactionID
		c.Producer.Transaction.Timeout = 60 * time.Second
		// Transactions imply idempotency
		c.Producer.Idempotent = true
		c.Net.MaxOpenRequests = 1
		c.Producer.RequiredAcks = sarama.WaitForAll
	}

	return c
}

// toSaramaMessage converts our domain Message into sarama's format.
func toSaramaMessage(msg *Message) *sarama.ProducerMessage {
	pm := &sarama.ProducerMessage{
		Topic: msg.Topic,
		Value: sarama.ByteEncoder(msg.Value),
	}

	if len(msg.Key) > 0 {
		pm.Key = sarama.ByteEncoder(msg.Key)
	}

	for k, v := range msg.Headers {
		pm.Headers = append(pm.Headers, sarama.RecordHeader{
			Key:   []byte(k),
			Value: v,
		})
	}

	if !msg.Timestamp.IsZero() {
		pm.Timestamp = msg.Timestamp
	}

	return pm
}
