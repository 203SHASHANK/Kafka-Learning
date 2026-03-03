// Example 06 — Retry Logic and Dead Letter Queue (DLQ)
//
// WHAT: Handle message processing failures gracefully with retries and a DLQ.
//
// WHY THIS PATTERN IS CRITICAL IN PRODUCTION:
//
// In distributed systems, processing failures are inevitable:
//   - Database connection timeouts (transient → retry)
//   - External service returning 503 (transient → retry)
//   - Malformed JSON payload (permanent → DLQ, don't retry)
//   - Business rule violation (permanent → DLQ with error context)
//
// THE PATTERN:
//
//   ┌──────────┐     ┌──────────────────┐     ┌───────────────────┐
//   │  Kafka   │────▶│  Consumer tries  │────▶│  Success: commit  │
//   │ (topic)  │     │  message handler │     │  offset, continue │
//   └──────────┘     └──────────────────┘     └───────────────────┘
//                              │
//                    transient error
//                              │
//                    ┌─────────▼─────────┐
//                    │  Retry with exp.  │
//                    │  backoff (3x)     │
//                    └─────────┬─────────┘
//                    all retries │ failed
//                              │
//                    ┌─────────▼─────────┐
//                    │  Send to DLQ      │──▶ orders.dlq topic
//                    │  with context:    │    (for investigation)
//                    │  - original msg   │
//                    │  - error reason   │
//                    │  - retry count    │
//                    └───────────────────┘
//
// DLQ BEST PRACTICES:
//   - Always include original topic, partition, offset in the DLQ message
//   - Include the error message and retry count
//   - Have a separate consumer monitoring the DLQ and alerting humans
//   - DLQ messages may need manual reprocessing or code fixes before requeue
//
// HOW TO RUN:
//   make docker-up && make run-dlq
//   Watch both "orders" and "orders.dlq" topics in Kafka UI.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

const (
	sourceTopic = "orders"
	dlqTopic    = "orders.dlq"
	maxRetries  = 3
)

// DLQMessage wraps the original failed message with error context.
// This is what gets published to the DLQ topic for investigation.
type DLQMessage struct {
	OriginalTopic     string    `json:"original_topic"`
	OriginalPartition int32     `json:"original_partition"`
	OriginalOffset    int64     `json:"original_offset"`
	OriginalKey       string    `json:"original_key"`
	OriginalValue     string    `json:"original_value"`
	ErrorReason       string    `json:"error_reason"`
	RetryCount        int       `json:"retry_count"`
	FailedAt          time.Time `json:"failed_at"`
}

func main() {
	brokers := []string{"localhost:9092"}

	// ── DLQ Producer ──────────────────────────────────────────────────────────
	// The consumer needs a producer to forward failed messages to the DLQ.
	prodCfg := sarama.NewConfig()
	prodCfg.Version = sarama.V3_6_0_0
	prodCfg.Producer.RequiredAcks = sarama.WaitForAll
	prodCfg.Producer.Return.Successes = true

	dlqProducer, err := sarama.NewSyncProducer(brokers, prodCfg)
	if err != nil {
		log.Fatalf("Creating DLQ producer: %v", err)
	}
	defer dlqProducer.Close()

	// ── Consumer Setup ────────────────────────────────────────────────────────
	conCfg := sarama.NewConfig()
	conCfg.Version = sarama.V3_6_0_0
	conCfg.Consumer.Offsets.AutoCommit.Enable = false
	conCfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	conCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}

	client, err := sarama.NewConsumerGroup(brokers, "dlq-demo-group", conCfg)
	if err != nil {
		log.Fatalf("Creating consumer group: %v", err)
	}
	defer client.Close()

	handler := &dlqHandler{dlqProducer: dlqProducer}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			if err := client.Consume(ctx, []string{sourceTopic}, handler); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Consumer error: %v", err)
			}
		}
	}()

	fmt.Println("DLQ Demo Consumer running.")
	fmt.Println("Messages with even order IDs will 'fail' and be retried, then sent to DLQ.")
	fmt.Printf("Watch topic=%q for DLQ messages.\n\n", dlqTopic)

	// Also produce some test messages
	go produceTestMessages(brokers, sourceTopic)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	cancel()
	wg.Wait()
}

// dlqHandler is a consumer that retries failed messages and forwards to DLQ.
type dlqHandler struct {
	dlqProducer sarama.SyncProducer
	ready       chan struct{}
	once        sync.Once
}

func (h *dlqHandler) Setup(s sarama.ConsumerGroupSession) error {
	if h.ready == nil {
		h.ready = make(chan struct{})
	}
	h.once.Do(func() { close(h.ready) })
	return nil
}

func (h *dlqHandler) Cleanup(s sarama.ConsumerGroupSession) error { return nil }

func (h *dlqHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		h.processWithRetryAndDLQ(session, msg)
	}
	return nil
}

// processWithRetryAndDLQ attempts processing with exponential backoff retry.
// On exhausted retries, it sends the message to the DLQ.
func (h *dlqHandler) processWithRetryAndDLQ(
	session sarama.ConsumerGroupSession,
	msg *sarama.ConsumerMessage,
) {
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		lastErr = h.handle(msg)
		if lastErr == nil {
			// Success: mark the offset and move on.
			session.MarkMessage(msg, "")
			fmt.Printf("[SUCCESS] partition=%d offset=%d attempt=%d\n",
				msg.Partition, msg.Offset, attempt)
			return
		}

		if attempt == maxRetries {
			break // Don't sleep after final attempt
		}

		// Exponential backoff with jitter: 100ms, 200ms, 400ms...
		delay := time.Duration(float64(100*time.Millisecond) * math.Pow(2, float64(attempt-1)))
		jitter := time.Duration(rand.Int64N(int64(delay / 4)))
		delay += jitter

		fmt.Printf("[RETRY %d/%d] partition=%d offset=%d error=%v, retrying in %v\n",
			attempt, maxRetries, msg.Partition, msg.Offset, lastErr, delay)

		select {
		case <-session.Context().Done():
			return
		case <-time.After(delay):
		}
	}

	// All retries exhausted: send to DLQ and commit the offset.
	// Committing after DLQ is critical: without it, this message loops forever.
	fmt.Printf("[DLQ] partition=%d offset=%d error=%v → sending to %q\n",
		msg.Partition, msg.Offset, lastErr, dlqTopic)

	if err := h.sendToDLQ(msg, lastErr.Error(), maxRetries); err != nil {
		// DLQ send failed: don't commit, let it be redelivered.
		fmt.Printf("[ERROR] Failed to send to DLQ: %v\n", err)
		return
	}

	// Mark after successful DLQ send so we don't reprocess this message.
	session.MarkMessage(msg, "sent-to-dlq")
}

// handle contains your business logic. Returns error on failure.
func (h *dlqHandler) handle(msg *sarama.ConsumerMessage) error {
	// Parse message to extract order ID
	var payload map[string]interface{}
	if err := json.Unmarshal(msg.Value, &payload); err != nil {
		// Permanent failure: bad JSON will never succeed. In production,
		// use retry.Permanent(err) to skip retries and go straight to DLQ.
		return fmt.Errorf("invalid JSON: %w", err)
	}

	orderID, _ := payload["order_id"].(float64)

	// Simulate: even-numbered orders fail (e.g., database constraint error)
	if int(orderID)%2 == 0 {
		return fmt.Errorf("order %d failed downstream validation", int(orderID))
	}

	fmt.Printf("[OK]    partition=%d offset=%d order_id=%.0f\n",
		msg.Partition, msg.Offset, orderID)
	return nil
}

// sendToDLQ publishes the original message + error context to the DLQ topic.
func (h *dlqHandler) sendToDLQ(original *sarama.ConsumerMessage, reason string, retries int) error {
	dlq := DLQMessage{
		OriginalTopic:     original.Topic,
		OriginalPartition: original.Partition,
		OriginalOffset:    original.Offset,
		OriginalKey:       string(original.Key),
		OriginalValue:     string(original.Value),
		ErrorReason:       reason,
		RetryCount:        retries,
		FailedAt:          time.Now().UTC(),
	}

	dlqBytes, err := json.Marshal(dlq)
	if err != nil {
		return fmt.Errorf("marshaling DLQ message: %w", err)
	}

	_, _, err = h.dlqProducer.SendMessage(&sarama.ProducerMessage{
		Topic: dlqTopic,
		Key:   sarama.ByteEncoder(original.Key), // keep original key for ordering
		Value: sarama.ByteEncoder(dlqBytes),
		Headers: []sarama.RecordHeader{
			{Key: []byte("dlq-reason"), Value: []byte(reason)},
			{Key: []byte("original-topic"), Value: []byte(original.Topic)},
		},
	})
	return err
}

// produceTestMessages sends test messages for the DLQ demo.
func produceTestMessages(brokers []string, topic string) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true

	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return
	}
	defer p.Close()

	time.Sleep(2 * time.Second) // Let consumer start first

	for i := 1; i <= 10; i++ {
		payload := fmt.Sprintf(`{"order_id":%d,"amount":%.2f}`, i, float64(i)*10.0)
		p.SendMessage(&sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("order-%d", i)),
			Value: sarama.StringEncoder(payload),
		})
		time.Sleep(200 * time.Millisecond)
	}
}
