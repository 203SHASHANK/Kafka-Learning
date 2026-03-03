// Example 09 — Exactly-Once Semantics (Read-Process-Write)
//
// WHAT: Process a message from one topic and write results to another topic
//       with a guarantee that each input message produces exactly one output.
//
// THE DELIVERY GUARANTEES SPECTRUM:
//
//   At-most-once:  Message may be lost. Never delivered twice. (fast, risky)
//   At-least-once: Message is never lost. May be delivered twice. (standard default)
//   Exactly-once:  Message is delivered exactly once. (most expensive, safest)
//
// WHY EXACTLY-ONCE IS HARD:
//
// "Read from A, transform, write to B" sounds simple. But consider:
//   1. Consumer reads from topic A
//   2. Consumer processes the message
//   3. Consumer writes result to topic B  ← success
//   4. Consumer commits offset on topic A ← CRASH before this step
//   5. After restart: consumer re-reads the same message from A
//   6. Result written to B again          ← DUPLICATE in topic B
//
// KAFKA'S EXACTLY-ONCE SOLUTION:
// Use a transactional producer that atomically:
//   - Writes output to topic B
//   - Commits the input offset on topic A
// Both happen in one atomic unit. Either both succeed or neither does.
//
// REQUIREMENTS:
//   - Transactional producer (config.Producer.Transaction.ID)
//   - Consumer group reading with isolation.level=read_committed
//     → consumers only see committed (non-transactional or committed transactional) messages
//
// PATTERN: "Consume-Transform-Produce" (CTP) with transactions
//
// HOW TO RUN:
//   make docker-up && make run-exactly-once
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

const (
	inputTopic  = "orders"
	outputTopic = "enriched-orders"
)

func main() {
	brokers := []string{"localhost:9092"}

	// ── Transactional Producer ────────────────────────────────────────────────
	// The Transaction ID must be UNIQUE and STABLE per logical processor instance.
	// If two producers share a Transaction ID, the broker treats it as a fence
	// (zombies are fenced off). In production: use a UUID per service instance,
	// persisted in stable storage.
	prodCfg := sarama.NewConfig()
	prodCfg.Version = sarama.V3_6_0_0

	// Required for transactions
	prodCfg.Producer.Idempotent = true
	prodCfg.Net.MaxOpenRequests = 1
	prodCfg.Producer.RequiredAcks = sarama.WaitForAll
	prodCfg.Producer.Return.Successes = true
	prodCfg.Producer.Return.Errors = true

	// THE TRANSACTION ID: uniquely identifies this producer instance.
	// The broker uses this to fence duplicate producers (zombie fencing).
	prodCfg.Producer.Transaction.ID = "eos-processor-instance-1"
	prodCfg.Producer.Transaction.Timeout = 60 * time.Second

	txnProducer, err := sarama.NewSyncProducer(brokers, prodCfg)
	if err != nil {
		log.Fatalf("Creating transactional producer: %v", err)
	}
	defer txnProducer.Close()

	// ── Consumer (read_committed isolation) ──────────────────────────────────
	// The consumer must use "read_committed" isolation level to only see
	// messages that were successfully committed in a transaction.
	// Without this, consumers would see transactional messages in an "uncommitted"
	// state if the producer crashes mid-transaction.
	conCfg := sarama.NewConfig()
	conCfg.Version = sarama.V3_6_0_0
	conCfg.Consumer.Offsets.AutoCommit.Enable = false
	conCfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	// READ_COMMITTED: only consume messages that are part of a committed transaction
	// (or non-transactional messages). Never see in-progress or aborted transactions.
	conCfg.Consumer.IsolationLevel = sarama.ReadCommitted

	conCfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}

	client, err := sarama.NewConsumerGroup(brokers, "eos-processor-group", conCfg)
	if err != nil {
		log.Fatalf("Creating consumer group: %v", err)
	}
	defer client.Close()

	handler := &eosHandler{producer: txnProducer, groupID: "eos-processor-group"}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			if err := client.Consume(ctx, []string{inputTopic}, handler); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Consume error: %v", err)
			}
		}
	}()

	// Start producer for demo
	go produceForEOS(ctx, brokers, inputTopic)

	fmt.Printf("Exactly-Once Processor running.\n")
	fmt.Printf("Reading from: %q → Writing to: %q\n", inputTopic, outputTopic)
	fmt.Println("Consumer uses read_committed isolation level.")
	fmt.Println("Press Ctrl+C to stop.\n")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	cancel()
	wg.Wait()
}

// eosHandler implements exactly-once consume-transform-produce.
type eosHandler struct {
	producer sarama.SyncProducer
	groupID  string
	ready    chan struct{}
	once     sync.Once
}

func (h *eosHandler) Setup(s sarama.ConsumerGroupSession) error {
	if h.ready == nil {
		h.ready = make(chan struct{})
	}
	h.once.Do(func() { close(h.ready) })
	return nil
}

func (h *eosHandler) Cleanup(s sarama.ConsumerGroupSession) error { return nil }

func (h *eosHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		if err := h.processExactlyOnce(session, msg); err != nil {
			log.Printf("EOS processing error at offset=%d: %v", msg.Offset, err)
		}
	}
	return nil
}

// processExactlyOnce wraps the read-transform-write in a single atomic transaction.
//
// The sequence:
//   1. BeginTxn          → opens a transaction
//   2. produce output    → writes enriched message (transactional)
//   3. AddOffsetsToTxn   → registers input offset as part of the transaction
//   4. CommitTxn         → atomically: commits output + advances input offset
//      OR AbortTxn       → on error: rolls back output, input offset unchanged
func (h *eosHandler) processExactlyOnce(
	session sarama.ConsumerGroupSession,
	msg *sarama.ConsumerMessage,
) error {
	// Step 1: Begin transaction
	if err := h.producer.BeginTxn(); err != nil {
		return fmt.Errorf("BeginTxn: %w", err)
	}

	// Step 2: Transform the message
	enriched, err := transform(msg.Value)
	if err != nil {
		// Permanent failure: abort transaction, skip this message
		_ = h.producer.AbortTxn()
		session.MarkMessage(msg, "transform-failed")
		log.Printf("Skipping malformed message at offset=%d: %v", msg.Offset, err)
		return nil
	}

	// Step 3: Produce the transformed message as part of the transaction
	_, _, err = h.producer.SendMessage(&sarama.ProducerMessage{
		Topic: outputTopic,
		Key:   sarama.ByteEncoder(msg.Key),
		Value: sarama.ByteEncoder(enriched),
		Headers: []sarama.RecordHeader{
			{Key: []byte("source-topic"), Value: []byte(msg.Topic)},
			{Key: []byte("source-offset"), Value: []byte(fmt.Sprintf("%d", msg.Offset))},
		},
	})
	if err != nil {
		_ = h.producer.AbortTxn()
		return fmt.Errorf("SendMessage in txn: %w", err)
	}

	// Step 4: Register the consumer offset as part of the transaction.
	// This is the key to exactly-once: offset commit is ATOMIC with the produce.
	// If this transaction commits → input offset advances + output is written.
	// If it aborts → neither happens → message redelivered, no duplicate output.
	offsets := map[string][]*sarama.PartitionOffsetMetadata{
		msg.Topic: {{
			Partition: msg.Partition,
			Offset:    msg.Offset + 1, // next offset to consume
		}},
	}
	if err := h.producer.AddOffsetsToTxn(offsets, h.groupID); err != nil {
		_ = h.producer.AbortTxn()
		return fmt.Errorf("AddOffsetsToTxn: %w", err)
	}

	// Step 5: Commit the transaction (atomic: output written + offset advanced)
	if err := h.producer.CommitTxn(); err != nil {
		return fmt.Errorf("CommitTxn: %w", err)
	}

	fmt.Printf("[EOS] offset=%-6d → enriched and committed atomically\n", msg.Offset)
	return nil
}

// transform adds fields to the original message payload.
func transform(value []byte) ([]byte, error) {
	// Simple enrichment: add a processing timestamp and enrichment tag
	original := strings.TrimRight(string(value), "}")
	if original == "" {
		return nil, fmt.Errorf("empty payload")
	}
	enriched := original + fmt.Sprintf(`, "enriched_at": "%s", "processor": "eos-v1"}`,
		time.Now().UTC().Format(time.RFC3339))
	return []byte(enriched), nil
}

func produceForEOS(ctx context.Context, brokers []string, topic string) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true

	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return
	}
	defer p.Close()

	time.Sleep(3 * time.Second)

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	i := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			i++
			payload := fmt.Sprintf(`{"order_id":%d,"amount":%.2f}`, i, float64(i)*19.99)
			p.SendMessage(&sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.StringEncoder(fmt.Sprintf("order-%d", i)),
				Value: sarama.StringEncoder(payload),
			})
			fmt.Printf("[PRODUCER] Produced order %d to %q\n", i, topic)
		}
	}
}
