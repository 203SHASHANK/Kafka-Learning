// Example 07 — Idempotent Producer
//
// WHAT: Configure a producer so that retried messages are never duplicated.
//
// THE PROBLEM WITHOUT IDEMPOTENCY:
//
//   Producer sends message → Broker writes it → Network error (ACK lost)
//   Producer doesn't know if it arrived → Producer retries → Broker writes it AGAIN
//   Result: duplicate message in the topic
//
//   This is especially problematic for financial events (double-charge),
//   inventory (double-decrement), or any non-idempotent operation.
//
// THE SOLUTION — IDEMPOTENT PRODUCER:
//
//   Each producer instance gets a unique Producer ID (PID) from the broker.
//   Every message is tagged with a sequence number (monotonically increasing per partition).
//
//   Broker logic:
//     - First time message N arrives: write it
//     - If message N arrives again (retry): detect duplicate via sequence number, ACK but DON'T write
//
//   Result: exactly-once at the producer level, per partition.
//
// REQUIREMENTS for idempotent producer:
//   1. config.Producer.Idempotent = true
//   2. config.Net.MaxOpenRequests = 1
//      (Only one in-flight request per connection ensures sequence ordering)
//   3. config.Producer.RequiredAcks = sarama.WaitForAll
//      (Idempotency requires all ISR to acknowledge)
//   4. config.Producer.Return.Successes = true
//
// LIMITATIONS:
//   - Idempotency is per-producer-session (PID is lost on producer restart)
//   - Only within a single partition (doesn't span topics)
//   - For cross-partition or cross-session idempotency: use Transactions (Example 10)
//
// HOW TO RUN:
//   make docker-up && make run-idempotent
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "orders"

	// ── Idempotent Producer Configuration ────────────────────────────────────
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0

	// THE KEY SETTING: Enable idempotent production.
	// With this, the broker assigns a Producer ID and tracks sequence numbers.
	cfg.Producer.Idempotent = true

	// REQUIRED: MaxOpenRequests must be 1 for idempotency.
	// Multiple concurrent in-flight requests could arrive at the broker
	// out of order, breaking the sequence number guarantee.
	cfg.Net.MaxOpenRequests = 1

	// REQUIRED: Idempotency only works with WaitForAll.
	// The broker needs ISR acknowledgment to safely track sequence numbers.
	cfg.Producer.RequiredAcks = sarama.WaitForAll

	// REQUIRED: SyncProducer needs this.
	cfg.Producer.Return.Successes = true

	// Retries are safe now—duplicates are detected and discarded by the broker.
	cfg.Producer.Retry.Max = 10
	cfg.Producer.Retry.Backoff = 250 * time.Millisecond

	// ── Create Idempotent Producer ────────────────────────────────────────────
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("Creating idempotent producer: %v", err)
	}
	defer producer.Close()

	fmt.Println("Idempotent Producer ready.")
	fmt.Println("Producer ID (PID) assigned by broker tracks sequence numbers per partition.")
	fmt.Println("Retried messages will NOT be duplicated.\n")

	// ── Send messages ─────────────────────────────────────────────────────────
	// Each message gets an internal sequence number (0, 1, 2, ...) per partition.
	// The broker tracks: "last seen sequence for this PID on partition P is N".
	// On retry: if sequence is <= N, it's a duplicate → ACK but don't write.
	for i := 1; i <= 10; i++ {
		payload := fmt.Sprintf(`{"order_id":%d,"ts":"%s"}`, i, time.Now().UTC().Format(time.RFC3339))

		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   sarama.StringEncoder(fmt.Sprintf("order-%d", i)),
			Value: sarama.StringEncoder(payload),
		}

		start := time.Now()
		partition, offset, err := producer.SendMessage(msg)
		elapsed := time.Since(start)

		if err != nil {
			fmt.Printf("ERROR: message %d failed: %v\n", i, err)
			continue
		}

		fmt.Printf("Message %2d: partition=%-2d offset=%-6d elapsed=%v\n",
			i, partition, offset, elapsed.Round(time.Millisecond))
	}

	// ── Explanation ───────────────────────────────────────────────────────────
	fmt.Println()
	fmt.Println("──────────────────────────────────────────────────────────────")
	fmt.Println("Idempotency guarantee: EXACTLY-ONCE per producer session")
	fmt.Println()
	fmt.Println("What the broker did internally:")
	fmt.Println("  1. Assigned PID (Producer ID) on first produce")
	fmt.Println("  2. Tracked sequence numbers per (PID, topic, partition)")
	fmt.Println("  3. On any retry: checked if sequence already seen → dropped duplicate")
	fmt.Println()
	fmt.Println("What this doesn't cover:")
	fmt.Println("  - After producer restart, a new PID is assigned → retry = new message")
	fmt.Println("  - Cross-partition atomicity: use Transactions (Example 10)")
	fmt.Println("──────────────────────────────────────────────────────────────")
}
