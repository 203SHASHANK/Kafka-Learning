// Example 01 — Basic Consumer (Single Partition)
//
// WHAT: Read messages from a specific Kafka partition using a standalone consumer.
//
// WHY START HERE:
// Understanding the low-level partition consumer helps you grasp what Kafka
// actually does: an ordered, immutable, append-only log per partition.
// Consumer groups (Example 02) build on top of this—they assign partitions
// to multiple consumer instances automatically.
//
// WHEN to use a standalone (non-group) consumer:
//   - Reading from a specific partition for debugging
//   - Building your own offset management strategy
//   - Consuming from multiple clusters with different group logic
//   - One-time data replay from a specific offset
//
// FOR PRODUCTION: Always use Consumer Groups (see Example 02).
// Standalone consumers require manual partition assignment and don't
// auto-scale or handle broker failures gracefully.
//
// HOW TO RUN:
//   Terminal 1: make docker-up
//   Terminal 2: make run-producer       ← start sending messages first
//   Terminal 3: make run-consumer       ← then read them here
//
// EXPECTED OUTPUT:
//   Consuming orders partition=0 from offset=newest
//   [offset=0] key=customer-1 value={"order_id": 1, ...}
//   [offset=1] key=customer-1 value={"order_id": 6, ...}
package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "orders"
	partition := int32(0) // Read from partition 0

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0

	// Return.Errors = true enables the Errors() channel.
	// Without this, errors are silently discarded. Always enable in production.
	cfg.Consumer.Return.Errors = true

	// ── Offset Configuration ──────────────────────────────────────────────────
	//
	// OffsetNewest (-1): Start from the tail of the log.
	//   Only messages produced AFTER the consumer starts are delivered.
	//   Use this when you only care about real-time events.
	//
	// OffsetOldest (-2): Start from the head of the log.
	//   ALL historical messages are replayed before new ones.
	//   Use this for backfill, reprocessing, or testing.
	//
	// Specific offset (e.g., 42): Resume from a known position.
	//   Use this for manual replay or debugging.
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	// ── Create Consumer ───────────────────────────────────────────────────────
	// NewConsumer connects to the broker and fetches cluster metadata (leader for each partition).
	consumer, err := sarama.NewConsumer(brokers, cfg)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v\n\nIs Kafka running? Run: make docker-up", err)
	}
	defer consumer.Close()

	// ConsumePartition attaches to a specific topic/partition.
	// The third argument is the starting offset: sarama.OffsetNewest in this case.
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("Failed to start partition consumer (topic=%q, partition=%d): %v", topic, partition, err)
	}
	defer partitionConsumer.Close()

	fmt.Printf("Consuming topic=%q partition=%d starting from=newest\n", topic, partition)
	fmt.Println("Waiting for messages... (run `make run-producer` in another terminal)")
	fmt.Println("Press Ctrl+C to stop\n")

	// ── Signal Handling ───────────────────────────────────────────────────────
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// ── Message Loop ──────────────────────────────────────────────────────────
	// partitionConsumer.Messages() is an unbuffered channel of *sarama.ConsumerMessage.
	// It blocks until a message arrives or the consumer is closed.
	for {
		select {
		case <-sigCh:
			fmt.Println("\nConsumer stopped")
			return

		case msg, ok := <-partitionConsumer.Messages():
			if !ok {
				fmt.Println("Message channel closed (broker disconnect?)")
				return
			}

			// ── Anatomy of a Kafka Message ────────────────────────────────────
			// Topic     : which topic it came from
			// Partition : which partition (0, 1, 2, ...)
			// Offset    : its unique position within this partition
			//             → monotonically increasing, never repeated
			//             → the "cursor" for this consumer
			// Key       : used for partitioning by the producer
			// Value     : the raw payload bytes
			// Timestamp : when the producer sent this message
			// Headers   : metadata key-value pairs
			fmt.Printf("[offset=%-6d] key=%-12s value=%s\n",
				msg.Offset,
				string(msg.Key),
				string(msg.Value),
			)

			if len(msg.Headers) > 0 {
				fmt.Printf("             headers: ")
				for _, h := range msg.Headers {
					fmt.Printf("%s=%s ", h.Key, h.Value)
				}
				fmt.Println()
			}

		case err := <-partitionConsumer.Errors():
			// Errors from the broker: fetch errors, leader changes, etc.
			fmt.Printf("Consumer error: %v\n", err)
		}
	}
}
