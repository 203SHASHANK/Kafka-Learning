// Example 01 — Basic Synchronous Producer
//
// WHAT: Send messages to a Kafka topic using a synchronous producer.
//
// WHY THIS IS YOUR STARTING POINT:
// Every Kafka use case starts with a producer. Understanding how producers work—
// configuration, acknowledgements, compression, partitioning—forms the foundation
// for everything that follows.
//
// KEY CONCEPTS DEMONSTRATED:
//   1. Connecting to a Kafka broker
//   2. Producer configuration (acks, retries, compression)
//   3. Message structure: topic, key, value, headers
//   4. How keys route messages to partitions
//   5. Reading produce results (partition + offset)
//
// HOW TO RUN:
//   Terminal 1: make docker-up
//   Terminal 2: make run-producer
//
// EXPECTED OUTPUT:
//   Producer connected. Sending messages...
//   Message #1 → topic=orders partition=0 offset=0 key=customer-1
//   Message #2 → topic=orders partition=2 offset=0 key=customer-2
//   ...
//
// OBSERVE in Kafka UI (http://localhost:8080):
//   → Topics → orders → Messages: see each message arrive in real time.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "orders"

	// ── Producer Configuration ────────────────────────────────────────────────
	// sarama.NewConfig() provides safe defaults. Override only what you need.
	cfg := sarama.NewConfig()

	// Protocol version must match or be lower than your Kafka cluster version.
	// Using a higher version than the broker causes protocol negotiation failures.
	cfg.Version = sarama.V3_6_0_0

	// RequiredAcks is the durability guarantee:
	//
	//   NoResponse  (0): fire-and-forget. Fastest, no delivery guarantee.
	//                    Use only for non-critical metrics/telemetry.
	//
	//   WaitForLocal(1): leader writes to its log, then acks. Good default.
	//                    If leader crashes before replication, message is lost.
	//
	//   WaitForAll (-1): ALL in-sync replicas write to their log, then leader acks.
	//                    Safest. Use this for critical business events.
	cfg.Producer.RequiredAcks = sarama.WaitForAll

	// Retry failed sends up to 5 times with 250ms backoff between retries.
	// This handles transient broker leader elections and network blips.
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Retry.Backoff = 250 * time.Millisecond

	// REQUIRED for SyncProducer: tells sarama to block and return the result.
	// Without this, there is no result channel and SendMessage panics.
	cfg.Producer.Return.Successes = true

	// Compression reduces network bandwidth and broker disk usage.
	// Snappy: good CPU/ratio balance. LZ4: fastest. ZSTD: best compression.
	// Enable compression for any non-trivial production workload.
	cfg.Producer.Compression = sarama.CompressionSnappy

	// ── Create Producer ───────────────────────────────────────────────────────
	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("Failed to connect to Kafka at %v: %v\n\nIs Kafka running? Run: make docker-up", brokers, err)
	}
	defer func() {
		// Close() flushes pending messages and releases the TCP connection.
		// Always defer this—skipping it can cause data loss.
		if err := producer.Close(); err != nil {
			log.Printf("Warning: producer close error: %v", err)
		}
	}()

	fmt.Printf("Connected to Kafka broker: %v\n", brokers)
	fmt.Printf("Publishing to topic: %q\n", topic)
	fmt.Println("Press Ctrl+C to stop\n")

	// ── Graceful Shutdown ─────────────────────────────────────────────────────
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		fmt.Println("\nShutdown signal received")
		cancel()
	}()

	// ── Send Messages ─────────────────────────────────────────────────────────
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	messageNum := 0
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Producer stopped gracefully")
			return
		case <-ticker.C:
			messageNum++

			// ── Understanding Message Keys ────────────────────────────────────
			// The key is hashed to determine partition assignment:
			//   partition = hash(key) % num_partitions
			//
			// Same key → same partition → ordered delivery for that key.
			// This is how Kafka guarantees "all events for customer X are ordered":
			// use the customer ID as the key.
			//
			// Nil key → round-robin assignment → no ordering guarantee.
			customerID := fmt.Sprintf("customer-%d", messageNum%5) // 5 rotating customers

			// ── Message Payload ───────────────────────────────────────────────
			// Value is raw bytes. We use JSON here, but it could be Protobuf, Avro,
			// MessagePack, or any binary format.
			value := fmt.Sprintf(
				`{"order_id": %d, "customer_id": "%s", "amount": %.2f, "ts": "%s"}`,
				messageNum,
				customerID,
				float64(messageNum)*9.99,
				time.Now().UTC().Format(time.RFC3339),
			)

			// ── Headers ───────────────────────────────────────────────────────
			// Headers are key-value metadata separate from the payload.
			// Use headers for:
			//   - Content-type negotiation (JSON, Protobuf, etc.)
			//   - Schema version (so consumers can handle migrations)
			//   - Trace IDs (for distributed tracing without modifying payload)
			//   - Routing hints (which service should handle this event)
			msg := &sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.StringEncoder(customerID),
				Value: sarama.StringEncoder(value),
				Headers: []sarama.RecordHeader{
					{Key: []byte("content-type"), Value: []byte("application/json")},
					{Key: []byte("schema-version"), Value: []byte("1.0")},
					{Key: []byte("source"), Value: []byte("order-service")},
				},
			}

			// ── Send and Check Result ─────────────────────────────────────────
			// SendMessage blocks until:
			//   - Kafka acknowledges (based on RequiredAcks setting), OR
			//   - An error occurs after all retries are exhausted
			//
			// Returns:
			//   partition: which partition received the message (0, 1, 2, ...)
			//   offset:    its position in that partition (ever-increasing number)
			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				// This happens after all retries fail—serious broker issue.
				fmt.Printf("ERROR sending message #%d: %v\n", messageNum, err)
				continue
			}

			fmt.Printf("Message #%d → topic=%-8s partition=%d offset=%-6d key=%s\n",
				messageNum, topic, partition, offset, customerID)
		}
	}
}
