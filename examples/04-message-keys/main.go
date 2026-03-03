// Example 04 — Message Keys and Partition Routing
//
// WHAT: Demonstrate how message keys control partition assignment and ordering.
//
// WHY KEYS MATTER:
//
// Kafka's ordering guarantee is: messages within a PARTITION are ordered.
// But across partitions, there is no ordering guarantee.
//
// Without keys (round-robin assignment):
//   Order A: $100 → partition 0
//   Order A: $200 → partition 1   ← different partition, unordered!
//   Order A: -$50 → partition 2   ← your refund might process before the purchase
//
// With keys (deterministic partition assignment):
//   hash("order-A") % 3 = 0  → partition 0, always
//   hash("order-B") % 3 = 1  → partition 1, always
//
//   All events for "order-A" go to partition 0 → they are always ordered.
//
// REAL-WORLD KEY STRATEGIES:
//
//   Customer ID:     All events for a customer are ordered. Good for account updates.
//   Order ID:        All events for an order are ordered. Good for order state machines.
//   Device ID:       All events from an IoT device are ordered.
//   Geographic shard: Route by region for locality.
//   nil key:         Round-robin for maximum throughput when ordering doesn't matter.
//
// KEY DISTRIBUTION: Choose keys with high cardinality and even distribution.
// If one key is extremely hot (e.g., "system" as key for all admin events),
// that partition becomes a bottleneck (hot partition problem).
//
// HOW TO RUN:
//   make docker-up
//   make run-keys
// Then check Kafka UI → Topic → orders → see how same keys → same partition.
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

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Compression = sarama.CompressionSnappy

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("Creating producer: %v", err)
	}
	defer producer.Close()

	// ── Demonstrate key-based partition routing ───────────────────────────────
	// We'll send messages for 3 different orders and show they always
	// go to the same partition (deterministic routing by key).
	orders := []struct {
		orderID string
		events  []string
	}{
		{
			orderID: "order-alpha",
			events:  []string{"placed", "payment-pending", "payment-confirmed", "shipped", "delivered"},
		},
		{
			orderID: "order-beta",
			events:  []string{"placed", "payment-failed", "payment-retried", "payment-confirmed", "shipped"},
		},
		{
			orderID: "order-gamma",
			events:  []string{"placed", "cancelled"},
		},
	}

	fmt.Println("Sending order events with keys for deterministic partition routing...")
	fmt.Println("Observe: same order ID always routes to the same partition.\n")

	// Track which partition each order lands on (for verification)
	orderPartitions := make(map[string]int32)

	for _, order := range orders {
		fmt.Printf("Order: %s\n", order.orderID)
		for _, event := range order.events {
			payload := fmt.Sprintf(`{"order_id":"%s","event":"%s","ts":"%s"}`,
				order.orderID, event, time.Now().UTC().Format(time.RFC3339))

			msg := &sarama.ProducerMessage{
				Topic: topic,
				// KEY: the order ID. All events for this order → same partition → ordered.
				// Kafka hashes this key using murmur2 algorithm.
				Key:   sarama.StringEncoder(order.orderID),
				Value: sarama.StringEncoder(payload),
				Headers: []sarama.RecordHeader{
					{Key: []byte("event-type"), Value: []byte(event)},
				},
			}

			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				fmt.Printf("  ERROR: %v\n", err)
				continue
			}

			// Verify: is this order always on the same partition?
			if prev, seen := orderPartitions[order.orderID]; seen && prev != partition {
				fmt.Printf("  ⚠ UNEXPECTED: %s went to partition %d (was %d)\n",
					order.orderID, partition, prev)
			}
			orderPartitions[order.orderID] = partition

			fmt.Printf("  [%s] partition=%-2d offset=%-6d event=%s\n",
				order.orderID, partition, offset, event)

			time.Sleep(50 * time.Millisecond) // slow down for readability
		}
		fmt.Println()
	}

	// ── Summary ───────────────────────────────────────────────────────────────
	fmt.Println("─── Partition Routing Summary ───────────────────────────────────")
	fmt.Println("Key-based routing guarantees each order's events are processed in")
	fmt.Println("the same order they were produced, regardless of consumer restarts.")
	fmt.Println()
	for orderID, partition := range orderPartitions {
		fmt.Printf("  %-16s → always routed to partition %d\n", orderID, partition)
	}

	// ── Contrast: nil key (round-robin) ──────────────────────────────────────
	fmt.Println("\n─── Round-Robin (nil key) ───────────────────────────────────────")
	fmt.Println("No ordering guarantee across partitions.")
	for i := 0; i < 6; i++ {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   nil, // nil key = round-robin
			Value: sarama.StringEncoder(fmt.Sprintf(`{"type":"metric","value":%d}`, i)),
		}
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			continue
		}
		fmt.Printf("  nil key → partition=%d offset=%d\n", partition, offset)
		time.Sleep(50 * time.Millisecond)
	}
}
