// Example 10 — Kafka Transactions (Atomic Multi-Topic Writes)
//
// WHAT: Write to multiple topics atomically—either all writes succeed or none do.
//
// WHY TRANSACTIONS?
//
// Problem: In a distributed system, writing to multiple topics in sequence
//          can result in partial writes if a crash occurs mid-way.
//
//   Without transactions:
//     Producer writes to "orders" topic    ← success
//     Producer writes to "inventory" topic ← CRASH
//     Result: Order created, inventory not decremented. Data inconsistency.
//
//   With transactions:
//     BEGIN transaction
//     Write to "orders" topic (buffered, invisible to read_committed consumers)
//     Write to "inventory" topic (buffered)
//     COMMIT transaction → both writes become visible atomically
//     OR ABORT → both writes are rolled back
//
// KAFKA TRANSACTIONS vs DATABASE TRANSACTIONS:
//   - Kafka transactions are "atomic visibility", not full ACID
//   - They don't lock resources—multiple producers can write concurrently
//   - They guarantee: either ALL topic-writes in a transaction are visible, or NONE
//
// USE CASES FOR KAFKA TRANSACTIONS:
//   - Fan-out: write the same event to 3 topics atomically
//   - Saga pattern: emit events for each step of a business transaction
//   - Consume-Transform-Produce (see Example 09 for full EOS pattern)
//
// HOW TO RUN:
//   make docker-up && make run-transactions
package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	brokers := []string{"localhost:9092"}

	// ── Transactional Producer Configuration ─────────────────────────────────
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0

	// All three settings are required for transactions:
	cfg.Producer.Idempotent = true
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true

	// Transaction.ID uniquely identifies this producer as a transactional source.
	// The broker fences out old instances with the same ID (zombie fencing).
	// In production: this should be stable across restarts (e.g., from config).
	cfg.Producer.Transaction.ID = "order-fulfillment-txn-producer"
	cfg.Producer.Transaction.Timeout = 60 * time.Second

	producer, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		log.Fatalf("Creating transactional producer: %v", err)
	}
	defer producer.Close()

	fmt.Println("Transactional Producer ready.")
	fmt.Println("Demonstrating atomic multi-topic writes...\n")

	// ── Scenario 1: Successful Transaction (all writes committed) ─────────────
	fmt.Println("═══ Scenario 1: Successful atomic write to 3 topics ═══")
	if err := placeOrder(producer, 1001, "customer-A", 299.99); err != nil {
		log.Printf("Order 1001 failed: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	// ── Scenario 2: Aborted Transaction (simulated failure) ───────────────────
	fmt.Println("\n═══ Scenario 2: Aborted transaction (simulated failure) ═══")
	if err := placeOrderWithFailure(producer, 1002, "customer-B", 149.99); err != nil {
		fmt.Printf("Order 1002 aborted: %v (no partial writes in any topic)\n", err)
	}

	time.Sleep(500 * time.Millisecond)

	// ── Scenario 3: Another successful transaction ────────────────────────────
	fmt.Println("\n═══ Scenario 3: Second successful transaction ═══")
	if err := placeOrder(producer, 1003, "customer-C", 79.99); err != nil {
		log.Printf("Order 1003 failed: %v", err)
	}

	fmt.Println("\n─────────────────────────────────────────────────────────────")
	fmt.Println("Results:")
	fmt.Println("  Orders 1001 and 1003: visible in ALL three topics (committed)")
	fmt.Println("  Order  1002: NOT visible in any topic (aborted)")
	fmt.Println()
	fmt.Println("Verify in Kafka UI (http://localhost:8080):")
	fmt.Println("  - Topics: orders, payments, notifications")
	fmt.Println("  - orders/1001 and 1003 exist; 1002 does not")
	fmt.Println("─────────────────────────────────────────────────────────────")
}

// placeOrder atomically writes to orders, payments, and notifications topics.
// This simulates a "place order" business operation that must touch multiple topics.
func placeOrder(producer sarama.SyncProducer, orderID int, customerID string, amount float64) error {
	ts := time.Now().UTC().Format(time.RFC3339)

	// ── Begin Transaction ─────────────────────────────────────────────────────
	if err := producer.BeginTxn(); err != nil {
		return fmt.Errorf("BeginTxn: %w", err)
	}
	fmt.Printf("[TXN BEGIN] order_id=%d\n", orderID)

	// ── Write 1: Orders topic ─────────────────────────────────────────────────
	orderPayload := fmt.Sprintf(
		`{"order_id":%d,"customer_id":"%s","amount":%.2f,"status":"placed","ts":"%s"}`,
		orderID, customerID, amount, ts)

	if _, _, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: "orders",
		Key:   sarama.StringEncoder(fmt.Sprintf("order-%d", orderID)),
		Value: sarama.StringEncoder(orderPayload),
	}); err != nil {
		_ = producer.AbortTxn()
		return fmt.Errorf("writing to orders: %w", err)
	}
	fmt.Printf("[TXN WRITE] orders ← order_id=%d amount=%.2f\n", orderID, amount)

	// ── Write 2: Payments topic ───────────────────────────────────────────────
	paymentPayload := fmt.Sprintf(
		`{"payment_id":"pay-%d","order_id":%d,"amount":%.2f,"status":"pending","ts":"%s"}`,
		orderID, orderID, amount, ts)

	if _, _, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: "payments",
		Key:   sarama.StringEncoder(fmt.Sprintf("pay-%d", orderID)),
		Value: sarama.StringEncoder(paymentPayload),
	}); err != nil {
		_ = producer.AbortTxn()
		return fmt.Errorf("writing to payments: %w", err)
	}
	fmt.Printf("[TXN WRITE] payments ← payment_id=pay-%d amount=%.2f\n", orderID, amount)

	// ── Write 3: Notifications topic ─────────────────────────────────────────
	notifPayload := fmt.Sprintf(
		`{"type":"order_placed","recipient":"%s","order_id":%d,"message":"Your order #%d is confirmed!","ts":"%s"}`,
		customerID, orderID, orderID, ts)

	if _, _, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic: "notifications",
		Key:   sarama.StringEncoder(customerID),
		Value: sarama.StringEncoder(notifPayload),
	}); err != nil {
		_ = producer.AbortTxn()
		return fmt.Errorf("writing to notifications: %w", err)
	}
	fmt.Printf("[TXN WRITE] notifications ← recipient=%s\n", customerID)

	// ── Commit Transaction ────────────────────────────────────────────────────
	// All 3 writes become visible atomically.
	// read_committed consumers won't see any of them until this commit.
	if err := producer.CommitTxn(); err != nil {
		_ = producer.AbortTxn()
		return fmt.Errorf("CommitTxn: %w", err)
	}
	fmt.Printf("[TXN COMMIT] order_id=%d → all 3 topics updated atomically ✓\n", orderID)
	return nil
}

// placeOrderWithFailure simulates a transaction that encounters an error mid-way
// and aborts the whole transaction cleanly.
func placeOrderWithFailure(producer sarama.SyncProducer, orderID int, customerID string, amount float64) error {
	ts := time.Now().UTC().Format(time.RFC3339)

	if err := producer.BeginTxn(); err != nil {
		return fmt.Errorf("BeginTxn: %w", err)
	}
	fmt.Printf("[TXN BEGIN] order_id=%d\n", orderID)

	// Write to orders (success)
	orderPayload := fmt.Sprintf(`{"order_id":%d,"customer_id":"%s","amount":%.2f,"ts":"%s"}`,
		orderID, customerID, amount, ts)
	producer.SendMessage(&sarama.ProducerMessage{
		Topic: "orders",
		Key:   sarama.StringEncoder(fmt.Sprintf("order-%d", orderID)),
		Value: sarama.StringEncoder(orderPayload),
	})
	fmt.Printf("[TXN WRITE] orders ← order_id=%d (buffered, not yet visible)\n", orderID)

	// Simulate: payment validation fails
	simulatedError := fmt.Errorf("payment gateway rejected customer %q: insufficient funds", customerID)
	fmt.Printf("[TXN ERROR] %v\n", simulatedError)

	// ABORT: rolls back all writes in this transaction.
	// The order written above will NOT be visible to consumers.
	if err := producer.AbortTxn(); err != nil {
		return fmt.Errorf("AbortTxn: %w", err)
	}
	fmt.Printf("[TXN ABORT] order_id=%d → all writes rolled back\n", orderID)

	return simulatedError
}
