// Example 02 — Consumer Groups
//
// WHAT: Use a consumer group to horizontally scale message consumption.
//
// WHY CONSUMER GROUPS?
//
// Problem: A single consumer reading a topic with 6 partitions can only
// process ~6x the partition throughput. If the producer is faster, lag builds up.
//
// Solution: Consumer groups. When multiple consumers share the same Group ID,
// Kafka distributes (assigns) partitions across them:
//
//   Topic: orders (6 partitions: P0, P1, P2, P3, P4, P5)
//
//   1 consumer in group → owns all 6 partitions
//   2 consumers in group → each owns 3 partitions
//   3 consumers in group → each owns 2 partitions
//   6 consumers in group → each owns 1 partition  ← maximum parallelism
//   7 consumers in group → 1 consumer is idle (no partitions left)
//
// KEY PROPERTIES:
//   - Each partition is owned by exactly ONE consumer per group
//   - Messages within a partition are always processed IN ORDER
//   - Cross-partition ordering is not guaranteed (and not needed for most use cases)
//   - If a consumer crashes, its partitions are reassigned to surviving consumers (REBALANCE)
//
// REBALANCE: The process of redistributing partitions is called a rebalance.
// It's triggered when:
//   - A consumer joins the group (scale out)
//   - A consumer leaves or crashes (scale in / failure)
//   - A topic partition count changes
//
// During rebalance, consumption pauses briefly. This is why heartbeats are important:
// a slow consumer that misses heartbeats triggers an unnecessary rebalance.
//
// TRY IT: Run multiple instances of this program and watch the partition assignments:
//   Terminal 1: APP_INSTANCE=1 go run examples/02-consumer-groups/main.go
//   Terminal 2: APP_INSTANCE=2 go run examples/02-consumer-groups/main.go
//   Terminal 3: go run examples/01-basics/producer/main.go
//
// You'll see the partitions split between consumers, and rebalancing when one exits.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

const (
	brokerAddr  = "localhost:9092"
	topicName   = "orders"
	groupID     = "order-processor-group"
)

func main() {
	// Read an optional instance identifier (for demo multi-instance output).
	instance := os.Getenv("APP_INSTANCE")
	if instance == "" {
		instance = "1"
	}

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0

	// AutoCommit = false: we commit offsets MANUALLY after successful processing.
	//
	// WHY manual commit?
	// With auto-commit, offsets advance on a timer, regardless of whether your
	// handler actually processed the message. If your app crashes mid-processing,
	// those messages are lost even though the offset was already committed.
	//
	// Manual commit = "at-least-once delivery":
	//   Kafka may deliver the same message twice (after rebalance / crash recovery),
	//   but every message is guaranteed to be delivered at least once.
	//   Your handler should be idempotent to handle duplicates safely.
	cfg.Consumer.Offsets.AutoCommit.Enable = false
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest

	// Session timeout: Kafka waits this long for a heartbeat before declaring the consumer dead.
	cfg.Consumer.Group.Session.Timeout = 10 * time.Second
	// Heartbeat interval: should be ~1/3 of session timeout.
	cfg.Consumer.Group.Heartbeat.Interval = 3 * time.Second

	// Balance strategy: how partitions are distributed across consumers.
	// RoundRobin: distributes partitions evenly across consumers.
	// Sticky:     minimizes partition movement on rebalance (better for stateful consumers).
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}

	// ── Create Consumer Group Client ──────────────────────────────────────────
	client, err := sarama.NewConsumerGroup([]string{brokerAddr}, groupID, cfg)
	if err != nil {
		log.Fatalf("Creating consumer group: %v", err)
	}
	defer client.Close()

	fmt.Printf("[Instance %s] Consumer group %q joined\n", instance, groupID)
	fmt.Printf("[Instance %s] Waiting for partition assignment...\n\n", instance)

	// ── Handler ───────────────────────────────────────────────────────────────
	handler := &OrderHandler{instance: instance, ready: make(chan struct{})}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// Each call to Consume() runs for ONE session (until rebalance).
			// We loop because a new session begins after each rebalance.
			// This is the standard sarama consumer group pattern.
			if err := client.Consume(ctx, []string{topicName}, handler); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Consume error: %v", err)
			}
		}
	}()

	// Wait until Setup() is called (partitions assigned) before printing ready message.
	<-handler.ready
	fmt.Printf("[Instance %s] READY: assigned partitions → consuming\n\n", instance)

	// Graceful shutdown on Ctrl+C
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	fmt.Printf("\n[Instance %s] Shutting down...\n", instance)
	cancel()
	wg.Wait()
	fmt.Printf("[Instance %s] Clean shutdown complete\n", instance)
}

// ── OrderHandler implements sarama.ConsumerGroupHandler ──────────────────────
//
// sarama calls the three methods below at specific lifecycle points.
// You MUST implement all three.
type OrderHandler struct {
	instance string
	ready    chan struct{}
	once     sync.Once
}

func NewOrderHandler(instance string) *OrderHandler {
	return &OrderHandler{
		instance: instance,
		ready:    make(chan struct{}),
	}
}

// Setup is called once per session, BEFORE ConsumeClaim begins.
// This is where you'd initialize caches, database connections, etc.
func (h *OrderHandler) Setup(session sarama.ConsumerGroupSession) error {
	fmt.Printf("[Instance %s] ── Rebalance: partitions assigned ──\n", h.instance)
	for topic, partitions := range session.Claims() {
		fmt.Printf("[Instance %s]   topic=%s  partitions=%v\n", h.instance, topic, partitions)
	}
	fmt.Println()

	// Signal that we're ready to consume.
	h.once.Do(func() { close(h.ready) })
	return nil
}

// Cleanup is called once per session, AFTER ConsumeClaim ends.
// This is where you'd flush buffers, log metrics, etc.
func (h *OrderHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Printf("[Instance %s] ── Rebalance: session ending ──\n\n", h.instance)
	return nil
}

// ConsumeClaim is called for each partition this consumer is assigned.
// It runs in its own goroutine—safe to do I/O here without blocking other partitions.
//
// IMPORTANT: Return from ConsumeClaim ONLY when claim.Messages() is closed.
// Returning early will cause the session to end prematurely and trigger a rebalance.
func (h *OrderHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	fmt.Printf("[Instance %s] Started consuming topic=%s partition=%d\n",
		h.instance, claim.Topic(), claim.Partition())

	for msg := range claim.Messages() {
		// Simulate processing time (replace with your actual business logic)
		processOrder(h.instance, msg)

		// MarkMessage tells sarama to include this offset in the next commit.
		// The commit happens either:
		//   - Automatically (if AutoCommit = true, on a timer)
		//   - On session end (rebalance / cleanup)
		//   - When you call session.Commit() explicitly
		//
		// The "" metadata can store arbitrary state (e.g., processing result).
		session.MarkMessage(msg, "processed")
	}

	return nil
}

func processOrder(instance string, msg *sarama.ConsumerMessage) {
	fmt.Printf("[Instance %s] partition=%-2d offset=%-6d key=%-12s value=%s\n",
		instance,
		msg.Partition,
		msg.Offset,
		string(msg.Key),
		string(msg.Value),
	)
}
