// Example 03 — Offset Management
//
// WHAT: Manually control Kafka consumer offsets.
//
// WHY OFFSET MANAGEMENT MATTERS:
//
// An offset is a message's sequential position within a partition.
// The consumer group commits (saves) its current offset to Kafka so that
// after a restart, it can resume from where it left off—not from the beginning.
//
//   Partition 0:  [0][1][2][3][4][5][6][7][8][9]
//                              ↑
//                     committed offset = 4
//                     (messages 0-3 were processed)
//
// OFFSET COMMIT STRATEGIES:
//
//   Auto-commit (cfg.Consumer.Offsets.AutoCommit.Enable = true):
//     Kafka commits on a timer (default: 5 seconds).
//     RISK: commit happens BEFORE processing finishes → messages can be lost.
//
//   Manual commit after processing:
//     You call session.MarkMessage() after successful processing.
//     sarama batches marks and commits them on the next session heartbeat.
//     GUARANTEES: "at-least-once"—every message is processed at least once.
//     You must handle duplicate processing (make your handler idempotent).
//
//   Manual commit with explicit session.Commit():
//     Immediately commit all marked offsets. Use when you need strong checkpointing.
//
// SPECIAL OFFSETS:
//   sarama.OffsetNewest (-1): latest message (default for new groups)
//   sarama.OffsetOldest (-2): very first message ever
//   specific int64:           resume from that exact position
//
// LEARNING EXERCISE:
//   1. Run the producer to create messages
//   2. Run this consumer (it commits offsets after each message)
//   3. Stop and restart the consumer → it resumes from where it stopped
//   4. Change KAFKA_CONSUMER_OFFSET_INITIAL=oldest → it replays from the beginning
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

func main() {
	brokers := []string{"localhost:9092"}
	topic := "orders"
	groupID := "offset-demo-group"

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	cfg.Consumer.Return.Errors = true

	// ── Manual offset management ──────────────────────────────────────────────
	// Disable auto-commit so we have full control over when offsets advance.
	cfg.Consumer.Offsets.AutoCommit.Enable = false

	// OffsetOldest: when this group has no committed offset, start from the very
	// beginning. This lets us replay all messages for the demo.
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}

	client, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		log.Fatalf("Creating consumer group: %v", err)
	}
	defer client.Close()

	handler := &offsetDemoHandler{
		commitAfterN: 3, // Commit offsets after every 3 messages (micro-batching)
		ready:        make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, []string{topic}, handler); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("Consumer error: %v", err)
			}
		}
	}()

	<-handler.ready
	fmt.Println("Consumer ready. Demonstrating offset management...")
	fmt.Println("Messages are committed in batches of 3.")
	fmt.Println("Stop and restart to see resume-from-committed-offset behavior.\n")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	cancel()
	wg.Wait()
}

// offsetDemoHandler demonstrates different offset commit strategies.
type offsetDemoHandler struct {
	ready        chan struct{}
	once         sync.Once
	commitAfterN int // Commit every N messages (micro-batching)
	count        int
}

func (h *offsetDemoHandler) Setup(s sarama.ConsumerGroupSession) error {
	h.once.Do(func() {
		if h.ready == nil {
			h.ready = make(chan struct{})
		}
		close(h.ready)
	})
	fmt.Printf("Session setup. Assigned: %v\n", s.Claims())
	return nil
}

func (h *offsetDemoHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	fmt.Println("Session cleanup.")
	return nil
}

func (h *offsetDemoHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	fmt.Printf("Consuming partition=%d from offset=%d\n",
		claim.Partition(), claim.InitialOffset())

	for msg := range claim.Messages() {
		h.count++

		// Simulate processing
		err := processWithOffset(msg)
		if err != nil {
			// Don't mark on error. The message will be redelivered
			// after the next rebalance (at-least-once semantics).
			fmt.Printf("  [SKIP] offset=%d error: %v\n", msg.Offset, err)
			continue
		}

		// Mark the message: tell sarama this offset should be committed.
		// NOTE: MarkMessage does NOT immediately commit to Kafka.
		// It stages the offset for the next commit flush.
		session.MarkMessage(msg, "")

		// ── Strategy: Commit every N messages ────────────────────────────────
		// Instead of committing after every single message (slow) or only on
		// session end (risky), we commit in micro-batches.
		//
		// Trade-off: with commitAfterN=3, we might reprocess up to 2 messages
		// after a crash. Larger batches = more throughput, more reprocessing risk.
		if h.count%h.commitAfterN == 0 {
			// session.Commit() immediately flushes all marked offsets to Kafka.
			session.Commit()
			fmt.Printf("  [COMMIT] Flushed offsets after %d messages\n", h.count)
		}
	}

	return nil
}

func processWithOffset(msg *sarama.ConsumerMessage) error {
	fmt.Printf("  [%s] partition=%-2d offset=%-6d key=%-12s\n",
		time.Now().Format("15:04:05"),
		msg.Partition,
		msg.Offset,
		string(msg.Key),
	)
	// In real code: parse the message, call business logic, update database, etc.
	return nil
}


