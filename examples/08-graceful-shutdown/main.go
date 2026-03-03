// Example 08 — Graceful Shutdown
//
// WHAT: Properly shut down a Kafka consumer without losing messages or
//       committing offsets for messages that weren't fully processed.
//
// WHY GRACEFUL SHUTDOWN IS CRITICAL:
//
// Without graceful shutdown:
//   1. SIGTERM sent (container stop, scale-in, deploy)
//   2. Process killed immediately
//   3. In-flight message: handler was processing it but didn't finish
//   4. Offset was auto-committed (or not committed at all)
//   5. After restart: message either lost or reprocessed incorrectly
//
// With graceful shutdown:
//   1. SIGTERM received → cancel context
//   2. Consumer group: stop accepting new messages for processing
//   3. Finish processing the current in-flight message
//   4. Commit all marked offsets
//   5. Close the consumer gracefully
//   6. Process exits cleanly
//
// SHUTDOWN PATTERNS:
//
//   1. Context cancellation (what we use here):
//      - Cancel the context passed to client.Consume()
//      - sarama stops delivering new messages after current batch
//      - Handler finishes current work, returns
//
//   2. WaitGroup for in-flight work:
//      - Track all active message handlers with a WaitGroup
//      - On shutdown: signal stop + wait for all to complete
//
//   3. Timeout-bounded shutdown:
//      - Give workers a deadline to finish
//      - After deadline: commit what's done, abandon the rest
//
// KUBERNETES NOTE:
//   k8s sends SIGTERM on pod termination. You have `terminationGracePeriodSeconds`
//   (default 30s) to clean up before SIGKILL is sent. Design your shutdown
//   to complete well within this window.
//
// HOW TO RUN:
//   make docker-up && make run-graceful
//   Press Ctrl+C to trigger graceful shutdown and observe the sequence.
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
	groupID := "graceful-shutdown-group"

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	cfg.Consumer.Offsets.AutoCommit.Enable = false
	cfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}

	client, err := sarama.NewConsumerGroup(brokers, groupID, cfg)
	if err != nil {
		log.Fatalf("Creating consumer group: %v", err)
	}

	// ── Shutdown coordination ─────────────────────────────────────────────────
	// ctx: cancelled when shutdown signal is received
	// wg:  tracks the consume loop goroutine
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	handler := &gracefulHandler{
		stopCh: ctx.Done(), // handler can check this to stop accepting new work
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := client.Consume(ctx, []string{topic}, handler); err != nil {
				if ctx.Err() != nil {
					return // Expected: context was cancelled
				}
				log.Printf("Consume error (will reconnect): %v", err)
			}
		}
	}()

	// Also start a producer for the demo
	go produceMessages(ctx, brokers, topic)

	// ── Wait for ready ────────────────────────────────────────────────────────
	select {
	case <-handler.readyCh():
		fmt.Println("[READY] Consumer group connected and consuming")
		fmt.Println("[READY] Press Ctrl+C for graceful shutdown demo\n")
	case <-time.After(15 * time.Second):
		log.Fatal("Timed out waiting for consumer to be ready")
	}

	// ── Wait for shutdown signal ──────────────────────────────────────────────
	// Listen for SIGINT (Ctrl+C) and SIGTERM (Docker/k8s stop command)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	fmt.Printf("\n[SHUTDOWN] Received signal: %v\n", sig)
	fmt.Println("[SHUTDOWN] Step 1: Cancelling context (stops new message delivery)")
	cancel()

	// ── Bounded wait ─────────────────────────────────────────────────────────
	// Give the consumer up to 30 seconds to finish current work.
	// In Kubernetes: set terminationGracePeriodSeconds = 30 or more.
	shutdownTimeout := 30 * time.Second
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("[SHUTDOWN] Step 2: All in-flight messages processed")
	case <-time.After(shutdownTimeout):
		fmt.Printf("[SHUTDOWN] Step 2: Timeout after %v, forcing close\n", shutdownTimeout)
	}

	fmt.Println("[SHUTDOWN] Step 3: Closing consumer group (commits offsets, releases connections)")
	if err := client.Close(); err != nil {
		fmt.Printf("[SHUTDOWN] Warning: client close error: %v\n", err)
	}

	fmt.Println("[SHUTDOWN] Clean shutdown complete ✓")
}

// gracefulHandler processes messages with awareness of the shutdown signal.
type gracefulHandler struct {
	stopCh <-chan struct{}
	ready  chan struct{}
	once   sync.Once
	// inFlight tracks goroutines processing messages in this session
	inFlight sync.WaitGroup
}

func (h *gracefulHandler) readyCh() <-chan struct{} {
	if h.ready == nil {
		h.ready = make(chan struct{})
	}
	return h.ready
}

func (h *gracefulHandler) Setup(session sarama.ConsumerGroupSession) error {
	fmt.Printf("[SETUP] Session started. Partitions: %v\n", session.Claims())
	if h.ready == nil {
		h.ready = make(chan struct{})
	}
	h.once.Do(func() { close(h.ready) })
	return nil
}

func (h *gracefulHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	fmt.Println("[CLEANUP] Session ending. Waiting for in-flight messages to complete...")
	// Wait for all in-flight message handlers to complete before session ends.
	// This ensures we don't commit offsets for messages we haven't finished processing.
	h.inFlight.Wait()
	fmt.Println("[CLEANUP] All in-flight messages complete. Offsets committed.")
	return nil
}

func (h *gracefulHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for msg := range claim.Messages() {
		// Check if we're shutting down BEFORE picking up a new message.
		// This prevents starting new work when we know we're stopping.
		select {
		case <-h.stopCh:
			fmt.Printf("[GRACEFUL] Shutdown in progress, not picking up offset=%d\n", msg.Offset)
			// Don't mark the message: it will be redelivered after restart.
			return nil
		default:
		}

		// Track this message as in-flight
		h.inFlight.Add(1)
		h.processMessage(session, msg)
	}
	return nil
}

func (h *gracefulHandler) processMessage(session sarama.ConsumerGroupSession, msg *sarama.ConsumerMessage) {
	defer h.inFlight.Done()

	// Simulate work that takes 500ms (database write, API call, etc.)
	workDuration := 500 * time.Millisecond

	fmt.Printf("[PROCESSING] offset=%-6d key=%-12s (will take %v)\n",
		msg.Offset, string(msg.Key), workDuration)

	select {
	case <-time.After(workDuration):
		// Work completed normally
		session.MarkMessage(msg, "")
		fmt.Printf("[DONE]       offset=%-6d committed\n", msg.Offset)

	case <-h.stopCh:
		// Shutdown requested during processing.
		// We have two options:
		//   a) Abandon: don't mark, message redelivered after restart
		//   b) Finish: complete the current work despite shutdown signal
		// Here we finish the current message (bounded by the 500ms work itself).
		// In production, your choice depends on whether partial work is safe to abandon.
		<-time.After(workDuration - 100*time.Millisecond) // let most of the work finish
		session.MarkMessage(msg, "")
		fmt.Printf("[DONE]       offset=%-6d committed (during shutdown)\n", msg.Offset)
	}
}

func produceMessages(ctx context.Context, brokers []string, topic string) {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V3_6_0_0
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Return.Successes = true

	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return
	}
	defer p.Close()

	time.Sleep(3 * time.Second) // Let consumer start

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	i := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			i++
			p.SendMessage(&sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.StringEncoder(fmt.Sprintf("key-%d", i%3)),
				Value: sarama.StringEncoder(fmt.Sprintf(`{"seq":%d}`, i)),
			})
		}
	}
}
