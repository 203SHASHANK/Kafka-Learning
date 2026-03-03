package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	"kafka-learning/internal/config"
)

// ConsumerGroup wraps sarama's consumer group with production-ready behavior:
//   - Automatic reconnection on network errors
//   - Structured logging for setup/cleanup/rebalance events
//   - Clean shutdown via context cancellation
//   - Readiness signaling (useful for health checks and tests)
//
// IMPORTANT CONCEPT: Consumer group lifecycle
//   Each call to client.Consume() runs for ONE session—from partition assignment
//   to the next rebalance. When any consumer in the group joins or leaves,
//   Kafka triggers a rebalance, which ends the current session and starts a new one.
//   This is why we loop: for { client.Consume(...) }
type ConsumerGroup struct {
	client  sarama.ConsumerGroup
	logger  *zap.Logger
	cfg     *config.Config
	topics  []string
	handler ProcessorFunc
	ready   chan struct{}
}

// ProcessorFunc is the callback for processing a single consumed message.
//
// Design contract:
//   - Return nil to mark the message as successfully processed (offset advances)
//   - Return non-nil to leave the offset uncommitted (message re-delivered after rebalance)
//   - Must be safe to call concurrently (one goroutine per partition)
//   - Must check ctx.Err() before long-running work
type ProcessorFunc func(ctx context.Context, msg *Message) error

// NewConsumerGroup creates a Kafka consumer group.
func NewConsumerGroup(cfg *config.Config, topics []string, fn ProcessorFunc) (*ConsumerGroup, error) {
	saramaCfg := buildConsumerGroupConfig(cfg)

	client, err := sarama.NewConsumerGroup(cfg.Kafka.Brokers, cfg.Kafka.ConsumerGroupID, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("creating consumer group %q: %w", cfg.Kafka.ConsumerGroupID, err)
	}

	log := zap.L().With(
		zap.String("component", "consumer-group"),
		zap.String("group_id", cfg.Kafka.ConsumerGroupID),
		zap.Strings("topics", topics),
	)
	log.Info("consumer group client created")

	return &ConsumerGroup{
		client:  client,
		logger:  log,
		cfg:     cfg,
		topics:  topics,
		handler: fn,
		ready:   make(chan struct{}),
	}, nil
}

// Start begins the consume loop. Blocks until ctx is cancelled.
//
// The consume loop handles rebalances transparently: each session ends on
// rebalance and a new one begins immediately. The processor function is called
// for each message in each session.
func (cg *ConsumerGroup) Start(ctx context.Context) error {
	h := &groupHandler{
		logger:  cg.logger,
		process: cg.handler,
		ready:   cg.ready,
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// Each call to Consume runs one session (until next rebalance or ctx cancel).
			if err := cg.client.Consume(ctx, cg.topics, h); err != nil {
				if ctx.Err() != nil {
					return // Normal shutdown
				}
				cg.logger.Error("consumer group error, reconnecting", zap.Error(err))
			}

			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Block until the first session setup completes (partitions assigned).
	select {
	case <-cg.ready:
		cg.logger.Info("consumer group ready")
	case <-ctx.Done():
		// Context cancelled before we were ready (e.g., fast shutdown in tests)
	}

	<-ctx.Done()
	cg.logger.Info("shutting down consumer group")
	wg.Wait()
	return cg.client.Close()
}

// Ready returns a channel closed when the consumer group has assigned partitions.
func (cg *ConsumerGroup) Ready() <-chan struct{} {
	return cg.ready
}

// ─── groupHandler ─────────────────────────────────────────────────────────────

// groupHandler implements sarama.ConsumerGroupHandler.
// This is sarama's callback interface for consumer group lifecycle events.
type groupHandler struct {
	logger  *zap.Logger
	process ProcessorFunc
	ready   chan struct{}
	once    sync.Once
}

// Setup is called at the start of each session, after partition assignment.
// Use it to initialize per-session state (e.g., database connections, buffers).
func (h *groupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.logger.Info("consumer group session starting",
		zap.String("member_id", session.MemberID()),
		zap.Int32("generation_id", session.GenerationID()),
		zap.Any("partitions", session.Claims()),
	)
	// Signal readiness once. sync.Once ensures this fires only on the first Setup call.
	h.once.Do(func() { close(h.ready) })
	return nil
}

// Cleanup is called at the end of each session, before rebalance.
// Use it to flush buffers, commit any pending state, or log end-of-session stats.
func (h *groupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	h.logger.Info("consumer group session ending",
		zap.String("member_id", session.MemberID()),
	)
	return nil
}

// ConsumeClaim runs in its own goroutine for each assigned partition.
// Kafka guarantees: one ConsumeClaim per partition per session.
// This means partition ordering is preserved—messages from partition 0 are
// processed sequentially because only one goroutine handles partition 0.
func (h *groupHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	h.logger.Info("consuming partition",
		zap.String("topic", claim.Topic()),
		zap.Int32("partition", claim.Partition()),
		zap.Int64("initial_offset", claim.InitialOffset()),
	)

	// claim.Messages() is closed when the session ends (rebalance or shutdown).
	for msg := range claim.Messages() {
		domainMsg := fromSaramaMessage(msg)

		if err := h.process(session.Context(), domainMsg); err != nil {
			h.logger.Error("processing failed",
				zap.String("topic", msg.Topic),
				zap.Int32("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Error(err),
			)
			// Do NOT mark the message. It will be redelivered after the next rebalance.
			// In production: implement retry-with-backoff here, then send to DLQ.
			continue
		}

		// MarkMessage advances the committed offset for this partition.
		// Without this, the consumer would re-read all messages after restart.
		// The "" metadata argument can store arbitrary string state if needed.
		session.MarkMessage(msg, "")
	}

	return nil
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

func buildConsumerGroupConfig(cfg *config.Config) *sarama.Config {
	c := sarama.NewConfig()
	c.Version = cfg.Kafka.Version

	// Disable auto-commit: we commit manually after successful processing.
	c.Consumer.Offsets.AutoCommit.Enable = cfg.Kafka.Consumer.AutoCommit
	c.Consumer.Offsets.Initial = cfg.Kafka.Consumer.InitialOffset

	c.Consumer.Group.Session.Timeout = cfg.Kafka.Consumer.SessionTimeout
	c.Consumer.Group.Heartbeat.Interval = cfg.Kafka.Consumer.HeartbeatInterval

	// RoundRobin distributes partitions evenly.
	// Alternative: Sticky (sarama.NewBalanceStrategySticky()) minimizes partition
	// movement on rebalance, improving cache locality in stateful consumers.
	c.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
		sarama.NewBalanceStrategyRoundRobin(),
	}

	c.Consumer.Return.Errors = true

	return c
}

// fromSaramaMessage converts a sarama ConsumerMessage to our domain Message.
func fromSaramaMessage(msg *sarama.ConsumerMessage) *Message {
	headers := make(map[string][]byte, len(msg.Headers))
	for _, h := range msg.Headers {
		headers[string(h.Key)] = h.Value
	}

	return &Message{
		Topic:     msg.Topic,
		Key:       msg.Key,
		Value:     msg.Value,
		Headers:   headers,
		Partition: msg.Partition,
		Offset:    msg.Offset,
		Timestamp: msg.Timestamp,
	}
}
