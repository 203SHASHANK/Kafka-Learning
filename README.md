# Kafka for Go Developers — From Zero to Production

A complete, production-grade learning repository for Apache Kafka with Go.
Every concept is explained with **working code**, **real configuration**, and
**clear documentation** of the WHY, not just the WHAT.

---

## Who This Is For

You are a Go developer who wants to:
- Understand how Kafka works at a conceptual and practical level
- Write production-quality Kafka producers and consumers
- Learn patterns like DLQ, exactly-once semantics, and transactional writes
- Have a reference codebase for real-world Kafka integrations

---

## What is Kafka?

Apache Kafka is a **distributed event streaming platform** originally developed at LinkedIn and open-sourced in 2011. It is now maintained by the Apache Software Foundation. Think of it as a high-performance, durable, fault-tolerant message bus for your microservices — but one that also acts as a **persistent, replayable log**.

The core insight behind Kafka's design: instead of deleting a message after it is consumed (like RabbitMQ or SQS), Kafka **appends messages to an immutable, ordered log on disk** and retains them for a configurable period. Consumers track their own position (offset) in that log. This means:

- A consumer can **replay history** from any point — useful for rebuilding state, backfilling new services, or debugging production incidents
- Multiple independent consumer groups each maintain their own offset — they don't interfere with each other
- A slow consumer doesn't block a fast one
- Kafka brokers are stateless with respect to consumer progress — consumers own their offsets

Unlike traditional queues (RabbitMQ, SQS):
- Messages are **retained on disk** (for hours, days, or forever) — consumers can replay history
- A topic is split into **partitions** — enabling horizontal scaling and true parallelism
- Multiple independent consumer groups can each read the same topic — fully decoupled consumers
- Kafka handles **millions of messages per second** — designed for throughput, not just latency
- The broker is dumb, the client is smart — Kafka pushes complexity to producers/consumers, keeping the broker fast

**When should you use Kafka?**
| Use Case | Good Fit? | Why |
|---|---|---|
| Async communication between microservices | ✅ | Decouples services; producer doesn't wait for consumer |
| Event sourcing and audit logs | ✅ | Immutable log is a natural fit; replay to rebuild state |
| Real-time data pipelines | ✅ | High throughput, low latency, fan-out to multiple consumers |
| High-throughput metrics and telemetry | ✅ | Handles millions of events/sec with compression |
| Simple task queues with one consumer | ❌ (use Redis/SQS) | Kafka's overhead isn't justified for simple work queues |
| RPC / request-response patterns | ❌ (use gRPC) | Kafka is fire-and-forget; correlation is complex |

---

## Architecture Overview

```
                    ┌────────────────────────────────────────────────┐
                    │              Kafka Cluster                       │
                    │                                                  │
  ┌──────────┐      │   Topic: "orders"  (3 partitions)               │      ┌──────────────────┐
  │ Producer │──────▶   P0: [0][1][2][3][4]                           ├──────▶ Consumer Group A │
  │(Go app)  │      │   P1: [0][1][2][3]                              │      │ (fulfillment)    │
  └──────────┘      │   P2: [0][1][2][3][4][5]                        │      └──────────────────┘
                    │                                                  │
  ┌──────────┐      │   Topic: "payments" (3 partitions)              │      ┌──────────────────┐
  │ Producer │──────▶   P0: [0][1]                                    ├──────▶ Consumer Group B │
  │(Go app)  │      │   P1: [0][1][2]                                 │      │ (analytics)      │
  └──────────┘      │   P2: [0]                                       │      └──────────────────┘
                    │                                                  │
                    └────────────────────────────────────────────────┘
```

**Key concepts at a glance:**
- **Broker**: A Kafka server (JVM process) that stores partition data on disk and serves producer/consumer requests. A cluster has 1–N brokers. One broker per partition is elected **leader** — all reads and writes go through the leader.
- **Topic**: A named, append-only stream of records. Analogous to a database table, but immutable and ordered. Topics are split into partitions for scalability.
- **Partition**: The unit of parallelism. A topic with N partitions can be consumed by up to N consumers in parallel. Each partition is an ordered, immutable sequence of records. Ordering is guaranteed **within** a partition, not across partitions.
- **Offset**: A message's unique, monotonically increasing integer position within a partition. Offset 0 is the first message ever written. Consumers commit offsets to track progress — on restart, they resume from the last committed offset.
- **Replica**: A copy of a partition stored on a different broker for fault tolerance. One replica is the **leader** (serves traffic); others are **followers** (replicate from leader). The set of in-sync followers is the **ISR (In-Sync Replicas)**.
- **Producer**: A client that writes (appends) messages to a topic. The producer decides which partition to write to — by round-robin, by message key hash, or by custom partitioner.
- **Consumer**: A client that reads messages from a topic by polling. Consumers track their own offset per partition. They never "receive" messages — they **pull** them.
- **Consumer Group**: A named group of consumer instances that collectively consume a topic. Kafka assigns each partition to exactly one consumer in the group — no two consumers in the same group read the same partition simultaneously. This is how Kafka achieves parallel consumption without duplicate processing.

---

## Learning Path

Work through the examples in order. Each builds on the previous.

| # | Concept | File | Key Takeaway |
|---|---------|------|--------------|
| 01 | Basic Producer | `examples/01-basics/producer/` | Connect, configure, send |
| 01 | Basic Consumer | `examples/01-basics/consumer/` | Read from a partition |
| 02 | Consumer Groups | `examples/02-consumer-groups/` | Scale consumption horizontally |
| 03 | Offset Management | `examples/03-offset-management/` | Manual commit for at-least-once |
| 04 | Message Keys | `examples/04-message-keys/` | Deterministic partition routing |
| 05 | Serialization | `examples/05-serialization/` | Typed messages with envelopes |
| 06 | Retry + DLQ | `examples/06-retry-dlq/` | Handle failures gracefully |
| 07 | Idempotent Producer | `examples/07-idempotent-producer/` | No duplicate messages on retry |
| 08 | Graceful Shutdown | `examples/08-graceful-shutdown/` | Clean stop without data loss |
| 09 | Exactly-Once | `examples/09-exactly-once/` | Atomic read-process-write |
| 10 | Transactions | `examples/10-transactions/` | Atomic multi-topic writes |
| 11 | High Throughput | `examples/11-high-throughput/` | Performance tuning benchmarks |

---

## Project Structure

```
kafka-learning/
│
├── examples/               ← Start here. One concept per directory.
│   ├── 01-basics/
│   │   ├── producer/main.go   ← Sync producer with full comments
│   │   └── consumer/main.go   ← Partition consumer
│   ├── 02-consumer-groups/    ← Consumer group lifecycle
│   ├── 03-offset-management/  ← Manual commit strategies
│   ├── 04-message-keys/       ← Key-based partitioning
│   ├── 05-serialization/      ← JSON + envelope pattern
│   ├── 06-retry-dlq/          ← Retry + Dead Letter Queue
│   ├── 07-idempotent-producer/← No duplicates on retry
│   ├── 08-graceful-shutdown/  ← Signal handling + clean stop
│   ├── 09-exactly-once/       ← EOS read-process-write
│   ├── 10-transactions/       ← Atomic multi-topic writes
│   └── 11-high-throughput/    ← Performance benchmarks
│
├── internal/               ← Production-grade wrappers (use in your real services)
│   ├── config/config.go       ← 12-Factor env-based configuration
│   ├── logger/logger.go       ← Structured logging with zap
│   └── kafka/
│       ├── types.go           ← Domain types (Message, TopicConfig, etc.)
│       ├── producer.go        ← SyncProducer + AsyncProducer wrappers
│       ├── consumer.go        ← ConsumerGroup with lifecycle management
│       └── admin.go           ← Topic creation/deletion/inspection
│
├── pkg/                    ← Reusable utilities
│   ├── serialization/json.go  ← Generic JSON serializer + Envelope pattern
│   ├── retry/retry.go         ← Exponential backoff with jitter
│   └── metrics/metrics.go     ← Prometheus metrics for all Kafka ops
│
├── docker/
│   ├── single-node/           ← KRaft single broker (for dev/learning)
│   ├── multi-node/            ← 3-broker cluster (simulates production)
│   └── monitoring/            ← Prometheus + Grafana stack
│
├── scripts/
│   ├── create-topics.sh       ← Create all required topics
│   ├── list-topics.sh         ← Inspect topics and consumer groups
│   ├── health-check.sh        ← Verify Kafka is running
│   └── consume-topic.sh       ← Tail a topic from the command line
│
├── docs/
│   ├── concepts.md            ← Deep-dive on Kafka concepts
│   └── architecture.md        ← System and code architecture diagrams
│
├── go.mod
├── Makefile                   ← All commands (build, test, run examples, docker)
└── .env.example               ← Template for all environment variables
```

---

## Quick Start

### Prerequisites

- Go 1.22+
- Docker and Docker Compose v2
- `make`

### 1. Start Kafka (single-node, KRaft mode)

```bash
make docker-up
```

Wait for the health check to pass (about 15-30 seconds):

```bash
make health-check
```

Then open **Kafka UI** at [http://localhost:8080](http://localhost:8080) to visualize topics and messages.

### 2. Set Up Go Environment

```bash
# Copy environment template
cp .env.example .env

# Download dependencies
make deps
```

### 3. Create Topics

```bash
make create-topics
```

### 4. Run Your First Example

Open two terminals:

```bash
# Terminal 1: Start the producer (sends 1 message/sec)
make run-producer

# Terminal 2: Start the consumer
make run-consumer
```

Watch messages flow in real time. Visit [http://localhost:8080](http://localhost:8080) to see them in Kafka UI.

---

## Running All Examples

Each example is self-contained. Run them with `make`:

```bash
make run-producer          # [01] Basic sync producer
make run-consumer          # [01] Basic consumer
make run-consumer-groups   # [02] Consumer groups (run 2 instances to see rebalancing)
make run-offset            # [03] Manual offset management
make run-keys              # [04] Message keys and partition routing
make run-serialization     # [05] JSON serialization + envelope pattern
make run-dlq               # [06] Retry + Dead Letter Queue
make run-idempotent        # [07] Idempotent producer
make run-graceful          # [08] Graceful shutdown (press Ctrl+C to trigger)
make run-exactly-once      # [09] Exactly-once semantics
make run-transactions      # [10] Kafka transactions
make run-high-throughput   # [11] Throughput benchmarks
```

### Watch Consumer Group Rebalancing (Example 02)

```bash
# Terminal 1
APP_INSTANCE=1 go run examples/02-consumer-groups/main.go

# Terminal 2 — watch partitions redistribute
APP_INSTANCE=2 go run examples/02-consumer-groups/main.go

# Terminal 3 — produce messages
make run-producer
```

---

## Multi-Node Cluster

To simulate a production 3-broker cluster:

```bash
# Stop single-node
make docker-down

# Start 3-broker cluster
make docker-up-multi
```

**Brokers:**
- `localhost:9092` (kafka-1)
- `localhost:9093` (kafka-2)
- `localhost:9094` (kafka-3)

Then update `.env`:
```
KAFKA_BROKERS=localhost:9092,localhost:9093,localhost:9094
```

**Experiment with fault tolerance:**
```bash
# Kill one broker and watch the cluster elect a new partition leader
docker stop kafka-2

# All examples continue working (replication factor = 3, min.isr = 2)
# Re-add the broker
docker start kafka-2
```

---

## Environment Configuration

All configuration is via environment variables. See `.env.example` for the full list.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker addresses |
| `KAFKA_VERSION` | `3.6.0` | Kafka protocol version |
| `KAFKA_CONSUMER_GROUP` | `kafka-learning-group` | Consumer group ID |
| `KAFKA_PRODUCER_REQUIRED_ACKS` | `all` | `none`, `local`, or `all` |
| `KAFKA_PRODUCER_COMPRESSION` | `snappy` | `none`, `gzip`, `snappy`, `lz4`, `zstd` |
| `KAFKA_CONSUMER_AUTO_COMMIT` | `false` | Auto-commit offsets |
| `KAFKA_CONSUMER_OFFSET_INITIAL` | `newest` | `newest` or `oldest` |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `APP_ENV` | `development` | `development` or `production` |

---

## Key Kafka Concepts (Quick Reference)

### Producer Acknowledgements (`acks`)

The `acks` setting controls how many broker acknowledgements the producer waits for before considering a write successful. It is the primary knob for trading **durability vs. latency**.

```
acks=0 (none)   → Fire-and-forget. Producer doesn't wait for any ack.
                  Fastest (no round-trip). Risk: message lost if broker crashes before writing.
                  Use for: high-volume metrics where occasional loss is acceptable.

acks=1 (local)  → Leader writes to its local log, then acks.
                  Good balance of speed and safety. Risk: message lost if leader crashes
                  before followers replicate. Use for: general-purpose events.

acks=-1 (all)   → Leader waits for ALL in-sync replicas (ISR) to write, then acks.
                  Most durable. Latency = slowest ISR replica. Risk: none under normal ops.
                  Use for: financial events, inventory changes, anything you can't lose.
                  Pair with: min.insync.replicas=2 to prevent acking with only 1 replica.
```

### Offset Management

An offset is just an integer — the position of a message in a partition. Kafka stores committed offsets in an internal topic called `__consumer_offsets`. When a consumer restarts, it reads its last committed offset from there and resumes.

```go
// Auto-commit (NOT recommended for production)
// Sarama commits on a timer (default: every 1s) regardless of whether
// your handler succeeded. If your process crashes mid-batch, messages
// between the last auto-commit and the crash are lost.
cfg.Consumer.Offsets.AutoCommit.Enable = true

// Manual commit (RECOMMENDED — at-least-once delivery)
// You control exactly when an offset is committed.
// If your process crashes before committing, the message is redelivered.
cfg.Consumer.Offsets.AutoCommit.Enable = false
// In your handler — after successful processing:
session.MarkMessage(msg, "")  // stages offset in memory (batched)
session.Commit()              // optional: force immediate flush to __consumer_offsets
// Without explicit Commit(), sarama flushes staged offsets on the next
// auto-commit tick OR at session end (rebalance/shutdown).
```

### Delivery Guarantees

Delivery guarantees describe what happens when things go wrong (crashes, network partitions, retries).

| Mode | How | Risk | When to use |
|---|---|---|---|
| **At-most-once** | Commit offset before processing | Message lost if handler crashes after commit | Metrics, logs where loss is acceptable |
| **At-least-once** | Commit offset after successful processing | Message redelivered if crash before commit → duplicate processing | Most business events — make handlers idempotent |
| **Exactly-once** | Kafka transactions + `isolation.level=read_committed` | Highest latency and complexity | Financial ledgers, inventory, anything where duplicates cause real harm |

The practical recommendation: **design for at-least-once, make handlers idempotent**. Exactly-once is expensive and rarely necessary if your handler can safely process the same message twice (e.g., upsert by ID instead of insert).

### Consumer Group Rebalancing

A **rebalance** is triggered whenever the group membership changes: a consumer joins, leaves, or crashes. During a rebalance, all consumers in the group stop consuming, partitions are reassigned, and consumption resumes. This is a brief pause — typically under a second with the default `RoundRobin` strategy.

The **Sticky** rebalance strategy (`sarama.NewBalanceStrategySticky()`) minimizes partition movement: consumers keep their existing partitions where possible, only moving the minimum necessary. This is important for stateful consumers that maintain per-partition caches.

### Consumer Scaling Rules

The maximum useful parallelism for a consumer group equals the number of partitions. Adding more consumers than partitions wastes resources.

```
partitions = 6
consumers = 1 → consumer owns all 6 partitions (no parallelism)
consumers = 3 → 2 partitions each (good balance)
consumers = 6 → 1 partition each (maximum parallelism)
consumers = 7 → 6 active + 1 idle (idle consumer is on standby — useful for failover)

Rule: set partitions at topic creation time based on your peak consumer count.
You can increase partitions later, but it breaks key-based ordering guarantees.
```

### Message Keys and Partitioning

When a producer sends a message with a key, Kafka hashes the key to determine the partition: `partition = hash(key) % numPartitions`. This guarantees that **all messages with the same key always land in the same partition**, and therefore are consumed in order by the same consumer.

```
Key = "user-123" → always → Partition 2 → always → Consumer B
Key = "user-456" → always → Partition 0 → always → Consumer A

Use case: all events for a given user/order/entity go to the same partition,
so a single consumer sees them in order — enabling stateful stream processing.

No key (nil) → round-robin across partitions → maximum throughput, no ordering guarantee.
```

### Log Compaction

By default, Kafka retains messages for a time period (`retention.ms`) or until a size limit (`retention.bytes`). But topics can also be configured with `cleanup.policy=compact`. A **compacted topic** retains only the latest message per key — older messages with the same key are garbage collected. This turns Kafka into a **key-value store** where the log is the source of truth. Useful for: configuration topics, user profile snapshots, CDC (change data capture) streams.

---

## Production Checklist

Use this checklist when deploying Kafka-based services:

### Producer
- [ ] Set `acks = all` for critical business events
- [ ] Enable compression (`snappy` or `lz4`)
- [ ] Set meaningful retry limits (5-10 with backoff)
- [ ] Use idempotent producer for financial/inventory events
- [ ] Close the producer on shutdown to flush pending messages

### Consumer
- [ ] Disable auto-commit; commit manually after successful processing
- [ ] Make message handlers idempotent (safe to run twice)
- [ ] Set `session.timeout` > 3× `heartbeat.interval`
- [ ] Implement graceful shutdown (signal handling + bounded wait)
- [ ] Implement DLQ for permanent failures

### Operations
- [ ] Monitor consumer lag (alert if rising)
- [ ] Monitor produce errors (alert if non-zero)
- [ ] Set topic retention based on your replay requirements
- [ ] Use replication factor ≥ 3 in production
- [ ] Set `min.insync.replicas = 2` for RF=3 topics

---

## Monitoring

Start the monitoring stack:

```bash
make docker-up
make docker-monitoring
```

| Service | URL |
|---|---|
| Kafka UI | [http://localhost:8080](http://localhost:8080) |
| Prometheus | [http://localhost:9090](http://localhost:9090) |
| Grafana | [http://localhost:3000](http://localhost:3000) (admin/admin) |

Your Go services expose Prometheus metrics at `/metrics` (port 2112).

**Most important metrics to alert on:**
- `kafka_consumer_lag` — rising lag = consumers can't keep up
- `kafka_produce_errors_total` — any error = potential data loss
- `kafka_dlq_messages_total` — DLQ messages need human attention

---

## Building Production Services

To build a production-grade service using the internal packages:

```go
package main

import (
    "context"
    "log"

    "kafka-learning/internal/config"
    "kafka-learning/internal/kafka"
    "kafka-learning/internal/logger"
)

func main() {
    cfg, err := config.Load()
    if err != nil {
        log.Fatal(err)
    }

    log := logger.Init(cfg.App.LogLevel, cfg.App.Environment)

    // Create consumer group with your handler
    group, err := kafka.NewConsumerGroup(cfg,
        []string{cfg.Topics.Orders},
        func(ctx context.Context, msg *kafka.Message) error {
            // Your business logic here
            log.Info("processing order", zap.Int64("offset", msg.Offset))
            return nil
        },
    )
    if err != nil {
        log.Fatal("creating consumer group", zap.Error(err))
    }

    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM)
    defer cancel()

    if err := group.Start(ctx); err != nil {
        log.Error("consumer group stopped", zap.Error(err))
    }
}
```

---

## Testing Kafka Code

### Unit Tests (mock IBM/sarama)

The `github.com/IBM/sarama/mocks` package provides in-process mock implementations of producers and consumers. No broker required — tests run fast and deterministically.

```go
// Use IBM/sarama's mock library for unit tests
import "github.com/IBM/sarama/mocks"

func TestOrderHandler(t *testing.T) {
    mockConsumer := mocks.NewConsumer(t, nil)
    // Set expectations: which messages will be delivered
    mockConsumer.ExpectConsumePartition("orders", 0, sarama.OffsetNewest).
        YieldMessage(&sarama.ConsumerMessage{Value: []byte(`{"id":"1"}`)})
    // ... test your handler logic without a real broker
}

func TestProducer(t *testing.T) {
    mockProducer := mocks.NewSyncProducer(t, nil)
    // Assert the producer sends exactly one message to the correct topic
    mockProducer.ExpectSendMessageAndSucceed()
    // ... call your production code that uses the producer
}
```

### Integration Tests (real broker via Docker)

```bash
# Start Kafka for tests
make docker-up

# Run all tests
make test

# Stop after tests
make docker-down
```

---

## Troubleshooting

### "Connection refused" when connecting to Kafka

```bash
# Is Kafka running?
make health-check

# Check container status
docker ps | grep kafka

# View Kafka logs
docker logs kafka --tail 50
```

### Consumer not receiving messages

```bash
# Check if the topic exists and has messages
./scripts/consume-topic.sh orders --from-beginning

# Check consumer group offset lag
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group YOUR_GROUP_ID
```

### Consumer group stuck in rebalance loop

Common causes:
- Processing takes longer than `session.timeout` → increase timeout or optimize handler
- Consumer crashes immediately after joining → check application logs for panics
- Multiple instances with the same `consumer.group.id` but different topics → ensure consistency

### Producer getting "Leader Not Available" errors

This is normal during Kafka startup or leader election after broker failure.
The producer's retry configuration handles this automatically.
Wait 10-15 seconds and the cluster will stabilize.

### "unknown topic or partition" error

```bash
# Create topics first
make create-topics

# Or check existing topics
./scripts/list-topics.sh
```

---

## About the Go Client: IBM/sarama

This repository uses **[`github.com/IBM/sarama`](https://github.com/IBM/sarama)** — the most widely used pure-Go Kafka client.

Brief history: the library was originally created as `Shopify/sarama` by Shopify. In 2023, IBM took over maintenance and the canonical import path became `github.com/IBM/sarama`. The library itself is unchanged in behavior — only the import path and maintainer changed. If you see older code using `github.com/Shopify/sarama`, it is the same library.

Key characteristics of IBM/sarama:
- Pure Go — no CGo, no native dependencies, single binary deployment
- Low-level API — gives you full control over partitions, offsets, and configs
- Implements the full Kafka protocol including transactions and exactly-once semantics
- Provides `sarama/mocks` for unit testing without a real broker
- Actively maintained with support for Kafka 0.8 through 3.x

Alternative Go clients (for awareness):
- [`confluent-kafka-go`](https://github.com/confluentinc/confluent-kafka-go) — wraps the C `librdkafka` library via CGo; higher throughput ceiling but adds a native dependency
- [`franz-go`](https://github.com/twmb/franz-go) — newer pure-Go client with a more ergonomic API and excellent performance; worth evaluating for new projects

---

## Further Reading

- [Apache Kafka Documentation](https://kafka.apache.org/documentation/) — official reference for all broker configs, APIs, and protocols
- [IBM/sarama on GitHub](https://github.com/IBM/sarama) — the Go Kafka client used in this repo (formerly Shopify/sarama)
- [IBM/sarama API docs](https://pkg.go.dev/github.com/IBM/sarama) — full Go package documentation
- [Kafka: The Definitive Guide (free PDF)](https://www.confluent.io/resources/kafka-the-definitive-guide/) — comprehensive book covering internals, operations, and patterns
- [Designing Event-Driven Systems](https://www.oreilly.com/library/view/designing-event-driven-systems/9781492038252/) — patterns and architecture for event-driven microservices
- [Kafka Improvement Proposals (KIPs)](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals) — understand the design decisions behind every Kafka feature
- [Jay Kreps — The Log](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying) — the foundational essay explaining why the append-only log is such a powerful abstraction

---

## License

MIT License. Use this codebase freely for learning, adapting, and building production systems.
