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

Apache Kafka is a **distributed event streaming platform**. Think of it as a
high-performance, durable message bus for your microservices.

Unlike traditional queues (RabbitMQ, SQS):
- Messages are **retained on disk** (for hours, days, or forever) — consumers
  can replay history
- A topic is split into **partitions** — enabling horizontal scaling
- Multiple independent consumer groups can each read the same topic — decoupled consumers
- Kafka handles **millions of messages per second** — designed for throughput

**When should you use Kafka?**
| Use Case | Good Fit? |
|---|---|
| Async communication between microservices | ✅ |
| Event sourcing and audit logs | ✅ |
| Real-time data pipelines | ✅ |
| High-throughput metrics and telemetry | ✅ |
| Simple task queues with one consumer | ❌ (use Redis/SQS) |
| RPC / request-response patterns | ❌ (use gRPC) |

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
- **Broker**: A Kafka server that stores data and serves clients
- **Topic**: A named stream of records (like a database table)
- **Partition**: A topic is divided into N ordered logs (enables parallelism)
- **Offset**: A message's unique sequential position within a partition
- **Producer**: Writes messages to topics
- **Consumer**: Reads messages from topics
- **Consumer Group**: Multiple consumers sharing the work of consuming a topic

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

```
acks=0 (none)   → Fire-and-forget. Fastest. No guarantee.
acks=1 (local)  → Leader writes to disk, then acks. Good default.
acks=-1 (all)   → All ISR replicas write, then acks. Most durable. Use for critical events.
```

### Offset Management

```go
// Auto-commit (NOT recommended for production)
cfg.Consumer.Offsets.AutoCommit.Enable = true   // commits on timer

// Manual commit (RECOMMENDED — at-least-once delivery)
cfg.Consumer.Offsets.AutoCommit.Enable = false
// Then in your handler:
session.MarkMessage(msg, "")  // stages offset for next commit
session.Commit()              // optional: immediately flush to Kafka
```

### Delivery Guarantees

| Mode | How | Risk |
|---|---|---|
| **At-most-once** | Commit before processing | Messages can be lost |
| **At-least-once** | Commit after processing | Messages can be duplicated |
| **Exactly-once** | Transactions + read_committed | Most expensive, safest |

### Consumer Scaling Rules

```
partitions = 6
consumers = 1 → consumer owns all 6 partitions
consumers = 3 → 2 partitions each
consumers = 6 → 1 partition each (maximum parallelism)
consumers = 7 → 1 consumer idle (extra consumers don't help)
```

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

### Unit Tests (mock sarama)

```go
// Use sarama's mock library for unit tests
import "github.com/IBM/sarama/mocks"

func TestOrderHandler(t *testing.T) {
    mockConsumer := mocks.NewConsumer(t, nil)
    // ... test your handler without a real broker
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

## Further Reading

- [Kafka Documentation](https://kafka.apache.org/documentation/) — official reference
- [IBM/sarama](https://github.com/IBM/sarama) — Go Kafka client we use
- [Kafka: The Definitive Guide](https://www.oreilly.com/library/view/kafka-the-definitive/9781491936153/) — comprehensive book
- [Designing Event-Driven Systems](https://www.oreilly.com/library/view/designing-event-driven-systems/9781492038252/) — patterns and architecture
- [Kafka Improvement Proposals](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals) — understand design decisions

---

## License

MIT License. Use this codebase freely for learning, adapting, and building production systems.
