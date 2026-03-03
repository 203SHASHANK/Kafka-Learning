# Architecture: Kafka Learning Repository

This document describes the architecture of both the **learning repository itself**
and the **production patterns** demonstrated within it.

---

## Repository Architecture

```
kafka-learning/
├── cmd/                    ← Production-style service entry points
│   └── (add your services here)
├── examples/               ← Self-contained learning examples (01–11)
│   ├── 01-basics/
│   ├── 02-consumer-groups/
│   └── ...
├── internal/               ← Private packages (not importable externally)
│   ├── config/             ← Environment-based configuration
│   ├── logger/             ← Structured logging (zap)
│   └── kafka/              ← Production Kafka wrappers
│       ├── producer.go     ← SyncProducer + AsyncProducer
│       ├── consumer.go     ← ConsumerGroup with lifecycle management
│       ├── admin.go        ← Topic/group management
│       └── types.go        ← Domain types (Message, TopicConfig, etc.)
├── pkg/                    ← Reusable packages
│   ├── serialization/      ← JSON encoding/decoding with generics
│   ├── retry/              ← Exponential backoff with jitter
│   └── metrics/            ← Prometheus counters/histograms/gauges
├── docker/                 ← Infrastructure as code
│   ├── single-node/        ← KRaft single broker (for development)
│   ├── multi-node/         ← 3-broker cluster (for production simulation)
│   └── monitoring/         ← Prometheus + Grafana stack
├── scripts/                ← Shell utilities
│   ├── create-topics.sh
│   ├── list-topics.sh
│   ├── health-check.sh
│   └── consume-topic.sh
└── docs/                   ← Reference documentation
    ├── concepts.md
    └── architecture.md
```

---

## Single-Node Development Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  Docker Compose (docker/single-node/)                                    │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────┐            │
│  │  Kafka Broker (confluentinc/cp-kafka:7.7.0)             │            │
│  │  Mode: KRaft (no ZooKeeper)                             │            │
│  │  Internal: kafka:29092   External: localhost:9092        │            │
│  │  JMX Port: 9101                                         │            │
│  └─────────────────────────────────────────────────────────┘            │
│                      ↑                                                   │
│  ┌─────────────────────────────────────────────────────────┐            │
│  │  Kafka UI (provectuslabs/kafka-ui)                      │            │
│  │  http://localhost:8080                                   │            │
│  │  Browse topics, messages, consumer groups                │            │
│  └─────────────────────────────────────────────────────────┘            │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
           ↑ localhost:9092
┌──────────────────────────────────────────────────────────────────────┐
│  Go Examples (your code)                                              │
│  make run-producer | run-consumer | run-consumer-groups | ...         │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Three-Broker Production Cluster Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Docker Compose (docker/multi-node/)                                         │
│                                                                              │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                  │
│  │  Broker 1    │    │  Broker 2    │    │  Broker 3    │                  │
│  │  Node ID: 1  │←──→│  Node ID: 2  │←──→│  Node ID: 3  │                  │
│  │  :9092       │    │  :9093       │    │  :9094       │                  │
│  │  [LEADER P0] │    │ [FOLLOWER P0]│    │ [FOLLOWER P0]│                  │
│  │  [FOLLOWER P1│    │  [LEADER P1] │    │ [FOLLOWER P1]│                  │
│  └──────────────┘    └──────────────┘    └──────────────┘                  │
│         ↕                   ↕                   ↕                           │
│         └───────── KRaft Controller Quorum ─────┘                           │
│                   (Raft consensus, no ZooKeeper)                             │
│                                                                              │
│  Replication Factor: 3   Min ISR: 2                                         │
│  Survives: 1 broker failure                                                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Partition Distribution Example

```
Topic: "orders" (6 partitions, replication factor 3)

         Broker 1      Broker 2      Broker 3
P0:      [LEADER]      [FOLLOWER]    [FOLLOWER]
P1:      [FOLLOWER]    [LEADER]      [FOLLOWER]
P2:      [FOLLOWER]    [FOLLOWER]    [LEADER]
P3:      [LEADER]      [FOLLOWER]    [FOLLOWER]
P4:      [FOLLOWER]    [LEADER]      [FOLLOWER]
P5:      [FOLLOWER]    [FOLLOWER]    [LEADER]

Producers write to the LEADER for each partition.
Consumers can read from LEADER (default) or FOLLOWERS.
```

---

## Internal Package Architecture

The `internal/kafka` package follows the **wrapper pattern**: it wraps sarama's
low-level API to enforce production defaults and add cross-cutting concerns.

```
  Application Code
       │
       ▼
  internal/kafka
  ┌─────────────────────────────────────────────────────────────┐
  │  SyncProducer                AsyncProducer                  │
  │    - logs every send           - drains success/error ch     │
  │    - tracks latency            - non-blocking send           │
  │    - wraps errors with context                               │
  │                                                              │
  │  ConsumerGroup                Admin                          │
  │    - auto-reconnects           - idempotent CreateTopic      │
  │    - signals readiness         - graceful DeleteTopic        │
  │    - logs lifecycle events                                   │
  └─────────────────────────────────────────────────────────────┘
       │
       ▼
  github.com/IBM/sarama (Kafka protocol implementation)
       │
       ▼
  Kafka Broker (TCP)
```

---

## Message Flow: Producer to Consumer

```
  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
  │   Producer   │    │    Broker    │    │   Consumer   │
  │   (Go app)   │    │  (Kafka)     │    │ Group (Go)   │
  └──────┬───────┘    └──────┬───────┘    └──────┬───────┘
         │                   │                   │
         │ ProduceRequest    │                   │
         │ (compressed batch)│                   │
         │──────────────────▶│                   │
         │                   │ Replicate         │
         │                   │──▶(follower)      │
         │ Ack (partition,   │                   │
         │      offset)      │                   │
         │◀──────────────────│                   │
         │                   │                   │
         │                   │   FetchRequest    │
         │                   │◀──────────────────│
         │                   │                   │
         │                   │ FetchResponse     │
         │                   │ (batch of records)│
         │                   │──────────────────▶│
         │                   │                   │
         │                   │ OffsetCommit      │
         │                   │ (after processing)│
         │                   │◀──────────────────│
         │                   │                   │
```

---

## Exactly-Once Architecture (Example 09 + 10)

The consume-transform-produce pattern with transactions:

```
  Input Topic         Transactional Producer      Output Topic
  ┌─────────┐        ┌────────────────────────┐   ┌─────────────┐
  │ orders  │──────▶ │ 1. BeginTxn()           │   │enriched-    │
  │         │        │ 2. SendMessage(output)  │──▶│  orders     │
  │ [P0,O5] │        │ 3. AddOffsetsToTxn()    │   │             │
  └─────────┘        │    (registers P0,O6)    │   └─────────────┘
                     │ 4. CommitTxn()          │
                     │    ↓ ATOMIC             │
                     │  • output visible        │
                     │  • offset P0→O6 committed│
                     └────────────────────────┘

Consumer reads with isolation.level=read_committed:
  - Sees output ONLY after CommitTxn()
  - Never sees output from an aborted transaction
```

---

## Dead Letter Queue Pattern (Example 06)

```
  ┌──────────────┐
  │ orders topic │
  └──────┬───────┘
         │ msg
         ▼
  ┌──────────────────────────────────────────┐
  │           Consumer Handler               │
  │                                          │
  │  attempt 1 → fail (transient error)      │
  │  wait 100ms                              │
  │  attempt 2 → fail                        │
  │  wait 200ms                              │
  │  attempt 3 → fail                        │
  │  wait 400ms                              │
  │  attempt 4 → fail   All retries exhausted│
  └──────────────────────┬───────────────────┘
                         │
                    ┌────▼────────────────────────────┐
                    │     DLQ Producer                 │
                    │   publish to orders.dlq:         │
                    │   {                              │
                    │     original_topic: "orders",   │
                    │     original_offset: 42,        │
                    │     error: "...",               │
                    │     retry_count: 4              │
                    │   }                              │
                    └──────────────────────────────────┘
                                   │
                    ┌──────────────▼──────────────┐
                    │ orders.dlq topic             │
                    │ ← monitored by ops team      │
                    │ ← alerts if non-empty        │
                    └──────────────────────────────┘
```
