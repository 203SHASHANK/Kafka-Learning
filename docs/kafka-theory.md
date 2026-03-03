# Kafka Theory — Every Concept Explained Simply

This file explains every important Kafka concept using plain language and real-world
scenarios. Read this before diving into the code examples.

---

## Table of Contents

1. [What Problem Does Kafka Solve?](#1-what-problem-does-kafka-solve)
2. [The Core Idea: A Commit Log](#2-the-core-idea-a-commit-log)
3. [Brokers](#3-brokers)
4. [Topics](#4-topics)
5. [Partitions](#5-partitions)
6. [Offsets](#6-offsets)
7. [Producers](#7-producers)
8. [Consumers](#8-consumers)
9. [Consumer Groups](#9-consumer-groups)
10. [Replication and Fault Tolerance](#10-replication-and-fault-tolerance)
11. [KRaft — Kafka Without ZooKeeper](#11-kraft--kafka-without-zookeeper)
12. [Message Keys and Partitioning Strategy](#12-message-keys-and-partitioning-strategy)
13. [Serialization and Schema Evolution](#13-serialization-and-schema-evolution)
14. [Delivery Semantics](#14-delivery-semantics)
15. [Consumer Lag](#15-consumer-lag)
16. [Rebalancing](#16-rebalancing)
17. [Retention and Log Compaction](#17-retention-and-log-compaction)
18. [Kafka Transactions](#18-kafka-transactions)
19. [Idempotent Producer](#19-idempotent-producer)
20. [Exactly-Once Semantics (EOS)](#20-exactly-once-semantics-eos)
21. [Throughput Tuning](#21-throughput-tuning)
22. [Monitoring and Observability](#22-monitoring-and-observability)
23. [Kafka vs Other Systems](#23-kafka-vs-other-systems)
24. [Common Pitfalls and How to Avoid Them](#24-common-pitfalls-and-how-to-avoid-them)

---

## 1. What Problem Does Kafka Solve?

### The problem without Kafka

Imagine you have an e-commerce platform with these services:
- **Order Service** — creates orders
- **Payment Service** — charges customers
- **Inventory Service** — decrements stock
- **Email Service** — sends confirmation emails
- **Analytics Service** — tracks business metrics

Without Kafka, Order Service has to call each service directly:

```
Order Service ──HTTP──▶ Payment Service
Order Service ──HTTP──▶ Inventory Service
Order Service ──HTTP──▶ Email Service
Order Service ──HTTP──▶ Analytics Service
```

**Problems:**
- If Email Service is down, the order fails (tight coupling)
- Order Service must know about every downstream service
- Adding a new service (e.g., Loyalty Points) requires changing Order Service code
- One slow downstream service slows down the entire order flow
- If Order Service crashes, in-flight requests are lost

### The solution with Kafka

```
Order Service ──▶ Kafka Topic: "orders" ──▶ Payment Service
                                         ──▶ Inventory Service
                                         ──▶ Email Service
                                         ──▶ Analytics Service
```

**Benefits:**
- Order Service only knows about Kafka — not about downstream services
- If Email Service is down, it catches up when it restarts (messages are stored)
- Adding Loyalty Points service = just subscribe to the "orders" topic
- Order Service runs at full speed; downstream services catch up at their own pace
- Messages are persisted on disk — no data loss if a service crashes

**In one sentence:** Kafka decouples producers from consumers in time, space, and speed.

---

## 2. The Core Idea: A Commit Log

The fundamental data structure in Kafka is a **commit log** (also called an append-only log).

Think of it like a ledger book:
- You can only add entries to the end — you can never change or delete past entries
- Each entry has a sequential number (offset)
- Anyone can read any entry by looking up its number
- The book is durable — it survives server restarts

```
Entry 0: {"order": "A", "amount": 100}   ← written first
Entry 1: {"order": "B", "amount": 200}
Entry 2: {"order": "A", "amount": -20}   ← refund for order A
Entry 3: {"order": "C", "amount": 50}
Entry 4: ...                              ← newest
```

**Why is this powerful?**

Because different readers can be at different positions:
- Payment Service has processed up to entry 3
- Analytics Service has processed up to entry 1
- A new reporting service can start from entry 0 and replay everything

Nobody interferes with each other. This is the magic of Kafka.

---

## 3. Brokers

A **broker** is a Kafka server. It:
- Stores messages on disk
- Handles write requests from producers
- Handles read requests from consumers
- Participates in the cluster (leader election, replication)

### Scenario: Single vs Multi-broker

**Single broker (dev/learning):**
```
Your Go App ──▶ [Broker 1: localhost:9092] ──▶ Your Go Consumer
```
Simple, but if this broker dies, everything stops.

**Three-broker cluster (production):**
```
Your Go App ──▶ [Broker 1] [Broker 2] [Broker 3] ──▶ Consumers
```
- Data is replicated across brokers
- If Broker 1 dies, Broker 2 becomes the new leader
- Cluster keeps running

### Bootstrap Brokers

When you connect to Kafka, you provide a **bootstrap broker** list:
```go
brokers := []string{"kafka1:9092", "kafka2:9092", "kafka3:9092"}
```
You don't need to list ALL brokers. Your client connects to any one, downloads
the full cluster metadata, and discovers the rest. Even one broker in the list is enough
to bootstrap — list 2-3 for resilience.

---

## 4. Topics

A **topic** is like a database table, but for streams of events.

### Real-world analogy

Think of a topic like a news feed on social media:
- Posts are appended, never edited or deleted (until retention expires)
- Multiple followers (consumers) each read at their own pace
- New followers can scroll back through history
- The original poster (producer) doesn't know or care who reads it

### Topic naming conventions

Good topic names describe the event that happened, not the action:
```
✅ orders              (events that happened to orders)
✅ payments            (events that happened to payments)
✅ user-signups        (new user registration events)
✅ inventory-updates   (stock level changes)

❌ process-order       (describes an action, not an event)
❌ data                (too vague)
❌ kafka-topic-1       (meaningless)
```

### Topic as a contract

A topic is a **public contract** between producers and consumers:
- Producers agree to publish events of a specific shape
- Consumers agree the format might change (schema evolution)
- Both sides are decoupled — they don't need to run at the same time

---

## 5. Partitions

A topic is divided into **partitions**. This is the most important concept for understanding Kafka's scalability.

### The newspaper printing press analogy

Imagine printing a million newspapers:
- **Without partitions:** One printing press → slow, one at a time
- **With 10 partitions:** 10 printing presses running in parallel → 10x faster

### How partitions work

```
Topic: "orders" (3 partitions)

Partition 0:  [msg0] → [msg3] → [msg6] → [msg9]  ...
Partition 1:  [msg1] → [msg4] → [msg7] → [msg10] ...
Partition 2:  [msg2] → [msg5] → [msg8] → [msg11] ...
              oldest                         newest →
```

- Each partition is an **independent ordered log**
- Messages within a partition are **strictly ordered**
- Messages across partitions have **no ordering guarantee**
- Each partition lives on exactly **one broker at a time** (the leader for that partition)

### How many partitions should you create?

```
Rule of thumb:
  partitions = max(
    target_write_throughput / throughput_per_partition,
    max_desired_consumer_parallelism
  )

Example:
  target: 500 MB/s writes
  per partition: 50 MB/s
  → need 10 partitions for throughput

  Also: want 20 consumers running in parallel
  → need 20 partitions for that

  → choose 20 partitions
```

⚠️ **Warning:** You can increase partition count but **never decrease** it.
Over-partitioning wastes resources; under-partitioning limits throughput.
Think carefully at topic creation.

---

## 6. Offsets

An **offset** is a message's unique sequential number within a partition.

### How offsets work

```
Partition 0:

  Offset: 0        1        2        3        4
          ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐
          │msg A │ │msg B │ │msg C │ │msg D │ │msg E │
          └──────┘ └──────┘ └──────┘ └──────┘ └──────┘

Consumer committed offset = 2
(means: messages at offsets 0 and 1 have been processed)
(next message to consume = offset 2)
```

### Key properties
- Offsets are **per-partition** (not global). Two different partitions both have an offset 0.
- Offsets are **monotonically increasing** — they never go backward or repeat.
- Offsets are **meaningful** — you can seek to any offset and replay from that point.

### Where are offsets stored?

Consumer groups commit their offsets to a special Kafka topic: `__consumer_offsets`.
This means:
- Broker failures don't lose your consumer position
- If your consumer crashes and restarts, it picks up exactly where it left off
- You can inspect committed offsets with admin tools

### Scenario: Consumer restart behavior

```
Situation:
  - Partition has messages 0 through 100
  - Consumer processed up to offset 72 and committed offset 73
  - Consumer crashes
  - Consumer restarts

Behavior:
  - Consumer reads committed offset from __consumer_offsets: 73
  - Consumer resumes from offset 73
  - Messages 0-72 are not reprocessed
  - Messages 73-100 are (re)processed
```

---

## 7. Producers

A **producer** is any application that writes messages to Kafka.

### The three acknowledgement modes

This is the most important producer configuration decision:

#### `acks=0` — Fire and forget
```
Producer ──▶ Broker
          ◀── (no response)
```
- Fastest possible throughput
- Zero durability guarantee: if the broker crashes at the wrong moment, the message is gone
- Use for: non-critical metrics, telemetry, logs where a few drops are acceptable

#### `acks=1` — Leader acknowledgement
```
Producer ──▶ Broker (leader writes to disk)
          ◀── "OK, got it"
                │
                └──(replicates to followers asynchronously)
```
- Good balance of speed and safety
- Risk: if the leader crashes BEFORE replication completes, message is lost
- The new leader (promoted from follower) won't have this message
- Use for: most production workloads that can tolerate rare data loss

#### `acks=-1` (all) — All in-sync replicas acknowledge
```
Producer ──▶ Broker (leader writes to disk)
                └──▶ Follower 1 (writes to disk) ──▶ ACK
                └──▶ Follower 2 (writes to disk) ──▶ ACK
          ◀── "OK, all ISR replicas have it"
```
- Slowest (waits for all ISR replicas)
- Highest durability: message survives even if the leader crashes immediately after ACK
- Use for: financial transactions, order events, anything where loss is unacceptable

### Producer batching

Kafka producers don't send one message at a time. They **batch** messages:

```
Messages arrive:   A  B  C  D  E  F  G  H
                   ↓
                Batch buffer
                   ↓ (flush when: size > batch.size OR time > linger.ms)
                One network request with all 8 messages
```

**Why batching matters:**
- 1 network request for 1000 messages = much cheaper than 1000 network requests
- Compression works better on larger batches (more repeated patterns to compress)
- Throughput scales linearly with batch efficiency

### Compression

```
Raw JSON:  {"order_id":"abc","amount":100,"customer":"john@example.com"}
           ← 65 bytes

Snappy:    compressed representation
           ← ~35 bytes (46% smaller)

After 1000 messages:
  No compression: 65KB of network traffic
  Snappy:         ~35KB of network traffic  ← less network, less broker disk
```

Real-world compression ratios for JSON: 60-80% size reduction.
For binary formats (Protobuf): 20-30% additional reduction.

---

## 8. Consumers

A **consumer** reads messages from Kafka. The key insight: **Kafka doesn't push messages to consumers; consumers pull messages from Kafka.**

### Pull vs Push

**Push (traditional message queues):**
```
Broker pushes to consumer at broker's speed
→ Consumer can be overwhelmed if it's slower than the broker
```

**Pull (Kafka):**
```
Consumer requests: "Give me up to 100 messages"
→ Consumer controls its own pace
→ Consumer can pause without causing backpressure
→ Consumer can batch-fetch for efficiency
```

### The fetch loop

```
for {
    records = kafka.fetch(maxBytes=1MB, maxWaitMs=500)  // fetch batch
    for _, record := range records {
        process(record)                                  // your logic
    }
    kafka.commitOffsets()                               // advance position
}
```

### Consumer isolation

Each consumer group reads the topic independently:

```
Topic: "orders" (offsets 0-1000)

Consumer Group "fulfillment":   processed up to offset 800
Consumer Group "analytics":     processed up to offset 200
Consumer Group "reporting":     processed from offset 0 (just started)
```

All three groups read the same messages. They don't interfere.
This is unlike a traditional queue where reading "consumes" the message.

---

## 9. Consumer Groups

A consumer group is the mechanism for **scaling consumption horizontally**.

### The fundamental rule

> Each partition is assigned to exactly ONE consumer within a group at any time.

### Visual explanation

```
Topic: "orders" — 6 partitions (P0, P1, P2, P3, P4, P5)

Scenario A: 1 consumer
  Consumer-1: P0, P1, P2, P3, P4, P5   ← handles all 6 partitions

Scenario B: 2 consumers (scale out)
  Consumer-1: P0, P1, P2
  Consumer-2: P3, P4, P5               ← each handles 3 partitions

Scenario C: 3 consumers
  Consumer-1: P0, P1
  Consumer-2: P2, P3
  Consumer-3: P4, P5                   ← each handles 2 partitions

Scenario D: 6 consumers (maximum parallelism)
  Consumer-1: P0
  Consumer-2: P1
  Consumer-3: P2
  Consumer-4: P3
  Consumer-5: P4
  Consumer-6: P5                       ← perfect parallelism

Scenario E: 7 consumers (over-provisioned)
  Consumer-1: P0
  ...
  Consumer-6: P5
  Consumer-7: (idle — no partitions left)
```

**Key insight:** You cannot have MORE useful consumers than partitions.
If you want to scale to 20 parallel consumers, create 20 partitions.

### Consumer group ID

The `group.id` is the identifier Kafka uses to track state for a group.
- All instances with the same group.id share partitions
- Different group.ids read independently
- Change the group.id and you start fresh (no committed offsets)

### Scenario: Running multiple instances of the same service

You have an `order-processor` service. You deploy 3 instances for reliability.
All 3 should share the work, not each process all messages:

```go
// ALL three instances use the SAME group ID
cfg.Consumer.GroupID = "order-processor"
```

Kafka distributes partitions among the three. Each order is processed by exactly one instance.

---

## 10. Replication and Fault Tolerance

### The problem: Single points of failure

If you store data on one server and that server dies, data is lost.
Kafka solves this with **replication**.

### Leader and followers

For each partition:
- One broker is the **leader** — producers write here, consumers read here
- Other brokers are **followers** — they copy everything from the leader
- Together they form the **replica set**

```
Topic "orders", Partition 0, 3 replicas:

  Broker 1: Partition-0 [LEADER]   ← all reads and writes go here
  Broker 2: Partition-0 [FOLLOWER] ← silently syncing from Broker 1
  Broker 3: Partition-0 [FOLLOWER] ← silently syncing from Broker 1
```

### In-Sync Replicas (ISR)

The ISR is the set of follower replicas that are fully caught up with the leader.

```
Broker 1: Partition-0 [LEADER,  offset=1000]  ← ISR
Broker 2: Partition-0 [FOLLOWER, offset=1000] ← ISR (fully caught up)
Broker 3: Partition-0 [FOLLOWER, offset=950]  ← NOT ISR (too far behind)
```

ISR membership is dynamic. A follower is removed from ISR if it falls more than
`replica.lag.time.max.ms` (default 30s) behind the leader.

### Failure scenario

```
Day 1: Everything working
  Broker 1: Partition-0 LEADER  (offset 500)
  Broker 2: Partition-0 FOLLOWER (offset 500) ← ISR
  Broker 3: Partition-0 FOLLOWER (offset 500) ← ISR

Day 2: Broker 1 crashes
  Kafka detects: no heartbeat from Broker 1
  Kafka controller elects new leader from ISR
  Broker 2 becomes new LEADER

  Clients connect to Broker 2 for Partition-0
  Zero data loss (Broker 2 had all messages up to offset 500)

Day 3: Broker 1 comes back
  Broker 1 rejoins as FOLLOWER
  Catches up by syncing from Broker 2
  Eventually rejoins ISR
```

### min.insync.replicas

This setting says: "a write is not acknowledged unless at least N replicas have it."

```
replication_factor = 3
min.insync.replicas = 2
acks = all

Producer writes message → leader writes → 1 follower writes → ACK
Even if one follower is down, writes succeed (2 of 3 are in sync)

If 2 brokers are down (only 1 ISR): writes FAIL with "not enough replicas"
→ This is safer than silently accepting data that might be lost
```

---

## 11. KRaft — Kafka Without ZooKeeper

### The old way (pre-3.3): ZooKeeper

Kafka used ZooKeeper as a separate coordination service:
```
ZooKeeper cluster (3 nodes)
    ↕ metadata, leader election, config
Kafka cluster (3 brokers)
```

**Problems:**
- Two systems to manage, monitor, and secure
- ZooKeeper was the scaling bottleneck (couldn't handle millions of partitions)
- Complex networking (ports for both systems)

### The new way: KRaft (Kafka Raft)

Kafka now has its own built-in consensus mechanism (based on the Raft algorithm):
```
Kafka cluster (3 brokers, all acting as controllers too)
    ↕ internal KRaft protocol handles metadata
```

**Benefits:**
- One system to manage instead of two
- Faster startup (no ZooKeeper bootstrap)
- Scales to millions of partitions
- Simpler security model
- Required from Kafka 4.0

### What is Raft?

Raft is a consensus algorithm that lets a cluster of nodes agree on values even if some nodes fail. Think of it as a voting system:
- 3 nodes: can survive 1 failure (2/3 majority needed)
- 5 nodes: can survive 2 failures (3/5 majority needed)

In KRaft mode, the "controllers" are the nodes that run Raft to manage cluster metadata (which broker is the leader for each partition, etc.).

---

## 12. Message Keys and Partitioning Strategy

### How keys determine partitions

```
partition = murmur2_hash(key) % num_partitions
```

Same key → same hash → same partition. Every time.

### When to use keys

#### Scenario 1: E-commerce orders (use customer ID as key)

```
Order #1 for customer-A: placed    → hash("customer-A") % 6 = partition 2
Order #2 for customer-A: paid      → hash("customer-A") % 6 = partition 2
Order #3 for customer-A: shipped   → hash("customer-A") % 6 = partition 2
Order #4 for customer-A: delivered → hash("customer-A") % 6 = partition 2
```

All 4 events for customer-A are in partition 2, in order.
The consumer on partition 2 processes them sequentially: placed → paid → shipped → delivered.

Without a key, these events could end up in different partitions and be processed out of order.

#### Scenario 2: Bank account transactions (use account ID as key)

```
Account-123: deposit $1000  → partition 0
Account-123: withdraw $500  → partition 0  (same account, same partition)
Account-123: check balance  → partition 0  (correct: balance = $500)
```

Without key: withdraw could be processed before deposit → balance goes negative.

### Hot partition problem

If you choose a key that's uneven, one partition gets overloaded:

```
Bad key: use "country" as key for global app
  USA messages:    90% → partition 0 (HOT 🔥)
  Canada messages: 5%  → partition 1
  Other:           5%  → partition 2

Good key: use user_id (high cardinality, even distribution)
  Each partition gets ~equal traffic
```

**Rule:** Choose keys with high cardinality and even distribution.

### When NOT to use keys (nil key)

If you don't care about ordering across messages, use nil key:
- Metrics and telemetry (each data point is independent)
- Bulk data ingestion (order doesn't matter)
- Fan-out events where ordering isn't needed

Nil key → round-robin partition assignment → maximum throughput.

---

## 13. Serialization and Schema Evolution

### The problem: Kafka only stores bytes

Kafka doesn't know if your message is JSON, Protobuf, XML, or plain text.
It stores and delivers raw bytes. YOU are responsible for serialization.

### JSON — Simple but verbose

```
Pros:
  - Human-readable (easy to debug)
  - No schema required
  - Easy to add optional fields

Cons:
  - Large payload (field names repeated in every message)
  - No type safety (is "amount" a string or number?)
  - No schema enforcement (producer and consumer can drift)
```

### Protobuf — Compact and typed

```
Pros:
  - Very small payload (binary, no field names on the wire)
  - Strongly typed
  - Schema evolution with backward/forward compatibility rules

Cons:
  - Not human-readable (need tooling to inspect)
  - Requires schema files (.proto)
  - Code generation required
```

### Avro — Best for large teams

```
Pros:
  - Compact binary format
  - Schema stored in Confluent Schema Registry
  - Built-in schema compatibility checks (prevent breaking changes)
  - Schema evolution is first-class

Cons:
  - Requires Schema Registry infrastructure
  - More complex setup
```

### The Envelope Pattern

For production JSON, always wrap your payload in an envelope:

```json
{
  "version": "1.0",
  "event_type": "OrderPlaced",
  "source": "order-service",
  "trace_id": "abc-123-xyz",
  "payload": {
    "order_id": "ord-456",
    "amount": 99.99
  }
}
```

**Why?**
- `version`: consumers can handle v1 and v2 formats during migrations
- `event_type`: one topic can hold multiple event types, consumers filter
- `trace_id`: correlate logs across services without modifying the payload
- `source`: know which service produced this event

### Schema evolution rules (JSON)

```
✅ SAFE: Add a new optional field (old consumers ignore unknown fields)
  v1: {"order_id": "123", "amount": 100}
  v2: {"order_id": "123", "amount": 100, "currency": "USD"}  ← old consumers still work

❌ UNSAFE: Remove or rename a required field
  v1: {"order_id": "123", "amount": 100}
  v2: {"id": "123", "total": 100}  ← old consumers expecting "order_id" break

✅ STRATEGY: Version your schema and handle multiple versions in consumers:
  if envelope.Version == "2.0" {
      // use new field names
  } else {
      // use old field names
  }
```

---

## 14. Delivery Semantics

This is often asked in interviews. Know these deeply.

### At-Most-Once (may lose messages)

```
Consumer reads message
Consumer commits offset IMMEDIATELY (before processing)
Consumer processes message
Consumer CRASHES

After restart:
  Offset was already committed → message is GONE
  This message is NEVER processed
```

**When to use:** Metrics, logging, monitoring — where occasional loss is acceptable.

### At-Least-Once (may duplicate messages)

```
Consumer reads message
Consumer processes message
Consumer commits offset
Consumer CRASHES (before commit)

After restart:
  Offset was not committed → Kafka re-delivers the message
  This message is processed AGAIN (duplicate)
```

This is the **default and recommended** mode for most systems.
Make your handler **idempotent** (safe to run twice with same input):

```go
// Idempotent example: use ON CONFLICT DO NOTHING (upsert)
db.Exec("INSERT INTO orders (id, amount) VALUES (?, ?) ON CONFLICT (id) DO NOTHING",
    order.ID, order.Amount)
// Running this twice has the same effect as running it once ✅
```

### Exactly-Once (never lost, never duplicated)

```
Uses Kafka Transactions:
  BEGIN TRANSACTION
  Write processed result to output topic
  Commit input offset AS PART OF the transaction
  COMMIT TRANSACTION  ← atomically: result visible + input offset advanced

If crash happens BEFORE COMMIT:
  Transaction aborts
  Input offset NOT advanced
  Message redelivered, result NOT visible in output
  → Process again, commit transaction
  → Result appears exactly once ✅
```

**When to use:** Financial systems, billing, inventory — where duplicates cause real harm.

**Cost:** Higher latency, lower throughput, more complex code.

---

## 15. Consumer Lag

### What is lag?

```
Partition 0:
  Latest offset (produced): 1000
  Consumer committed offset: 750
  Lag = 1000 - 750 = 250 messages
```

Lag tells you: "how far behind are my consumers?"

### What causes lag?

1. **Consumer too slow:** processing takes longer than messages arrive
2. **Consumer crashed:** stopped processing, messages accumulate
3. **Not enough consumers:** need more instances to handle the volume
4. **Slow downstream:** database writes, API calls slowing the handler

### Lag in a time-based scenario

```
12:00:00 — Producer writes 1000 messages/second
12:00:00 — Consumer processes 800 messages/second
12:00:01 — Lag = 200 messages (growing!)
12:00:10 — Lag = 2000 messages (still growing)

Action: Scale consumers from 3 to 6 instances
12:00:11 — Consumer now processes 1600 messages/second (2 × 800)
12:00:12 — Lag starts decreasing
12:00:22 — Lag = 0 (caught up)
```

### Why lag is THE most important Kafka metric

Rising lag means:
- Your system is falling behind
- Events are being delayed (SLA violations)
- Storage pressure builds up (messages retained longer than expected)
- If lag grows unbounded, consumer falls behind the retention window
  → oldest messages are deleted BEFORE consumer processes them → **data loss**

**Always alert on rising consumer lag.**

---

## 16. Rebalancing

### What is a rebalance?

A rebalance is the process of redistributing partitions among consumers when the group composition changes.

### What triggers a rebalance?

1. A new consumer joins the group (deploy, scale out)
2. A consumer leaves gracefully (shutdown, scale in)
3. A consumer dies (timeout, crash)
4. A topic partition count changes
5. Group subscription changes

### What happens during a rebalance?

```
BEFORE rebalance (2 consumers):
  Consumer-A: P0, P1, P2
  Consumer-B: P3, P4, P5

Rebalance triggered (Consumer-C joins)

DURING rebalance:
  ALL consumers pause consumption (no messages delivered)
  Kafka determines new assignment

AFTER rebalance (3 consumers):
  Consumer-A: P0, P1
  Consumer-B: P2, P3
  Consumer-C: P4, P5
```

### The "stop the world" problem

The pause during rebalance is brief but disruptive. Long rebalances can cause
message processing delays or even trigger SLA violations.

### Causes of excessive rebalancing

1. **Consumer processing too slow:** misses heartbeats → Kafka thinks it died
   ```
   Fix: increase session.timeout.ms, or optimize the handler
   ```

2. **Auto-commit causing false "alive" signals:**
   ```
   Consumer is stuck in processing but auto-commit fires → looks alive to Kafka
   Fix: use manual commit so you control when to advance the offset
   ```

3. **Frequent deploys:** every deploy = rebalance
   ```
   Fix: use sticky partition assignment (reduces partition movement on rebalance)
   ```

### Sticky vs Round-Robin assignment

**Round-Robin:**
```
Rebalance 1:  Consumer-A: P0,P1,P2    Consumer-B: P3,P4,P5
Rebalance 2:  Consumer-A: P0,P2,P4    Consumer-B: P1,P3,P5
              (all partitions moved!)
```

**Sticky:**
```
Rebalance 1:  Consumer-A: P0,P1,P2    Consumer-B: P3,P4,P5
Rebalance 2:  Consumer-A: P0,P1       Consumer-B: P3,P4,P5  Consumer-C: P2 (only minimum moved)
              (minimizes disruption!)
```

---

## 17. Retention and Log Compaction

### Time-based retention (default)

Kafka deletes old messages after a configurable time:

```
retention.ms = 604800000  (7 days, the default)

Day 1:  [msg0][msg1][msg2]...[msg1000]
Day 8:  [msg0] is deleted (7 days old)
Day 8:  [msg1001]...[msg2000] added
```

This is like a rolling window of history. Good for event streams where you only
care about recent events.

### Size-based retention

```
retention.bytes = 10GB per partition

When partition reaches 10GB:
  Oldest segments are deleted to make room
```

### Log compaction (for changelog topics)

Instead of deleting by age, compaction deletes by **key duplication**:
only the latest value for each key is kept.

```
BEFORE compaction:
  user-42: {"name":"Alice","email":"alice@old.com"}
  user-99: {"name":"Bob","email":"bob@example.com"}
  user-42: {"name":"Alice","email":"alice@new.com"}  ← updated email
  user-42: {"name":"Alice","email":"alice@gmail.com"} ← updated again

AFTER compaction:
  user-99: {"name":"Bob","email":"bob@example.com"}
  user-42: {"name":"Alice","email":"alice@gmail.com"} ← only latest
```

**Use cases for compacted topics:**
- User profile cache (always want the latest state)
- Application configuration
- Database Change Data Capture (CDC) — keep latest state of each row

**Tombstone messages:** Set value to `null` to delete a key from a compacted topic.
```
user-42: null  ← marks user-42 for deletion during compaction
```

---

## 18. Kafka Transactions

### The problem transactions solve

```
You want to atomically:
  1. Write to topic "orders" (order confirmed)
  2. Write to topic "inventory" (stock decremented)
  3. Write to topic "notifications" (email queued)

WITHOUT transactions:
  Step 1 succeeds ✅
  Step 2 succeeds ✅
  App crashes 💥
  Step 3 never happens ❌
  Result: Order confirmed and stock decremented, but no email sent
  → Data inconsistency between topics

WITH transactions:
  All 3 writes happen atomically
  If crash before commit: none of them appear
  Consumer sees either all 3 or none
```

### The transaction lifecycle

```
producer.BeginTxn()
    │
    ├─ producer.SendMessage({Topic: "orders", ...})    ← buffered
    ├─ producer.SendMessage({Topic: "inventory", ...}) ← buffered
    ├─ producer.SendMessage({Topic: "notifications", ...}) ← buffered
    │
producer.CommitTxn()   ← all 3 messages become visible atomically
    OR
producer.AbortTxn()    ← all 3 messages are discarded
```

### read_committed isolation level

Consumers must use `isolation.level = read_committed` to benefit from transactions:

```
Without read_committed:
  Consumer sees transactional messages immediately when written
  (even before CommitTxn is called)
  If transaction aborts later, consumer already processed "ghost" messages

With read_committed:
  Consumer ONLY sees messages after CommitTxn is called
  Aborted transaction messages are never seen
```

### Transaction ID and zombie fencing

The Transaction ID must be stable and unique per logical producer:

```
Producer Instance A (running): Transaction-ID = "order-processor-1"
  → assigned Producer Epoch = 1

Producer Instance B starts with same Transaction-ID:
  → Broker assigns Epoch = 2

Producer Instance A tries to commit:
  → Broker sees Epoch 1 < current Epoch 2
  → FENCED: A's transaction is aborted
  → Only the latest instance (B) can produce
```

This prevents split-brain: two instances of the same logical producer can't
both write transactionally at the same time.

---

## 19. Idempotent Producer

### The duplicate problem

```
Normal producer (without idempotency):

  Producer sends: {"order": "A"}  ─▶ Broker writes ─▶ ACK
                                                         ↓
                                               Network error!
                                               Producer doesn't receive ACK
                                               Producer RETRIES
  Producer sends: {"order": "A"}  ─▶ Broker writes AGAIN

  Result: "order A" appears twice in the topic
```

### The idempotent solution

```
Idempotent producer:

  Broker assigns PID (Producer ID) = 42

  Producer sends message #1 (seq=0): {"order": "A"}
    Broker: "PID=42, seq=0 — first time, write it"
    Broker writes, sends ACK

  Network error, ACK lost. Producer retries:
  Producer sends message #1 (seq=0) again: {"order": "A"}
    Broker: "PID=42, seq=0 — I already have seq=0 for PID 42, this is a DUPLICATE"
    Broker ACKs (to stop producer retrying) but does NOT write again

  Result: "order A" appears EXACTLY once
```

### Configuration requirements

```go
config.Producer.Idempotent = true
config.Net.MaxOpenRequests = 1   // REQUIRED: maintain ordering for sequence numbers
config.Producer.RequiredAcks = sarama.WaitForAll  // REQUIRED for idempotency
```

### Scope of idempotency

- ✅ Safe within one partition within one producer session
- ❌ NOT safe across producer restarts (new PID assigned on restart)
- ❌ NOT safe across multiple partitions (use transactions for that)

---

## 20. Exactly-Once Semantics (EOS)

### The consume-transform-produce pattern

This is the most common pattern for Kafka stream processing:

```
Input Topic ──▶ Consumer reads ──▶ Transform/Enrich ──▶ Producer writes ──▶ Output Topic
```

The challenge: what if the consumer crashes AFTER writing to the output topic but BEFORE committing the input offset?

```
Without EOS:
  1. Read msg at offset 5 from input
  2. Write result to output ✅
  3. CRASH 💥 (offset never committed)
  4. Restart: re-read offset 5 from input
  5. Write result to output AGAIN ← DUPLICATE in output topic
```

### EOS solution: atomic offset commit

```
With EOS:
  producer.BeginTxn()
  producer.SendMessage(outputTopic, result)     ← buffered
  producer.AddOffsetsToTxn(inputOffset + 1)     ← register offset in transaction
  producer.CommitTxn()                          ← ATOMIC:
                                                    - result appears in output
                                                    - input offset advances

  If crash BEFORE CommitTxn:
    Transaction aborts
    Output message NOT visible
    Input offset NOT advanced
    → On restart: re-read same input, process again, commit transaction
    → Output appears exactly once
```

---

## 21. Throughput Tuning

### Producer-side tuning

| Setting | Default | High-Throughput | Explanation |
|---|---|---|---|
| `compression.type` | none | snappy/lz4 | Reduces payload size 60-80% |
| `batch.size` | 16KB | 512KB-1MB | Larger batches = fewer requests |
| `linger.ms` | 0 | 5-50ms | Wait for more messages before sending |
| `acks` | all | 1 | Reduce ack latency (loses some durability) |
| `max.in.flight.requests` | 5 | 5 | Pipeline multiple requests |

### Consumer-side tuning

| Setting | Default | High-Throughput | Explanation |
|---|---|---|---|
| `fetch.min.bytes` | 1 | 1MB | Wait for larger batches |
| `fetch.max.wait.ms` | 500 | 500 | Max wait for fetch.min.bytes |
| `max.poll.records` | 500 | 2000 | Records per poll call |

### The throughput vs latency trade-off

```
Low latency config:
  linger.ms = 0       (send immediately)
  batch.size = 16KB   (small batches)
  compression = none  (no compression overhead)
  → Each message delivered quickly, but many small network requests

High throughput config:
  linger.ms = 10ms    (wait 10ms for more messages)
  batch.size = 1MB    (large batches)
  compression = snappy (CPU cost, but big payload reduction)
  → Messages delivered in large efficient batches, but small delay
```

### The formula for required partitions

```
Required throughput: 500 MB/s
Producer throughput per partition: ~50 MB/s (typical)
Required partitions for producers: 500/50 = 10

Consumer throughput per partition: ~30 MB/s (due to processing overhead)
Required partitions for consumers: 500/30 ≈ 17

Choose: 20 partitions (round up, future-proof)
```

---

## 22. Monitoring and Observability

### The metrics that matter

#### 1. Consumer Lag (most important)
```
Metric: kafka_consumer_group_lag
Alert: lag > 10,000 AND growing
Action: scale consumers or optimize handler
```

#### 2. Producer Error Rate
```
Metric: kafka_producer_record_error_rate
Alert: any non-zero value for critical topics
Action: investigate broker health, network
```

#### 3. Under-Replicated Partitions
```
Metric: kafka_server_replica_under_replicated_partitions
Alert: any non-zero value
Action: check broker health, ISR status
```

#### 4. Request Latency
```
Metric: kafka_network_request_total_time_ms (p99)
Alert: p99 > 100ms (or your SLA)
Action: check broker load, JVM heap, disk I/O
```

#### 5. Active Controller Count
```
Metric: kafka_controller_active_count
Value should be exactly 1
Alert: != 1 means controller election in progress
```

### The monitoring stack

```
Kafka Broker → JMX (Java Management Extensions)
    ↓
JMX Exporter (converts JMX to Prometheus format)
    ↓
Prometheus (stores metrics time-series)
    ↓
Grafana (visualizes metrics with dashboards)
    +
AlertManager (fires alerts when thresholds are exceeded)
```

---

## 23. Kafka vs Other Systems

### Kafka vs RabbitMQ

| Feature | Kafka | RabbitMQ |
|---|---|---|
| Message ordering | Per-partition | Per-queue |
| Message retention | Configurable (days/forever) | Deleted after consumption |
| Replay | ✅ Yes | ❌ No |
| Multiple consumers | Each group reads independently | Message consumed by one |
| Throughput | Very high (millions/s) | Moderate (thousands/s) |
| Use case | Event streaming, audit logs | Task queues, RPC, fan-out |

**Choose Kafka when:** You need replay, audit, or multiple independent consumers.
**Choose RabbitMQ when:** You need complex routing, task queues, or request-response.

### Kafka vs AWS SQS/SNS

| Feature | Kafka | SQS |
|---|---|---|
| Retention | Days to forever | Max 14 days |
| Throughput | Near unlimited | High but throttled |
| Ordering | Yes (per partition) | FIFO queues only |
| Self-managed | Yes | No (fully managed) |
| Replay | ✅ Yes | ❌ No |

**Choose SQS when:** You want zero ops, AWS ecosystem, don't need replay.
**Choose Kafka when:** You need replay, high throughput, or multi-region replication.

### Kafka vs Redis Pub/Sub

| Feature | Kafka | Redis Pub/Sub |
|---|---|---|
| Persistence | ✅ Yes | ❌ No (fire-and-forget) |
| Consumer groups | ✅ Yes | ❌ No (all subscribers get all messages) |
| Replay | ✅ Yes | ❌ No |
| Latency | Low (ms) | Very low (sub-ms) |

**Choose Redis when:** You need sub-millisecond delivery and can tolerate message loss.
**Choose Kafka when:** You need durability, replay, or consumer groups.

---

## 24. Common Pitfalls and How to Avoid Them

### Pitfall 1: Committing offsets before processing

```go
// ❌ WRONG: commit before you know if processing succeeded
session.MarkMessage(msg, "")  // ← commit first
err := processMessage(msg)    // ← process second
// If processMessage crashes, the offset is committed but message wasn't processed
// → DATA LOSS

// ✅ CORRECT: process first, commit after success
err := processMessage(msg)
if err == nil {
    session.MarkMessage(msg, "")  // ← commit only after success
}
```

### Pitfall 2: Not draining async producer channels

```go
// ❌ WRONG: creating async producer without draining channels
producer, _ := sarama.NewAsyncProducer(brokers, config)
producer.Input() <- message
// Channels fill up → producer deadlocks

// ✅ CORRECT: always start drain goroutines
go func() { for range producer.Successes() {} }()
go func() { for range producer.Errors() {} }()
```

### Pitfall 3: Using auto-commit with slow message handlers

```go
// ❌ PROBLEM: handler takes 30 seconds, auto-commit fires every 5 seconds
// Offset committed at second 5, but processing still ongoing
// Consumer crashes at second 20: offset already committed
// Message is LOST

// ✅ FIX: disable auto-commit, commit manually after processing
config.Consumer.Offsets.AutoCommit.Enable = false
// In handler:
err := slowProcessMessage(msg)  // takes 30 seconds
if err == nil {
    session.MarkMessage(msg, "")
}
```

### Pitfall 4: Not making handlers idempotent

```go
// ❌ RISKY: handler is not idempotent
func handler(msg *kafka.Message) error {
    db.Exec("INSERT INTO orders VALUES (?)", order.ID)  // duplicate key error on replay!
    return nil
}

// ✅ SAFE: handler is idempotent
func handler(msg *kafka.Message) error {
    db.Exec("INSERT INTO orders VALUES (?) ON CONFLICT (id) DO NOTHING", order.ID)
    return nil
}
```

### Pitfall 5: Forgetting to close producers

```go
// ❌ WRONG: producer not closed
producer, _ := sarama.NewSyncProducer(brokers, config)
// ... produce messages
// process exits
// Last few buffered messages are LOST

// ✅ CORRECT: always defer Close()
producer, _ := sarama.NewSyncProducer(brokers, config)
defer producer.Close()  // ← flush remaining messages on exit
```

### Pitfall 6: Blocking in ConsumeClaim without checking context

```go
// ❌ WRONG: blocking call that ignores shutdown
func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    for msg := range claim.Messages() {
        time.Sleep(60 * time.Second)  // ← long block, ignores context
        session.MarkMessage(msg, "")
    }
    return nil
}

// ✅ CORRECT: check context during long operations
func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    for msg := range claim.Messages() {
        select {
        case <-session.Context().Done():
            return nil  // ← clean shutdown
        default:
        }
        processWithContext(session.Context(), msg)
        session.MarkMessage(msg, "")
    }
    return nil
}
```

### Pitfall 7: Too many partitions

```
Don't create 1000 partitions for a low-volume topic.
Each partition:
  - Needs a file handle on the broker
  - Adds rebalance overhead
  - Increases controller memory usage

Start with 3-10 partitions. Increase if needed.
```

### Pitfall 8: Setting partition count lower than consumer count

```
// Situation: 3 partitions, 10 consumer instances
// 7 consumers are idle and wasting resources
// More consumers than partitions NEVER increases throughput

// Fix: match partition count to your expected consumer parallelism
```
