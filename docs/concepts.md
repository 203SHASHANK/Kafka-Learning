# Kafka Core Concepts

A focused reference for the concepts used throughout this repository.

---

## The Big Picture

Kafka is a **distributed event streaming platform**. At its core, it's a commit log:
an append-only, ordered sequence of records that multiple applications can read at their own pace.

```
 Producers              Kafka Cluster                Consumers
 ─────────              ─────────────                ─────────
 Order Service  ──▶    Topic: "orders"    ──▶    Fulfillment Service
 Payment Service ──▶   Topic: "payments"  ──▶    Analytics Service
 User Service    ──▶   Topic: "events"    ──▶    Email Service
```

**Key insight**: Producers and consumers are completely decoupled. The order service
doesn't know who reads its messages, or when, or how many times.

---

## Broker

A Kafka broker is a server that:
- Stores messages on disk in **log segments**
- Handles producer write requests
- Serves consumer fetch requests
- Participates in cluster coordination (via KRaft or ZooKeeper)

A **Kafka cluster** is 3+ brokers working together. More brokers = more storage capacity
and higher throughput.

```
   Kafka Cluster
   ┌──────────────────────────────────────────┐
   │   Broker 1    Broker 2    Broker 3        │
   │   :9092       :9093       :9094           │
   └──────────────────────────────────────────┘
```

---

## Topic

A **topic** is a named, durable stream of records. Think of it as a database table,
but:
- **Append-only**: you can only add records, never update or delete individual ones
- **Ordered within partitions**: records in a partition have a guaranteed sequence
- **Replicated**: data is copied across multiple brokers for fault tolerance
- **Persistent**: data is stored on disk and retained for a configurable period

```
Topic: "orders"
  ─────────────────────────────────────────────────────────────────
  | Record 0 | Record 1 | Record 2 | Record 3 | Record 4 | ...
  ─────────────────────────────────────────────────────────────────
                                               ↑ newest
```

---

## Partition

A topic is split into **partitions**. Each partition is an independent ordered log.

**Why partitions?**
- **Parallelism**: each partition can be consumed by a separate consumer in a group
- **Throughput**: writes to different partitions happen concurrently
- **Ordering**: ordering is guaranteed within a partition, not across partitions

```
Topic: "orders"  (3 partitions)

  Partition 0:  [msg0][msg3][msg6][msg9]
  Partition 1:  [msg1][msg4][msg7][msg10]
  Partition 2:  [msg2][msg5][msg8][msg11]
```

**Partition count is permanent** (you can increase but not decrease).
Choose wisely at topic creation. Rule of thumb:
`partitions = max(target throughput / throughput_per_partition, desired_consumer_parallelism)`

---

## Offset

An **offset** is a message's sequential position within a partition.
- Unique per partition (not globally unique)
- Monotonically increasing (never repeats, never decreases)
- How consumers track "where I am" in the log

```
Partition 0 offsets:
  [0: {"order":"A"}][1: {"order":"B"}][2: {"order":"C"}]...
                                         ↑
                              consumer committed offset = 2
                              (messages 0 and 1 were processed)
```

**Consumer committed offset**: stored in Kafka's `__consumer_offsets` topic.
After restart, the consumer group reads its last committed offset and resumes from there.

---

## Replication

Each partition has **one leader** and zero or more **followers** (replicas).

```
Topic "orders", Partition 0, Replication Factor 3:

  Broker 1: Partition 0 [LEADER]  ← producer writes here, consumers read here
  Broker 2: Partition 0 [FOLLOWER] ← synced copy from leader
  Broker 3: Partition 0 [FOLLOWER] ← synced copy from leader
```

**In-Sync Replicas (ISR)**: the set of replicas fully caught up with the leader.
If the leader fails, Kafka elects a new leader from the ISR.

**min.insync.replicas (min.isr)**: minimum number of ISR replicas that must
acknowledge a write. Setting `min.isr=2` with `acks=all` means at least 2 brokers
must have the data before the producer gets an acknowledgement.

---

## Producer

A producer writes messages to Kafka. Key configuration choices:

### Acknowledgement (`acks`)
```
acks=0 (NoResponse):   Fire-and-forget. Fastest, no guarantee.
acks=1 (WaitForLocal): Leader writes, then acks. Good default.
acks=-1 (WaitForAll):  All ISR must write, then acks. Most durable.
```

### Idempotent Producer
With `Producer.Idempotent = true`:
- Each message gets a sequence number
- Broker deduplicates retried messages
- Prevents duplicates within a producer session

### Transactional Producer
With `Producer.Transaction.ID = "my-txn-id"`:
- Atomic writes across multiple partitions/topics
- Either all writes commit or none do
- Required for exactly-once semantics

---

## Consumer Group

A **consumer group** is a set of consumers that cooperatively consume a topic.

**Rules**:
- Each partition is assigned to exactly ONE consumer in the group
- A consumer can own multiple partitions
- More consumers than partitions → some consumers are idle
- Messages within a partition maintain their order

```
Topic: "orders" (6 partitions), Consumer Group: "fulfillment"

  2 consumers:
    Consumer A → P0, P1, P2
    Consumer B → P3, P4, P5

  3 consumers:
    Consumer A → P0, P1
    Consumer B → P2, P3
    Consumer C → P4, P5

  7 consumers:
    Consumers A-F → P0-P5 (one each)
    Consumer G → idle
```

---

## Rebalance

A **rebalance** redistributes partitions when the group membership changes:
- New consumer joins (scale out)
- Consumer leaves or crashes (scale in / failure)
- Topic partition count increases

During a rebalance, all consumption pauses briefly. This is why:
- **Heartbeats are critical**: a slow consumer missing heartbeats triggers needless rebalances
- **Session timeout should be generous**: too short = false positives; too long = slow failure recovery
- **Sticky assignor** (alternative to round-robin): minimizes partition movement on rebalance

---

## Delivery Semantics

| Guarantee     | Behavior                               | Config                                    |
|---------------|----------------------------------------|-------------------------------------------|
| At-most-once  | May lose messages, never duplicates    | auto-commit before processing             |
| At-least-once | Never loses, may duplicate             | manual commit after processing            |
| Exactly-once  | Never loses, never duplicates          | transactions + `isolation.level=read_committed` |

**At-least-once** is the standard for most production systems. Make your message
handlers **idempotent** (safe to run twice with the same input) to handle duplicates.

---

## KRaft Mode (Kafka Without ZooKeeper)

Since Kafka 3.3, **KRaft mode** replaces ZooKeeper for cluster coordination.
KRaft is required from Kafka 4.0.

```
Old architecture (pre-3.3):
  Kafka Brokers → ZooKeeper (separate cluster for metadata)

New architecture (KRaft):
  Kafka Brokers → built-in Raft controller (no ZooKeeper)
```

Benefits of KRaft:
- Simpler deployment (one less system to manage)
- Faster broker startup
- Scales to millions of partitions
- Better security model

---

## Message Headers

Headers are key-value metadata attached to a message, separate from the payload.

Common uses:
- `content-type: application/json` → how to decode the value
- `schema-version: 2.0` → which schema version the payload uses
- `trace-id: abc-123` → distributed tracing correlation ID
- `source: order-service` → which service produced this event
- `event-type: OrderPlaced` → routing hint for consumers

Headers do **not** affect partitioning (only the key does).

---

## Consumer Lag

**Consumer lag** = latest offset in partition - consumer's committed offset.

```
Partition 0: latest offset = 1000
Consumer A committed offset = 850
Consumer lag = 1000 - 850 = 150 messages behind
```

Rising lag means consumers can't keep up with producers. Solutions:
1. Add more consumer instances (scale out, up to `num_partitions` max)
2. Optimize the message handler (make it faster)
3. Increase partition count (allows more parallelism)
4. Reduce message complexity or size

**Alert threshold**: define your acceptable lag based on business requirements.
For real-time systems: lag > 10,000 messages = alert. For batch: lag > 1M = alert.
