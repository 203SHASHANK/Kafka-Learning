# Kafka Interview Questions & Answers

Complete preparation guide for Kafka interviews — from junior to staff engineer level.
Questions are grouped by topic and difficulty level.

---

## Table of Contents

1. [Fundamentals (Junior Level)](#1-fundamentals-junior-level)
2. [Architecture (Mid Level)](#2-architecture-mid-level)
3. [Producers (Mid Level)](#3-producers-mid-level)
4. [Consumers & Consumer Groups (Mid Level)](#4-consumers--consumer-groups-mid-level)
5. [Reliability & Delivery Semantics (Senior Level)](#5-reliability--delivery-semantics-senior-level)
6. [Performance & Tuning (Senior Level)](#6-performance--tuning-senior-level)
7. [Transactions & EOS (Senior Level)](#7-transactions--eos-senior-level)
8. [Operations & Monitoring (Senior Level)](#8-operations--monitoring-senior-level)
9. [Design Scenarios (Staff/Architect Level)](#9-design-scenarios-staffarchitect-level)
10. [Go + Kafka Specific Questions](#10-go--kafka-specific-questions)

---

## 1. Fundamentals (Junior Level)

---

**Q1: What is Apache Kafka and what problem does it solve?**

**A:**
Apache Kafka is a distributed event streaming platform designed for high-throughput, fault-tolerant, real-time data pipelines.

It solves the problem of **tight coupling** in distributed systems. Without Kafka:
- Service A must directly call Services B, C, D synchronously
- If any downstream service is slow or down, Service A is impacted
- Adding a new downstream service requires changing Service A's code

With Kafka:
- Service A publishes events to a topic
- Services B, C, D each subscribe independently
- Services are decoupled in time (B can be down and catch up later), space (don't need to know each other's addresses), and speed (each consumes at its own pace)

**Use cases:** Event streaming, audit logs, real-time analytics, microservice communication, CDC (Change Data Capture), IoT data pipelines.

---

**Q2: What is a Kafka Topic?**

**A:**
A topic is a named, durable, append-only log of records. It's the fundamental abstraction in Kafka.

Key characteristics:
- **Append-only:** You can add records but never modify or delete individual ones
- **Ordered within partitions:** Records have sequential offsets
- **Persistent:** Stored on disk with configurable retention (time-based or size-based)
- **Multi-subscriber:** Multiple independent consumer groups can read the same topic without interfering

Analogy: Think of a topic as a database table, but one that can only be inserted into, never updated, and multiple readers can query it independently at any point in history.

---

**Q3: What is a Kafka Partition?**

**A:**
A partition is a subdivision of a topic. Each topic is split into N partitions, where each partition is an independent ordered log.

**Why partitions exist:**
1. **Scalability:** Partitions can live on different brokers, distributing load
2. **Parallelism:** Multiple consumers can read different partitions simultaneously
3. **Ordering:** Kafka guarantees ordering within a partition (not across partitions)

**Key rules:**
- Partition count can be increased but never decreased
- A message is assigned to one partition based on its key hash (or round-robin if no key)
- Each partition has one leader broker and zero or more follower replicas

---

**Q4: What is an offset in Kafka?**

**A:**
An offset is a unique, sequential integer that identifies each record's position within a partition.

Key properties:
- Offsets start at 0 and increment monotonically
- Offsets are per-partition (two different partitions both have offset 0)
- Consumers commit their current offset to track their position
- You can seek to any offset to replay historical messages

Example: If a consumer's committed offset for partition 0 is 42, the next message it will receive is at offset 42. Offsets 0-41 have already been processed.

---

**Q5: What is a Kafka Broker?**

**A:**
A Kafka broker is a server that:
- Stores topic partitions on disk
- Handles producer write requests
- Serves consumer fetch requests
- Participates in cluster coordination (leader election, replication)

A Kafka cluster is typically 3 or more brokers. Each partition has one leader broker (handles reads/writes) and follower brokers (replicas). Producers and consumers only interact with the leader.

---

**Q6: What is the difference between a Kafka Topic and a Queue?**

**A:**

| Aspect | Kafka Topic | Traditional Queue |
|---|---|---|
| Consumption | Multiple consumer groups read independently | One consumer consumes the message |
| Retention | Configurable (days/forever) | Deleted after consumption |
| Replay | Yes — rewind to any offset | No |
| Ordering | Per-partition | Depends on implementation |
| Scalability | Horizontal via partitions | Limited |

**Key difference:** In a queue, consuming a message "removes" it. In Kafka, messages stay in the log and multiple independent consumers can each read them. Consumer Group A at offset 100 and Consumer Group B at offset 50 don't interfere with each other.

---

**Q7: What is a Consumer Group?**

**A:**
A consumer group is a set of consumer instances that collectively consume a topic, with each partition assigned to exactly one consumer in the group at any time.

Purpose: **Horizontal scaling of consumption.**

Rules:
- N partitions + M consumers: each partition goes to exactly one consumer
- If M > N: some consumers are idle (no benefit from adding more consumers than partitions)
- If M < N: some consumers handle multiple partitions
- When a consumer fails, its partitions are reassigned to surviving consumers (rebalance)

Two different groups read the same topic completely independently:
```
Group "analytics":   offset 50
Group "fulfillment": offset 200
```
Neither knows about the other.

---

**Q8: What is the difference between `OffsetNewest` and `OffsetOldest`?**

**A:**

- **OffsetNewest (-1):** The consumer starts consuming only NEW messages — messages produced after the consumer first connects. Historical messages are ignored. Use when you only care about real-time events.

- **OffsetOldest (-2):** The consumer starts from the very beginning of the log — all historical messages are replayed before new ones. Use when you need to process all historical data or test your consumer end-to-end.

These settings only apply when a consumer group has NO previously committed offset for a partition (i.e., first-ever run). Once an offset is committed, the consumer always resumes from the committed offset on restart.

---

## 2. Architecture (Mid Level)

---

**Q9: Explain Kafka's architecture. How do brokers, topics, and partitions relate?**

**A:**
```
Kafka Cluster
├── Broker 1 (server)
│   ├── Topic "orders",   Partition 0 [LEADER]
│   ├── Topic "orders",   Partition 1 [FOLLOWER]
│   └── Topic "payments", Partition 0 [LEADER]
│
├── Broker 2 (server)
│   ├── Topic "orders",   Partition 0 [FOLLOWER]
│   ├── Topic "orders",   Partition 1 [LEADER]
│   └── Topic "payments", Partition 0 [FOLLOWER]
│
└── Broker 3 (server)
    └── ...
```

- The **cluster** is a group of brokers working together
- A **topic** is a logical stream of events, split into partitions
- Each **partition** has one **leader** (on one broker) and optionally **followers** (on other brokers)
- Producers write to the leader; followers replicate from the leader
- If the leader broker fails, a follower is promoted to leader (failover)

---

**Q10: What is KRaft and why was it introduced?**

**A:**
KRaft (Kafka Raft) is a built-in consensus mechanism that replaces ZooKeeper for Kafka cluster coordination. Introduced in KIP-500, it became production-ready in Kafka 3.3 and is mandatory from Kafka 4.0.

**Why ZooKeeper was replaced:**
1. **Operational complexity:** Two separate systems (Kafka + ZooKeeper) to manage, monitor, and secure
2. **Scaling limitation:** ZooKeeper couldn't handle millions of partitions (metadata bottleneck)
3. **Slower operations:** Broker startup required ZooKeeper bootstrap; controller failover was slow
4. **Security:** Double the attack surface; difficult to apply uniform security policies

**KRaft benefits:**
- One system to manage
- Scales to millions of partitions
- Faster startup and controller failover
- Consistent security model

---

**Q11: What is an In-Sync Replica (ISR)?**

**A:**
The ISR (In-Sync Replica set) is the subset of partition replicas that are fully caught up with the leader.

A replica is in the ISR if it has:
1. Sent a heartbeat to the leader recently (within `replica.lag.time.max.ms`, default 30s)
2. Fetched all messages up to the end of the log (no meaningful lag)

**Why ISR matters:**
- `acks=all` means ALL ISR replicas must acknowledge a write (not all replicas)
- New leader can only be elected from the ISR (guarantees no data loss)
- If ISR shrinks below `min.insync.replicas`, producers with `acks=all` get an error

**Example:** If RF=3, min.isr=2, and one broker is down:
- ISR = 2 brokers
- Writes succeed (2 ≥ min.isr)
- If a second broker goes down: ISR = 1 < min.isr → writes fail with `NotEnoughReplicas`

---

**Q12: What is leader election in Kafka?**

**A:**
Leader election is the process of selecting a new partition leader when the current leader broker fails.

**Process in KRaft mode:**
1. Kafka controller detects the failed broker (via heartbeat timeout)
2. Controller identifies which ISR members are available for each partition the failed broker led
3. Controller selects one ISR member as the new leader (typically the first in the ISR list)
4. Controller updates metadata: "Partition P0 leader is now Broker 2"
5. Clients (producers/consumers) refresh metadata and connect to the new leader

**Unclean leader election (`unclean.leader.election.enable`):**
- If the ISR is empty (all in-sync replicas are also down), you can optionally elect a non-ISR replica
- Risk: this replica may be behind — you get data but potentially with gaps or duplicates
- Default: false (prefer unavailability over data loss)

---

**Q13: How does Kafka store data on disk?**

**A:**
Each partition maps to a **directory** on the broker's disk. Inside that directory:
- **Log segments:** large binary files (default 1GB each), named by their base offset
- **Index files:** sparse index mapping offsets → file positions (for fast seeking)
- **Time index files:** mapping timestamps → offsets

```
/var/kafka/data/orders-0/
  00000000000000000000.log   ← segment starting at offset 0
  00000000000000000000.index ← offset → file position
  00000000000000000000.timeindex
  00000000000001073741824.log ← next segment (when first reaches 1GB)
  ...
```

**Key design choices:**
- Sequential writes only (append-only) → optimal disk I/O (HDDs can sustain 100+ MB/s sequential)
- OS page cache: Kafka relies heavily on the OS page cache; messages are often served from RAM
- sendfile syscall: messages sent directly from page cache to network socket (zero-copy)

---

## 3. Producers (Mid Level)

---

**Q14: Explain the producer `acks` setting. What are the trade-offs?**

**A:**

`acks=0` (NoResponse):
- Producer sends, doesn't wait for any acknowledgement
- Fastest possible throughput
- No durability guarantee whatsoever
- Use for: non-critical telemetry, metrics where losing some data is acceptable

`acks=1` (WaitForLocal):
- Producer waits for the leader to write to its local log and respond
- Good balance between speed and durability
- Risk: if leader crashes after ACK but before replication, message is lost
- Use for: most production workloads with acceptable rare data loss

`acks=-1` (WaitForAll):
- Producer waits for ALL in-sync replicas to write and acknowledge
- Slowest option but highest durability
- With `min.insync.replicas=2`, survives one broker failure
- Use for: financial events, orders, anything where loss is unacceptable

---

**Q15: What is the difference between SyncProducer and AsyncProducer?**

**A:**

**SyncProducer:**
- `SendMessage()` blocks until Kafka returns the partition and offset
- Simple error handling (just check the returned error)
- Throughput limited by round-trip latency per message
- Use when: you need delivery confirmation before continuing (e.g., HTTP handler returning 200)

```go
partition, offset, err := producer.SendMessage(msg)  // blocks
```

**AsyncProducer:**
- `Input()` channel never blocks on network I/O
- Messages are buffered, batched, and sent asynchronously
- Results returned on `Successes()` and `Errors()` channels
- Much higher throughput (thousands to millions/s)
- More complex error handling (must drain channels or deadlock)
- Use when: maximum throughput matters, can handle results out-of-band

```go
producer.Input() <- msg                      // non-blocking
go func() { for s := range producer.Successes() { ... } }()
go func() { for e := range producer.Errors() { ... } }()
```

---

**Q16: What is an idempotent producer and when should you use it?**

**A:**
An idempotent producer ensures that retried messages are never written more than once to a partition.

**How it works:**
- Broker assigns a Producer ID (PID) to each producer
- Each message gets a monotonically increasing sequence number per (PID, partition)
- On retry, broker checks: "have I seen sequence N for PID P on partition X?"
  - No → write it
  - Yes → ACK but don't write again (it's a duplicate)

**Configuration:**
```go
config.Producer.Idempotent = true
config.Net.MaxOpenRequests = 1        // required
config.Producer.RequiredAcks = sarama.WaitForAll  // required
```

**Use when:** Any scenario where retries could cause duplicates is unacceptable:
- Payment processing, inventory management, billing

**Limitations:**
- Per-session only: new producer session = new PID = retries are new messages
- Per-partition: doesn't prevent duplicates across partitions
- For cross-partition/session deduplication: use transactions

---

**Q17: How does message compression work in Kafka?**

**A:**
Compression is applied at the **batch level** by the producer. A batch of messages is compressed as a unit before being sent to the broker.

Supported codecs: `gzip`, `snappy`, `lz4`, `zstd`

**Compression ratios for typical JSON:**
- Snappy: ~50-60% reduction (fast, CPU-light)
- LZ4: ~50% reduction (fastest)
- ZSTD: ~60-70% reduction (best ratio, more CPU)
- GZIP: ~60-70% reduction (slowest, but widely supported)

**Benefits:**
1. Less network bandwidth (producer → broker)
2. Less disk space on broker
3. Less network bandwidth (broker → consumer)

**Trade-offs:** CPU overhead on producer (compress) and consumer (decompress).

**Best practice:** Use snappy or lz4 for most workloads. Use zstd when storage cost is a concern.

---

## 4. Consumers & Consumer Groups (Mid Level)

---

**Q18: What is a rebalance and what triggers it?**

**A:**
A rebalance is the process of redistributing partition ownership among consumers in a group.

**Triggers:**
1. Consumer joins the group (scale out, rolling deploy)
2. Consumer leaves gracefully (`consumer.close()`)
3. Consumer is considered dead (missed heartbeat beyond `session.timeout.ms`)
4. Topic partition count increases
5. Group subscription changes

**What happens during rebalance:**
- All consumers in the group stop consuming (brief pause)
- Kafka coordinator reassigns partitions
- Consumers receive their new assignments and resume

**Impact:** Rebalances cause consumption pauses. Frequent rebalances = availability issue.

**Mitigation:**
- Use `StickyAssignor` to minimize partition movement
- Set generous `session.timeout.ms` (≥ 30s) for slow consumers
- Ensure heartbeat interval is ≤ 1/3 of session timeout

---

**Q19: Explain the difference between auto-commit and manual commit of offsets.**

**A:**

**Auto-commit (AutoCommit.Enable = true):**
- Kafka commits offsets on a timer (default every 5 seconds)
- Simple: zero code required
- Risk: offset committed before processing completes → crash = message loss
- Risk: offset not committed for several seconds → crash = reprocess last 5 seconds of messages
- Use for: non-critical consumers where occasional loss or replay is acceptable

**Manual commit (AutoCommit.Enable = false):**
- You call `session.MarkMessage(msg, "")` after successful processing
- Offset only advances after you confirm success
- Guarantees: at-least-once delivery (message processed at least once)
- Requires: idempotent handlers (safe to process same message twice)

**Micro-batching (common pattern):**
```go
count := 0
for msg := range claim.Messages() {
    process(msg)
    session.MarkMessage(msg, "")
    count++
    if count % 100 == 0 {
        session.Commit()  // flush every 100 messages
    }
}
```

---

**Q20: What happens if a consumer takes too long to process a message?**

**A:**
If a consumer stops sending heartbeats (because it's stuck processing), Kafka declares it dead after `session.timeout.ms` and triggers a rebalance.

**The session timeout dilemma:**
- Too short: false positives → spurious rebalances for slow-but-alive consumers
- Too long: slow failure detection → partitions unowned for longer after real crashes

**Solutions:**

1. **Increase session timeout:**
   ```
   session.timeout.ms = 60000   (60 seconds)
   heartbeat.interval.ms = 20000  (send heartbeat every 20s)
   ```

2. **Separate heartbeat from processing (background heartbeat):**
   sarama's consumer group automatically heartbeats in the background — the heartbeat goroutine runs independently from ConsumeClaim. This is why it's safe to do slow processing in ConsumeClaim — the heartbeat keeps coming.

3. **Process asynchronously with a worker pool:**
   ```go
   for msg := range claim.Messages() {
       workerPool.Submit(msg)  // non-blocking, worker handles slow processing
   }
   ```

---

**Q21: What is `isolation.level` and why does it matter?**

**A:**
The `isolation.level` setting controls which messages a consumer sees relative to Kafka transactions.

**`read_uncommitted` (default):**
- Consumer sees all messages, including those from in-progress or aborted transactions
- Simpler, lower latency
- Risk: may process transactional messages that are later aborted

**`read_committed`:**
- Consumer only sees:
  1. Non-transactional messages
  2. Messages from successfully COMMITTED transactions
- Never sees messages from ABORTED or IN-PROGRESS transactions
- Required for exactly-once consumer behavior

**When to use `read_committed`:**
Any consumer that reads from topics written by transactional producers in your EOS pipeline.

---

## 5. Reliability & Delivery Semantics (Senior Level)

---

**Q22: Explain the three delivery semantics in depth: at-most-once, at-least-once, and exactly-once.**

**A:**

**At-most-once:**
- Commit offset BEFORE processing
- If crash: offset advanced, message never processed → **message lost**
- No duplicates guaranteed
- Use for: non-critical events (logging, metrics)

**At-least-once:**
- Process THEN commit offset
- If crash before commit: offset NOT advanced → message **redelivered on restart**
- May process same message twice → handler must be idempotent
- This is the **production default** for most systems
- Idempotent handler example: `INSERT ... ON CONFLICT DO NOTHING`

**Exactly-once:**
- Use Kafka Transactions to atomically commit: (output write) + (input offset advance)
- Either BOTH happen, or NEITHER happens
- No loss, no duplicates
- Requires: transactional producer + `read_committed` consumer
- Cost: higher latency, lower throughput, more complex implementation
- Use for: financial systems, billing, inventory where duplicates cause real-world harm

**The honest answer about exactly-once:**
"Exactly-once within Kafka" is achievable. "Exactly-once end-to-end" (including external database writes) requires idempotent side effects or application-level deduplication. Kafka EOS cannot prevent duplicate database writes if the database commit succeeds but the Kafka transaction commit fails.

---

**Q23: What is the "at-least-once delivery" guarantee and how do you handle duplicates?**

**A:**
At-least-once means Kafka guarantees every message will be delivered to the consumer, but may deliver it more than once (e.g., after a crash + restart before offset commit).

**Handling duplicates — strategies:**

1. **Natural idempotency:** Some operations are safe to repeat
   ```go
   db.Exec("INSERT INTO events (id, data) VALUES (?, ?) ON CONFLICT DO NOTHING", id, data)
   ```

2. **Deduplication via external store:**
   ```go
   if redis.SetNX(ctx, "processed:"+msg.ID, 1, 24*time.Hour) {
       // Not seen before, process it
       process(msg)
   }
   // If SetNX returns false, it's a duplicate, skip it
   ```

3. **Idempotency via upsert:**
   ```go
   db.Exec("INSERT INTO orders (id, status) VALUES (?, ?) ON CONFLICT (id) DO UPDATE SET status = EXCLUDED.status", id, status)
   ```

4. **Business logic idempotency:**
   ```go
   if order.Status != "shipped" {
       order.Status = "shipped"  // Setting status is idempotent; calling it twice has same effect
       db.Save(order)
   }
   ```

---

**Q24: How does Kafka prevent data loss with `acks=all` and `min.insync.replicas`?**

**A:**
These two settings work together to provide a durability guarantee:

```
replication.factor = 3        (3 copies of each partition)
min.insync.replicas = 2       (at least 2 must write before ACK)
acks = all                    (producer waits for all ISR to ACK)
```

**Timeline:**
```
Producer sends message M
  → Leader writes M to disk
  → Follower-1 replicates M (syncs within lag threshold) → ISR
  → Follower-2 replicates M (syncs within lag threshold) → ISR
  → ISR size = 3 ≥ min.isr = 2 → ACK sent to producer

Producer now knows: at least 2 brokers have M on disk
```

**Failure scenario:**
```
Leader crashes AFTER ACK:
  - Follower-1 is promoted to leader (it has M)
  - M is not lost ✅

Leader crashes WITH Follower-2:
  - Only Follower-1 is alive
  - ISR = 1 < min.isr = 2
  - New writes are REJECTED (producers get NotEnoughReplicas error)
  - This is correct: better to refuse new writes than silently lose data
  - Cluster becomes unavailable for writes until a broker comes back
```

The trade-off: higher durability = lower availability under multi-broker failure.

---

**Q25: What causes message ordering issues and how do you fix them?**

**A:**

**Root cause:** Kafka only guarantees ordering WITHIN a partition. Cross-partition ordering is not guaranteed.

**Common ordering violations:**

1. **No message key:**
   ```
   Producer sends: A → P0, B → P1, C → P0
   Consumer processes P0: A, C
   Consumer processes P1: B
   → Cross-partition: A, C, B (not the original order A, B, C)
   ```
   Fix: use a consistent key → same entity always goes to same partition

2. **Multiple partitions per entity without key:**
   ```
   Order #42: created → P0 (no key, round-robin)
   Order #42: paid    → P1 (different partition!)
   Consumer sees "paid" before "created" → logic error
   ```
   Fix: use order_id as key → all events for order #42 go to same partition

3. **Multiple consumer threads processing same partition:**
   ```
   Consumer has 8 goroutines all processing partition 0 messages
   Thread 1 processes offset 5, Thread 2 processes offset 4
   → Out-of-order processing
   ```
   Fix: Kafka's ConsumeClaim runs one goroutine per partition (sequential by default)

4. **Retries with `max.in.flight.requests > 1` (without idempotency):**
   ```
   Request 1 (msg A) → fails in transit
   Request 2 (msg B) → succeeds (arrives at broker before A's retry)
   Request 1 retry  → now arrives → B before A in the log
   ```
   Fix: enable idempotent producer OR set `max.in.flight.requests = 1`

---

## 6. Performance & Tuning (Senior Level)

---

**Q26: How would you tune Kafka for maximum throughput?**

**A:**

**Producer side:**
```
compression.type = snappy          # 50-80% payload reduction
batch.size = 1MB                   # larger batches = fewer requests
linger.ms = 10-50                  # wait for more messages to accumulate
max.in.flight.requests.per.connection = 5  # pipeline requests
acks = 1                           # reduce ACK latency (accept some durability risk)
buffer.memory = 256MB              # larger in-memory buffer
```

**Consumer side:**
```
fetch.min.bytes = 1MB              # wait for larger fetch responses
fetch.max.wait.ms = 500            # max wait for fetch.min.bytes
max.poll.records = 2000            # more records per poll
```

**Broker side:**
```
num.network.threads = 8            # more threads for network I/O
num.io.threads = 16                # more threads for disk I/O
socket.send.buffer.bytes = 102400  # OS socket buffer size
log.flush.interval.messages = 10000 # batch disk flushes
```

**Network & hardware:**
- Use SSDs for broker storage
- Separate disks for data and logs
- Dedicated network interface with jumbo frames
- Multiple partitions across multiple disks (I/O parallelism)

**Practical throughput numbers:**
- Sync producer: ~10-50k msg/s per goroutine
- Async producer: ~100k-1M msg/s (with batching + compression)
- Consumer: ~50-500k msg/s depending on processing logic

---

**Q27: What is consumer lag and how do you monitor and reduce it?**

**A:**

**What it is:**
```
Lag = (latest_offset_in_partition) - (consumer_committed_offset)
```
Lag represents how many messages the consumer is "behind" the producer.

**How to monitor:**
```bash
# CLI
kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group my-group

# Output:
# TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
# orders  0          850             1000            150
# orders  1          920             1000            80
```

**Prometheus metric:** `kafka_consumer_group_lag{topic, partition, group}`

**Reducing lag:**

1. **Scale consumers** (up to max = partition count):
   ```
   6 partitions, currently 2 consumers → add 4 more consumers
   Each consumer handles 1 partition → 3x throughput improvement
   ```

2. **Optimize the handler** (reduce processing time per message):
   ```go
   // Slow: synchronous database write per message
   db.Save(order)  // 10ms per message → max 100 msg/s

   // Fast: batch database writes
   buffer = append(buffer, order)
   if len(buffer) >= 100 {
       db.BatchSave(buffer)  // 10ms for 100 messages → 10,000 msg/s
   }
   ```

3. **Increase partition count** (then add consumers to match)

4. **Move I/O-bound work async:**
   ```go
   // Consumer marks quickly, work happens in a goroutine pool
   workPool <- msg.Value
   session.MarkMessage(msg, "")  // fast path
   ```

---

**Q28: How does Kafka achieve such high throughput?**

**A:**

Kafka achieves millions of messages/second through several key design decisions:

1. **Sequential disk writes only:**
   - Appending to a log is sequential I/O
   - Modern HDDs: 100 MB/s sequential vs 1 MB/s random
   - SSDs: 500+ MB/s sequential
   - Kafka never does random writes

2. **OS page cache:**
   - Kafka trusts the OS to buffer writes (doesn't manage its own cache)
   - Active topics often served entirely from RAM
   - No double buffering (Kafka cache + OS cache)

3. **Zero-copy (sendfile):**
   ```
   Normal: Disk → Kernel buffer → User buffer → Kernel buffer → Network socket (4 copies)
   Zero-copy: Disk → Kernel buffer → Network socket (2 copies, no user space)
   ```
   Reducing copies cuts CPU and memory bandwidth significantly.

4. **Batching:**
   - Producers batch many messages into one network request
   - Consumers fetch batches (not one message at a time)
   - Compression works better on larger batches

5. **No message acknowledgements from consumers:**
   - Consumers track their own offset
   - Broker doesn't track "has this consumer processed this?"
   - This eliminates per-message ACK overhead

6. **Partitioning:**
   - Multiple partitions on multiple brokers = parallel I/O
   - Multiple consumer threads = parallel processing

---

## 7. Transactions & EOS (Senior Level)

---

**Q29: Explain Kafka Transactions. How do they work?**

**A:**

Kafka Transactions provide atomic, multi-partition writes: all writes in a transaction commit or abort together.

**Key components:**
- **Transactional Producer:** has a stable `transactional.id`; can `BeginTxn()`, `CommitTxn()`, `AbortTxn()`
- **Transaction Coordinator:** a special broker that manages transaction state per `transactional.id`
- **Transaction Log:** internal Kafka topic `__transaction_state` that durably records transaction state
- **Consumer isolation:** `read_committed` consumers only see committed transactional messages

**Write flow:**
```
1. producer.BeginTxn()
     → Broker assigns epoch E to transactional.id T
     → Transaction state: ONGOING

2. producer.SendMessage(topic_A, msg_1)
     → Message written to partition, tagged as transactional (pid, epoch, seq)
     → NOT visible to read_committed consumers yet

3. producer.SendMessage(topic_B, msg_2)
     → Same, tagged with same transaction

4. producer.CommitTxn()
     → Broker writes COMMITTED marker to all involved partitions
     → Consumer with read_committed now sees msg_1 and msg_2 atomically

Or:
4. producer.AbortTxn()
     → Broker writes ABORTED marker
     → msg_1 and msg_2 are never visible to read_committed consumers
```

---

**Q30: What is "Zombie Fencing" in the context of Kafka Transactions?**

**A:**
Zombie fencing prevents two producer instances with the same `transactional.id` from writing simultaneously (which would cause data corruption).

**The zombie problem:**
```
Scenario: Producer P1 is running. Due to a GC pause, Kafka thinks P1 is dead.
Kafka starts a new instance P2 with the same transactional.id.
Now both P1 (zombie — still alive) and P2 are writing.
→ Data corruption risk
```

**The fencing mechanism:**
Each producer epoch is stored by the broker:
```
P1 starts: transactional.id="order-proc", epoch=1
P2 starts: transactional.id="order-proc", epoch=2  ← broker increments epoch

P1 (zombie) tries to write with epoch=1:
  Broker: "Current epoch is 2, not 1 → FENCE P1"
  P1 gets ProducerFencedException → should log error and stop

P2 continues writing with epoch=2 ✅
```

**In Go (sarama):** If your producer gets `ProducerFencedException`, it means another instance with the same `transactional.id` has taken over. The correct response is to shut down gracefully, not retry.

---

**Q31: What is the Consume-Transform-Produce (CTP) pattern?**

**A:**
CTP is the core pattern for Kafka stream processing: read from input topic → transform the data → write to output topic.

**The challenge:** ensuring exactly-once semantics across read and write.

**Without EOS:**
```
1. Consume message from "orders" at offset 5
2. Enrich/transform the message
3. Write result to "enriched-orders" ✅
4. CRASH before committing offset 6 on "orders"
5. Restart: re-read offset 5 → write DUPLICATE to "enriched-orders"
```

**With EOS (transactions):**
```go
producer.BeginTxn()
// Step 1: Write to output topic (buffered in transaction)
producer.SendMessage(enrichedOrdersTopic, enrichedMsg)
// Step 2: Register the input offset as part of this transaction
producer.AddOffsetsToTxn(map[string][]*OffsetMeta{
    "orders": {{Partition: 0, Offset: 6}},  // next offset = current + 1
}, "my-consumer-group")
// Step 3: Commit atomically
producer.CommitTxn()
// Both the output write and the offset advance happen atomically.
// If crash before CommitTxn: transaction aborts, no output, offset not advanced, message redelivered.
```

---

## 8. Operations & Monitoring (Senior Level)

---

**Q32: What metrics should you monitor in a Kafka cluster?**

**A:**

**Top-priority metrics:**

| Metric | What it tells you | Alert when |
|---|---|---|
| `kafka.consumer.group.lag` | How far behind consumers are | Rising trend or > threshold |
| `kafka.producer.record.error.rate` | Producer failures | Any non-zero value |
| `kafka.server.replica.under_replicated_partitions` | Replication health | > 0 |
| `kafka.server.active.controller.count` | Controller health | != 1 |
| `kafka.network.request.total.time.ms` (p99) | Broker request latency | > your SLA |
| `kafka.server.bytes.in.per.sec` | Write throughput | Sudden drops |

**Consumer-specific:**
- Consumer lag per group/topic/partition (rising lag = can't keep up)
- Number of rebalances (frequent rebalances = instability)
- Processing duration per message (slow handlers)

**Broker-specific:**
- Disk usage per broker (% full)
- JVM heap usage (GC pressure causes pauses → triggers rebalances)
- Network throughput per broker
- Request queue size (high queue = broker under load)

---

**Q33: How do you handle a Kafka broker failure?**

**A:**

**Immediate impact:**
- Partitions for which the failed broker was leader become unavailable briefly
- Controller detects the failure (heartbeat timeout, default 10-30s)
- Controller elects new leaders from ISR for affected partitions
- Producers/consumers refresh metadata and reconnect to new leaders
- Total downtime per partition: typically seconds to tens of seconds

**Recovery steps:**

1. **Let Kafka handle it automatically:** if RF ≥ 2 and ISR is healthy, Kafka self-heals
2. **Monitor:** check if all partitions have leaders (`under_replicated_partitions = 0`)
3. **Investigate:** check broker logs for the root cause (disk full, OOM, network issue)
4. **Restart the broker:** Kafka rebalances partitions back automatically
5. **Watch for incomplete replication:** after restart, the broker catches up; verify ISR restores

**Production tips:**
- Set `auto.leader.rebalance.enable = true` to rebalance preferred leaders after restart
- Use `kafka-preferred-replica-election.sh` to manually trigger preferred replica election
- Monitor `UnderReplicatedPartitions` metric — any non-zero value needs attention

---

**Q34: How do you increase the throughput of a Kafka cluster?**

**A:**

**Producer-side:**
- Enable compression (snappy/lz4): reduces network and disk I/O 50-80%
- Increase `batch.size` and `linger.ms`: larger batches, fewer requests
- Use async producer for fire-and-forget workloads
- Reduce `acks` from `all` to `1` if you can tolerate rare data loss

**Consumer-side:**
- Add more consumer instances (up to partition count)
- Increase `max.poll.records` for larger fetches
- Batch database writes inside the consumer handler

**Broker-side:**
- Add more brokers to the cluster
- Increase `num.io.threads` and `num.network.threads`
- Use SSDs instead of HDDs (much lower latency)
- Distribute partitions across multiple disks (`log.dirs`)

**Topic-side:**
- Increase partition count (allows more consumers and broker parallelism)
- Note: increasing partitions requires coordination and may cause brief rebalance

---

**Q35: How do you perform a zero-downtime Kafka upgrade?**

**A:**

**Rolling upgrade procedure:**

1. Check compatibility matrix (new broker version ↔ old client versions)
2. Set `inter.broker.protocol.version` to the CURRENT version (don't upgrade yet)
3. One at a time, for each broker:
   a. Reassign partitions away from this broker (optional but safe)
   b. Stop the broker
   c. Upgrade the Kafka binary
   d. Start the broker
   e. Wait for it to rejoin the cluster and catch up (ISR restoration)
   f. Verify: `under_replicated_partitions = 0` before proceeding
4. After all brokers upgraded, update `inter.broker.protocol.version` to new version
5. Update `log.message.format.version` to new version

**Key principle:** Old clients can connect to new brokers (backward compatible). This is why you can upgrade brokers before clients.

---

## 9. Design Scenarios (Staff/Architect Level)

---

**Q36: Design a real-time order processing system using Kafka.**

**A:**

```
Architecture:

Order Service ──▶ Topic: "orders" (6 partitions)
                         │
               ┌─────────┼─────────────────┐
               ↓         ↓                 ↓
         Payment      Inventory      Notification
         Service      Service        Service
         Group:       Group:         Group:
         "payment"    "inventory"    "notification"
               │
               ▼
         Topic: "payments"
               │
         ┌─────┴───────┐
         ↓             ↓
    Fulfillment    Analytics
    Service        Service
```

**Topic design:**
- `orders` — 6 partitions, key=customer_id (ordering per customer), RF=3
- `payments` — 6 partitions, key=order_id, RF=3
- `notifications` — 3 partitions, key=customer_id, RF=3
- `orders.dlq` — 3 partitions (dead letter queue for failed processing)

**Delivery guarantee:** at-least-once with idempotent handlers.
Each service uses the order_id/payment_id as idempotency key in its database.

**Failure handling:**
- Transient failures → retry with exponential backoff (3-5 attempts)
- Permanent failures (bad data, unknown order) → DLQ + alert

**Monitoring:** consumer lag alert per service group, DLQ message count alert.

---

**Q37: How would you implement the Saga pattern with Kafka?**

**A:**
The Saga pattern coordinates a distributed transaction as a sequence of local transactions, each publishing an event that triggers the next step.

**Choreography-based Saga (no central coordinator):**

```
Order Service:
  1. Create order (local DB commit)
  2. Publish "OrderCreated" to "orders" topic

Payment Service (consumes "orders"):
  3. Process payment (local DB commit)
  4a. Publish "PaymentSucceeded" OR "PaymentFailed" to "payments" topic

Inventory Service (consumes "payments"):
  5a. If PaymentSucceeded: Reserve inventory
      Publish "InventoryReserved" OR "InventoryFailed"
  5b. If PaymentFailed: No action needed

Order Service (consumes "payments", "inventory"):
  6a. If PaymentFailed: Cancel order, refund
      Publish "OrderCancelled" (compensating transaction)
  6b. If InventoryFailed: Refund payment, cancel order
      Publish "OrderCancelled" + trigger PaymentRefund
```

**Key design decisions:**
- Each step is idempotent
- Compensating transactions handle rollback
- Kafka's log preserves full event history for audit/replay
- Topic per entity (orders, payments, inventory) with appropriate keys

---

**Q38: How would you handle schema evolution in a high-traffic Kafka system?**

**A:**

**The challenge:** Producer changes the message format; consumers must still work.

**Strategy 1: Forward compatibility (add only, never remove)**
```json
// v1 message
{"order_id": "123", "amount": 100}

// v2 message (added fields with defaults)
{"order_id": "123", "amount": 100, "currency": "USD", "region": "US"}
// v1 consumers ignore unknown fields ✅
```

**Strategy 2: Envelope versioning**
```json
{
  "version": "2",
  "payload": {"...v2 fields..."}
}
```
Consumer branches on version:
```go
switch env.Version {
case "1": handleV1(env.Payload)
case "2": handleV2(env.Payload)
}
```

**Strategy 3: Confluent Schema Registry (production best practice)**
- Each schema version registered with compatibility rules
- `BACKWARD` compatibility: new consumer can read old messages
- `FORWARD` compatibility: old consumer can read new messages
- `FULL` compatibility: both
- Registry rejects schemas that break compatibility rules
- Message includes schema ID (4 bytes) → consumer looks up schema automatically

**Migration approach:**
1. Register new schema with BACKWARD compatibility
2. Deploy consumers that handle both v1 and v2
3. Update producers to emit v2
4. After retention period (old messages expire), simplify consumers to v2 only

---

**Q39: How do you handle a "hot partition" problem?**

**A:**
A hot partition occurs when one partition receives disproportionately more traffic than others, becoming a throughput bottleneck.

**Causes:**
- Uneven key distribution (e.g., "US" as region key: 90% of traffic)
- Timestamp-based key: all recent activity → same partition
- Null key with sequential message IDs: many messages → same partition

**Solutions:**

1. **Rethink the key:** Use higher-cardinality keys
   ```
   Bad:  key = country (only 200 values globally)
   Good: key = user_id (millions of unique values)
   ```

2. **Key salting:** Add a suffix to distribute within a logical group
   ```go
   salt := rand.Intn(numSalts)  // e.g., 0-9
   key = fmt.Sprintf("%s-%d", userID, salt)
   // Consumer must aggregate results from all salted partitions
   ```

3. **Separate hot entities:** Give them their own topic
   ```
   Topic "orders-large-accounts" (key=account_id, for top 1% accounts)
   Topic "orders" (key=account_id, for normal accounts)
   ```

4. **Custom partitioner:** Override partition assignment logic
   ```go
   func customPartitioner(topic string, key, value []byte, numPartitions int32) int32 {
       // Custom logic to spread hot keys
   }
   ```

---

## 10. Go + Kafka Specific Questions

---

**Q40: Why use `github.com/IBM/sarama` instead of `confluent-kafka-go`?**

**A:**

**sarama (IBM/sarama, formerly Shopify/sarama):**
- Pure Go implementation
- No CGO dependency
- Easy cross-compilation (single static binary)
- More Idiomatic Go API
- Larger Go community usage
- Better for pure Go shops

**confluent-kafka-go:**
- CGO wrapper around librdkafka (C library)
- librdkafka is battle-tested, high-performance C implementation
- Better parity with other language clients (Java, Python)
- Required for some advanced features (Schema Registry integration)
- Harder to cross-compile (requires C toolchain)

**Choose sarama when:**
- Pure Go binary is important (containers, cross-compilation)
- Your team is Go-centric
- You don't need Schema Registry integration

**Choose confluent-kafka-go when:**
- You need maximum throughput (librdkafka is highly optimized)
- You use Confluent's ecosystem (Schema Registry, Cloud)
- C dependency in your Docker image is acceptable

---

**Q41: How do you properly handle context cancellation in a Kafka consumer in Go?**

**A:**

```go
// Context propagation through the consumer lifecycle:

func (h *Handler) ConsumeClaim(
    session sarama.ConsumerGroupSession,
    claim sarama.ConsumerGroupClaim,
) error {
    for {
        select {
        case msg, ok := <-claim.Messages():
            if !ok {
                return nil  // channel closed (rebalance or shutdown)
            }
            // Pass session context to your handler for cancellation propagation
            if err := h.processMessage(session.Context(), msg); err != nil {
                // handle error
                continue
            }
            session.MarkMessage(msg, "")

        case <-session.Context().Done():
            // Session context cancelled — either shutdown or rebalance
            return nil
        }
    }
}

func (h *Handler) processMessage(ctx context.Context, msg *sarama.ConsumerMessage) error {
    // Respect context in all blocking operations:
    result, err := http.NewRequestWithContext(ctx, "POST", url, body)
    // ...
    db.QueryContext(ctx, "SELECT ...", args...)
    // ...
}
```

Key principle: `session.Context()` is cancelled when the group session ends (rebalance or shutdown). Propagate it to ALL downstream calls to ensure clean cancellation.

---

**Q42: How do you test Kafka consumers and producers in Go?**

**A:**

**Unit testing with sarama mocks:**
```go
import "github.com/IBM/sarama/mocks"

func TestOrderHandler(t *testing.T) {
    // Mock producer
    config := sarama.NewConfig()
    mockProducer := mocks.NewSyncProducer(t, config)

    // Expect one message to be sent
    mockProducer.ExpectSendMessageAndSucceed()

    handler := &OrderHandler{producer: mockProducer}
    err := handler.Process(testMessage)

    assert.NoError(t, err)
    mockProducer.Close()
}
```

**Integration testing with Docker:**
```go
func TestIntegration(t *testing.T) {
    if testing.Short() {
        t.Skip("skipping integration test")
    }

    // Start Kafka via docker-compose or testcontainers
    ctx := context.Background()
    kafkaContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
        ContainerRequest: testcontainers.ContainerRequest{
            Image:        "confluentinc/cp-kafka:7.7.0",
            ExposedPorts: []string{"9092/tcp"},
        },
        Started: true,
    })

    broker, _ := kafkaContainer.Endpoint(ctx, "")
    // ... test with real Kafka
}
```

**Testing consumer group logic:**
```go
func TestConsumerGroup(t *testing.T) {
    // Produce test messages
    producer := setupTestProducer(t)
    producer.SendMessage(testMsg)

    // Start consumer
    group := setupConsumerGroup(t)

    // Use ready channel to wait for partition assignment
    <-group.Ready()

    // Assert handler was called with expected message
    assert.Equal(t, expectedPayload, capturedMessage)
}
```

---

**Q43: What is the correct way to gracefully shut down a Kafka consumer in Go?**

**A:**

```go
func main() {
    client, _ := sarama.NewConsumerGroup(brokers, groupID, config)

    // 1. Context for coordinated shutdown
    ctx, cancel := context.WithCancel(context.Background())

    // 2. WaitGroup to track the consume loop
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        for {
            if err := client.Consume(ctx, topics, handler); err != nil {
                if ctx.Err() != nil {
                    return  // expected shutdown
                }
                log.Printf("error: %v", err)
            }
        }
    }()

    // 3. Listen for OS shutdown signal
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    <-sigCh

    // 4. Cancel context — stops new message delivery
    cancel()

    // 5. Wait with bounded timeout for in-flight processing to complete
    done := make(chan struct{})
    go func() { wg.Wait(); close(done) }()

    select {
    case <-done:
        log.Println("clean shutdown")
    case <-time.After(30 * time.Second):
        log.Println("forced shutdown after timeout")
    }

    // 6. Close the client — commits pending offsets
    client.Close()
}
```

**Critical steps:**
1. Cancel context (stops new message delivery from Kafka)
2. Wait for in-flight handlers to complete (not abrupt)
3. Close the client (commits pending marked offsets, releases connection)

---

**Q44: How do you handle errors in a Kafka consumer without causing infinite loops?**

**A:**

```go
func (h *Handler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    for msg := range claim.Messages() {
        var lastErr error

        // Retry with backoff for transient errors
        for attempt := 1; attempt <= 3; attempt++ {
            lastErr = h.process(session.Context(), msg)
            if lastErr == nil {
                break
            }

            // Don't retry permanent errors (bad data, validation)
            if isPermanent(lastErr) {
                break
            }

            select {
            case <-time.After(time.Duration(attempt) * 100 * time.Millisecond):
            case <-session.Context().Done():
                return nil
            }
        }

        if lastErr != nil {
            // Send to DLQ for human investigation
            h.sendToDLQ(msg, lastErr)
            // ALWAYS commit the offset — even failed messages
            // Without this commit, we loop forever on the same bad message
            session.MarkMessage(msg, "failed")
        } else {
            session.MarkMessage(msg, "success")
        }
    }
    return nil
}
```

**Key insight:** Always commit the offset, even for permanently failed messages.
If you don't commit, the failed message will be redelivered after every restart — infinite loop.
The DLQ captures the failure for investigation; the offset commit moves the consumer forward.

---

## Quick Reference Cheat Sheet

### Essential Kafka Configs

```go
// PRODUCER - Safe defaults
config.Producer.RequiredAcks = sarama.WaitForAll    // durability
config.Producer.Retry.Max = 5                        // resilience
config.Producer.Return.Successes = true              // required for sync
config.Producer.Compression = sarama.CompressionSnappy // efficiency
config.Producer.Idempotent = true                    // no duplicates on retry
config.Net.MaxOpenRequests = 1                       // required with idempotent

// CONSUMER - Safe defaults
config.Consumer.Offsets.AutoCommit.Enable = false    // manual commit
config.Consumer.Offsets.Initial = sarama.OffsetNewest // start from latest
config.Consumer.Group.Session.Timeout = 30 * time.Second
config.Consumer.Group.Heartbeat.Interval = 10 * time.Second
config.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{
    sarama.NewBalanceStrategyRoundRobin(),
}
```

### The Production Checklist Summary

```
Producers:
  ✅ acks=all for critical data
  ✅ Idempotent producer enabled
  ✅ Compression enabled
  ✅ defer producer.Close()

Consumers:
  ✅ Auto-commit disabled
  ✅ Manual commit after processing
  ✅ Idempotent handlers (safe to run twice)
  ✅ Graceful shutdown (signal handling + WaitGroup)
  ✅ DLQ for permanent failures
  ✅ Error logged with offset/partition for debugging

Topics:
  ✅ Replication factor ≥ 3 (production)
  ✅ min.insync.replicas = 2 (production)
  ✅ Partition count matches parallelism needs

Monitoring:
  ✅ Consumer lag alerts
  ✅ Producer error rate alerts
  ✅ Under-replicated partitions alerts
  ✅ DLQ message count alerts
```
