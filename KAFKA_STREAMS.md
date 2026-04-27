# Kafka Streams — Concepts & Comparison

## 1. Plain Consumer vs Kafka Streams

### Current team model (Plain Consumer)
```
read event → process → write result to DB or another topic → done
```
Each event is handled independently. Any state (e.g. "how many events did user-1 send in the last 5 minutes?") must live in an external system like a relational DB or Redis.

### Kafka Streams model
```
stream → transform → aggregate → emit
```
The library manages a local, fault-tolerant state store (backed by RocksDB + Kafka changelog topic). You declare *what* you want (a topology), and the framework handles threading, partitioning, state recovery, and exactly-once guarantees.

| Aspect | Plain Consumer | Kafka Streams |
|---|---|---|
| **State handling** | External DB / cache required | Built-in local state store (RocksDB) |
| **Complexity** | Low for stateless; high for stateful | Higher upfront, lower for complex pipelines |
| **Scaling** | Manual partition assignment | Automatic — tasks scale with partitions |
| **Windowing** | Must implement manually with timers | First-class citizen (`windowedBy`) |
| **Fault tolerance** | Manual offset management | Automated via changelog topics |
| **Use cases** | Simple enrichment, routing, filtering | Aggregations, joins, windowed analytics |

---

## 2. Core Concepts

### Topology
The directed acyclic graph (DAG) of processing steps. You define it programmatically using `StreamsBuilder`, and it is immutable once built. Each node is either a **source** (reads from a topic), a **processor** (transforms or aggregates), or a **sink** (writes to a topic).

### KStream vs KTable
- **KStream** — an unbounded, ever-growing stream of individual events. Every record is an independent fact. Think: event log.
- **KTable** — a changelog stream interpreted as a table. Each record is an *upsert* by key. The latest value per key is the "current state". Think: database table.

A windowed aggregation (`KTable<Windowed<K>, V>`) is a KTable where the key also encodes the time window.

### Stateless vs Stateful operations
- **Stateless**: `map`, `filter`, `flatMap`, `selectKey` — process each record independently, no memory required.
- **Stateful**: `count`, `aggregate`, `reduce`, `join` — maintain state across records. Require a state store.

### State Store (RocksDB)
An embedded key-value store (backed by RocksDB on disk) that lives inside the application process. Kafka Streams writes every state update to it locally *and* publishes it to a Kafka changelog topic for replication.

### Changelog Topics
Internal Kafka topics (auto-created, named `<app-id>-<store-name>-changelog`) that replicate every state-store mutation. On restart or rebalance, Kafka Streams rebuilds state by replaying the changelog — no external DB needed.

---

## 3. Windowing

### Tumbling Windows
Fixed-size, non-overlapping windows. Each event belongs to exactly one window.

```
|---5min---|---5min---|---5min---|
 window 1    window 2    window 3
```

This project uses `TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(5))`.

### Event Time vs Processing Time
- **Processing time**: when the record arrives at the application. Simple, but late events are lost or mis-attributed.
- **Event time**: the timestamp embedded in the record itself (e.g. `event.timestamp`). More accurate for business logic; requires the producer to set the timestamp correctly.

Kafka Streams defaults to using the record's Kafka timestamp (set by the producer). Our `UserEvent.timestamp` field could be used via a custom `TimestampExtractor` for true event-time semantics.

### Why is windowing hard with plain consumers?
With a plain consumer you would need to:
1. Store every incoming event in a DB with its timestamp.
2. Run a periodic job (or trigger on every event) to query "events per user in the last 5 minutes".
3. Handle clock skew, late arrivals, and window boundary edge cases yourself.
4. Manage cleanup of old records.

This is error-prone, hard to test, and adds external infrastructure. Kafka Streams handles all of it internally.

---

## 4. Exactly-once & State

### State consistency
Kafka Streams can run in exactly-once mode (`processing.guarantee=exactly_once_v2`). It wraps the read-process-write cycle in a Kafka transaction: the consumer offset commit, state store update, and output record produce all happen atomically.

### Why no external DB is needed
The state store is local (fast, no network), automatically backed up to Kafka changelog topics (durable), and rebuilt from those topics on restart. There is no single point of failure and no extra infrastructure dependency.

### What happens on restart
1. The application starts and discovers its assigned partitions.
2. For each partition, Kafka Streams replays the changelog topic into the local RocksDB store.
3. Once caught up, normal processing resumes.
4. Depending on standby replicas config, this can be near-instant.

---

## 5. When NOT to Use Kafka Streams

### Plain consumer is enough when:
- You only do stateless transforms: filter, enrich, route.
- Each event can be processed independently.
- There is no time-based aggregation required.
- The team already has simple consumer code that works.

### Kafka Streams may be overkill when:
- You have very low throughput (< a few events/sec) — the overhead is unnecessary.
- You need complex cross-stream joins that are easier to express in SQL (consider ksqlDB instead).
- Your team has no Java/JVM expertise and the pipeline is simple.
- You need a fully managed solution — consider Flink, Spark Streaming, or a cloud-managed service.
