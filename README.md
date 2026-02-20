# Flourine

Unified data infrastructure for event streaming, CDC, and schema management.

## The Problem

Data infrastructure is built incidentally, not deliberately. Kafka started as a log. Debezium bolted CDC onto Kafka Connect. Sqoop moved batches between HDFS and databases. Avro, Parquet, and Iceberg each solved a serialization or table format problem in isolation. The result is a stack of independently evolved systems glued together by YAML and prayer: ZooKeeper (or KRaft), Connect clusters, a schema registry, format converters, catalog sync jobs, and an operator who understands all of them.

Schema management is the worst casualty. Confluent Schema Registry runs as a separate service, enforces compatibility only at registration time (never on the broker), and requires clients to manually look up schema IDs. Your code defines a struct, your build generates an Avro schema, you `curl` it into the registry, note the ID, and hard-code it into your producer config. When a field is renamed or removed, you manage the migration outside the system — the registry has no concept of renames or deletions. The schema is an artifact alongside your code rather than derived from it.

Flourine replaces this with a single system. One binary handles event ingestion, transport, schema management, and (planned) CDC and cataloging. Schemas are defined as native language types — annotated dataclasses in Python, annotated POJOs in Java — and the SDK generates Avro schemas, handles serialization, and tracks evolution (field renames and deletions) in the schema metadata itself. The thesis is that these concerns are not independent — they share metadata, storage, and failure semantics — and unifying them eliminates the operational surface area that makes data infrastructure hostile to deploy and maintain.

## What Flourine Does

Flourine is a disaggregated event bus: stateless Rust brokers, S3 for data, Postgres for metadata. Writers send records over WebSocket. Brokers buffer, compress (ZSTD), and flush to S3 as TBIN files. A single Postgres transaction per flush atomically commits offsets, segment index, and writer dedup state. Readers fetch segments via S3 range reads indexed by the segment catalog in Postgres.

Key properties:
- **Exactly-once writer-to-storage** — idempotent writers with LRU + Postgres dedup
- **Gap-free offsets** — atomic allocation per partition in the flush transaction
- **CRC-verified storage** — per-segment CRC32 in TBIN files, checked on every read
- **Stateless brokers** — all durable state in Postgres; any broker serves any client

## Architecture

```
                     ┌──────────────────────────────────┐
  Writers ──WS──►    │          Broker (Rust)           │
                     │                                  │
                     │  buffer → TBIN (ZSTD+CRC32)     │
                     │      │              │            │
                     │      ▼              ▼            │
                     │     S3           Postgres        │
                     │  (segments)   (offsets, index,   │
                     │               dedup, groups,     │
                     │               schemas, auth)     │
                     │      │              │            │
                     │      ▼              ▼            │
  Readers ◄──WS──   │  range read    segment lookup    │
                     └──────────────────────────────────┘
```

What this replaces:

| Flourine component         | Replaces                          |
|----------------------------|-----------------------------------|
| Broker (write/read path)   | Kafka/Redpanda brokers            |
| Schema registry            | Confluent Schema Registry         |
| TBIN storage on S3         | Kafka log segments + tiered storage |
| CDC ingestion (planned)    | Debezium + Kafka Connect          |
| Data catalog (planned)     | Separate catalog services         |
| Iceberg sink (planned)     | Kafka Connect S3/Iceberg sinks    |

## What's Built Today

**Event bus with exactly-once append.** Writers use monotonic sequence numbers. The broker deduplicates via a two-tier cache (LRU in-memory, Postgres fallback). Retries on backpressure or disconnect are safe — same `append_seq`, same result.

**Pipelined flush.** The broker buffers records from multiple writers, merges them by (topic, partition, schema), compresses into TBIN files (ZSTD + CRC32 per segment), writes to S3, and atomically commits offsets + segment index + dedup state in a single Postgres transaction. One flush runs while the next batch fills.

**Reader groups.** Lease-based partition assignment coordinated through Postgres. Deterministic range-based assignment (pure function, no leader election). Incremental rebalance — readers keep owned partitions when possible. Dead readers detected via heartbeat expiry.

**Schema registry.** Avro schemas with backward compatibility validation at registration time. High-performance Avro deserialization (5-9x faster than apache-avro, 1.5 GiB/s throughput) with zero-copy and bump allocation.

**Auth.** API key authentication with ACLs on the admin API.

**CLI.** Topic/schema management, record writes, live-tail with TUI or JSON output.

**Observability.** Prometheus metrics and OpenTelemetry tracing.

## SDKs

Rust, Java, and Python. All implement the binary wire protocol over WebSocket with idempotent writes, consumer groups, and automatic retry with exponential backoff.

### Schema-as-Code

Schemas are defined as native language types. The SDK generates Avro schemas, serializes/deserializes, and tracks evolution metadata (field renames and deletions) — no separate schema files, no manual registry calls.

**Python** — `@schema` decorator on dataclasses:

```python
from flourine import schema, NonNull, Int32
from dataclasses import dataclass
from typing import Annotated

@schema(
    namespace="com.example",
    renames={"old_name": "new_name"},  # generates Avro alias
    deletions=["removed_field"],       # tracked in schema metadata
)
@dataclass
class Event:
    id: Annotated[str, NonNull]
    new_name: int
    count: Annotated[int, Int32]       # Avro int instead of default long
    tags: list[str]                    # nullable by default

event = Event(id="evt-1", new_name=42, count=7, tags=["a"])
data: bytes = event.to_bytes()                # schemaless Avro binary
restored: Event = Event.from_bytes(data)
schema_json: str = Event.schema_json()        # for registry upload
```

**Java** — `@FlourineSchema` annotation on POJOs:

```java
@FlourineSchema(
    namespace = "com.example",
    renames = {@Rename(from = "old_name", to = "newName")},
    deletions = {"removedField"}
)
public class Event {
    @NonNull public String id;
    public int newName;
    public List<String> tags;          // nullable by default
}

byte[] data = Schemas.toBytes(event);          // schemaless Avro binary
Event restored = Schemas.fromBytes(Event.class, data);
String json = Schemas.schemaJson(Event.class); // for registry upload
```

Both generate valid Avro schemas with aliases for renamed fields and `flourine.deletions` metadata for dropped fields, so the registry's backward compatibility check passes across renames and deletions.

### Writer API

Writers connect over WebSocket, acquire a unique writer ID, and append records with automatic idempotency (monotonic `append_seq` + broker dedup). Backpressure triggers exponential backoff transparently.

**Python** — async context manager, fire-and-forget via `send_async`:

```python
async with Writer.connect("ws://localhost:9000") as writer:
    ack = await writer.send_one(topic_id=1, partition_id=0, schema_id=100,
                                key=b"user-1", value=event.to_bytes())
    # ack.start_offset, ack.end_offset
```

**Java** — `AutoCloseable`, sync and `CompletableFuture` variants:

```java
try (Writer writer = Writer.connect("ws://localhost:9000")) {
    BatchAck ack = writer.sendOne(1, 0, 100, "user-1".getBytes(),
                                  Schemas.toBytes(event));
}
```

**Rust** — `Arc<Writer>` for multi-task sharing, `JoinHandle` for fire-and-forget:

```rust
let writer = Writer::connect("ws://localhost:9000").await?;
let ack = writer.append_one(TopicId(1), PartitionId(0), SchemaId(100),
                            Some(Bytes::from("user-1")), event_bytes).await?;
```

### Reader API

Readers join consumer groups with lease-based partition assignment. Heartbeats, rebalancing, and offset tracking are handled by the SDK. The poll loop returns records grouped by partition and schema ID.

**Python** — async iterator for continuous consumption:

```python
config = ReaderConfig(url="ws://localhost:9000", group_id="my-group", topic_id=1)
async with GroupReader.join(config) as reader:
    reader.start_heartbeat()
    async for results in reader.poll_loop():
        for result in results:
            for record in result.records:
                event = Event.from_bytes(record.value)
        await reader.commit()
```

**Java** — standard poll loop with `AutoCloseable`:

```java
ReaderConfig config = new ReaderConfig()
    .url("ws://localhost:9000").groupId("my-group").topicId(1);
try (GroupReader reader = GroupReader.join(config)) {
    reader.startHeartbeat();
    List<PartitionResult> results = reader.poll();
    for (PartitionResult r : results)
        for (Record rec : r.getRecordsList())
            Event event = Schemas.fromBytes(Event.class, rec.getValue().toByteArray());
    reader.commit();
}
```

**Rust** — async poll with `Arc<GroupReader>`:

```rust
let reader = GroupReader::join(config).await?;
reader.start_heartbeat();
let results = reader.poll().await?;
for result in &results {
    for record in &result.records {
        // deserialize record.value
    }
}
reader.commit().await?;
```

## Roadmap

- **CDC ingestion** — database change capture without Debezium or Connect
- **Data catalog** — unified metadata across topics, schemas, and consumers
- **Iceberg sink** — streaming to lakehouse without separate connectors (Avro→Arrow→Parquet converter infra exists in `flourine-core`)
- **JS SDK**, multi-topic reader groups, quotas, multi-tenancy

## Quick Start

### 1. Start PostgreSQL

```bash
cd docker
docker-compose up -d
```

### 2. Start the broker

```bash
export DATABASE_URL=postgres://postgres:postgres@localhost:5433/flourine
cargo run -p flourine-broker --bin flourine-broker
```

The broker starts:
- WebSocket on `127.0.0.1:9000` (data plane)
- Admin HTTP on `127.0.0.1:9001` (management)

### 3. Bootstrap an API key

```bash
export DATABASE_URL=postgres://postgres:postgres@localhost:5433/flourine
cargo run -p flourine-cli -- bootstrap
```

Export the printed key:

```bash
export FLOURINE_API_KEY=tb_...
```

### 4. Create a topic, write, and tail

```bash
flourine topic create my-events --partitions 3
flourine write --topic my-events '{"name":"alice"}'
flourine tail --topic my-events
```

See [crates/flourine-cli/README.md](crates/flourine-cli/README.md) for the full CLI reference.

## Project Structure

| Crate / Directory | Purpose |
|-------------------|---------|
| `crates/flourine-broker` | Broker binary: WebSocket server, flush pipeline, coordinator |
| `crates/flourine-sdk` | Rust SDK: Writer, GroupReader |
| `crates/flourine-core` | Shared types, Avro engine, Arrow/Parquet converters |
| `crates/flourine-schema` | Schema registry client and compatibility logic |
| `crates/flourine-wire` | Binary wire protocol encoding/decoding |
| `crates/flourine-common` | Shared utilities |
| `crates/flourine-cli` | CLI tool (topic, schema, write, tail) |
| `sdks/java` | Java SDK |
| `sdks/python` | Python SDK |

## Links

- [logic.md](logic.md) — failure-tolerance algorithms (idempotent append, flush pipeline, reader groups)
- [eventbus_comparison.md](eventbus_comparison.md) — detailed comparison with Kafka, Redpanda, WarpStream, Bufstream, Ursa
