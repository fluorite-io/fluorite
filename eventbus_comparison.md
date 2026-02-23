# Flourine vs Kafka, Redpanda, WarpStream, Bufstream, Ursa

## Architecture Family

These systems fall into two camps:

| | Replicated-log (stateful brokers) | Disaggregated (stateless brokers + object storage) |
|---|---|---|
| **Systems** | Kafka, Redpanda | WarpStream, Bufstream, Ursa, **Flourine** |
| **Durability** | Broker-managed ISR / Raft replication | S3 for payloads, external metadata store |
| **Scaling** | Partition rebalancing on add/remove | Add/remove brokers instantly |
| **Failure recovery** | Leader election, ISR catch-up | Retry on any broker |

Flourine is firmly in the disaggregated camp. Its closest architectural peers are **WarpStream** and **Bufstream**.

---

## Wire Protocol

| System | Protocol | Drop-in for Kafka clients? |
|---|---|---|
| Kafka | Kafka binary protocol | (origin) |
| Redpanda | Kafka binary protocol | Yes |
| WarpStream | Kafka binary protocol | Yes |
| Bufstream | Kafka binary protocol | Yes |
| Ursa | Kafka binary protocol | Yes |
| **Flourine** | **Custom Protobuf over WebSocket** | **No** |

This is Flourine's most significant differentiator — and its biggest adoption barrier. Every other system in this comparison speaks Kafka natively. Flourine requires its own SDKs (Rust, Java, Python; JS planned). Existing Kafka Connect connectors, Kafka Streams, Flink Kafka sources/sinks, and the entire Kafka tooling ecosystem don't work with Flourine.

**Tradeoff**: The custom protocol is simpler (no need to implement 30+ Kafka API versions), allows Flourine to evolve its semantics freely, and avoids inheriting Kafka protocol bugs (e.g., KAFKA-17754 — the cross-socket sequence number gap that causes torn transactions in all Kafka-compatible systems, found by Jepsen in Bufstream). But it means Flourine must build its own ecosystem from scratch.

---

## Metadata Store

| System | Metadata store | Failure mode |
|---|---|---|
| Kafka | ZooKeeper / KRaft (embedded) | Self-contained |
| Redpanda | Raft (embedded, no external deps) | Self-contained |
| WarpStream | Managed cloud DB (DynamoDB/Spanner/CosmosDB) | Vendor-managed |
| Bufstream | Pluggable: Postgres, Spanner, or etcd | Operator-managed |
| Ursa | Oxia (custom, TLA+ verified, sharded) | Operator-managed |
| **Flourine** | **Postgres** | **Operator-managed, SPOF** |

Flourine uses Postgres for everything: offsets, segment index, writer dedup, reader groups, schemas, auth. This is elegant (single ACID transaction per flush) but makes Postgres a single point of failure. No writes or reads can proceed if Postgres is down.

WarpStream avoids this by using managed cloud databases with built-in HA. Bufstream offers pluggable backends including Spanner (multi-region). Ursa built a custom metadata store (Oxia) with horizontal sharding. Kafka and Redpanda avoid the problem entirely by embedding metadata in the cluster itself.

---

## Write Path Latency

| System | Typical e2e latency | Why |
|---|---|---|
| Kafka | 2-10ms | Local disk + ISR ack |
| Redpanda | 2-5ms (write caching) | Direct I/O, in-memory Raft quorum |
| WarpStream | ~250ms (S3 Express) to ~1s (S3 Standard) | S3 PUT in critical path |
| Bufstream | ~260ms median, ~500ms p99 | Object storage in critical path |
| Ursa | 200-500ms (S3 WAL) or ~5ms (BookKeeper) | Configurable per-topic |
| **Flourine** | **~100ms + S3 + Postgres** | Buffer interval + S3 PUT + PG commit |

All disaggregated systems pay the S3 PUT latency penalty. Flourine additionally pays for the Postgres transaction (offset allocation + segment index + writer state upsert). Ursa is unique in offering a per-topic choice between the low-latency path (BookKeeper WAL) and the cost-optimized path (S3).

---

## Exactly-Once and Transactions

| System | Idempotent producer | Cross-partition transactions | Mechanism |
|---|---|---|---|
| Kafka | Yes (per-partition seq window of 5) | Yes | Producer epochs + transaction coordinator |
| Redpanda | Yes (Kafka-compatible) | Yes (Jepsen-validated post-fixes) | Kafka-compatible |
| WarpStream | Yes (retroactive tombstones) | Yes | Metadata-layer dedup + Kafka txn protocol |
| Bufstream | Yes (Jepsen-validated) | Yes (Jepsen-validated, modulo KAFKA-17754) | Kafka-compatible |
| Ursa | Unknown | **No** (not yet supported) | — |
| **Flourine** | **Writer-scoped, single last_seq** | **No** | LRU cache + Postgres fallback |

Flourine's dedup is simpler than Kafka's: it tracks a single `last_seq` per writer rather than a sliding window of 5 per partition. This is safe under the SDK's monotonic-increment contract but weaker at the protocol level. There are no transactions — the whitepaper explicitly lists this as a non-goal for v1.

The practical impact: Flourine cannot support read-process-write loops with exactly-once guarantees (the core Kafka Streams / Flink use case). For most produce-only workloads with idempotent consumers, this doesn't matter.

---

## Consumer / Reader Groups

| System | Protocol | Assignment | Coordination |
|---|---|---|---|
| Kafka | JoinGroup/SyncGroup/Heartbeat | Pluggable assignors (Range, RoundRobin, Sticky, Cooperative) | Coordinator broker (leader of `__consumer_offsets`) |
| Redpanda | Kafka-compatible | Kafka-compatible | Internal Raft |
| WarpStream | Kafka-compatible | Kafka-compatible | Any agent or backend |
| Bufstream | Kafka-compatible | Kafka-compatible | Metadata backend |
| Ursa | Kafka-compatible | Kafka-compatible | Oxia |
| **Flourine** | **Custom (JoinGroup/Heartbeat/Rejoin/Leave/Commit)** | **Deterministic range-based only** | **Postgres leases** |

Flourine's reader group protocol is simpler: a single deterministic range assignment function (`compute_assignment`) with lease-based partition ownership in Postgres. No pluggable assignors, no cooperative rebalance. This is sufficient for most use cases but lacks the flexibility of Kafka's protocol (sticky assignments minimize partition migration during rebalances).

---

## Storage Format and Lakehouse Integration

| System | Primary format | Native Iceberg/Parquet? | Lakehouse path |
|---|---|---|---|
| Kafka | Proprietary log segments | No | Kafka Connect → S3 → ETL → Iceberg |
| Redpanda | Proprietary log segments | No | Same as Kafka |
| WarpStream | Mixed-partition files in S3 | No (compaction produces optimized files) | External ETL |
| Bufstream | Intake → Archive (Parquet/Iceberg) | **Yes — zero-copy** | Built-in |
| Ursa | WAL → Parquet/Iceberg/Delta | **Yes — native compaction** | Built-in |
| **Flourine** | **FL (custom ZSTD container)** | **No (Avro→Arrow→Parquet infra exists, Iceberg deferred to v2)** | Planned |

Bufstream and Ursa have a significant edge here: they write Parquet/Iceberg natively, eliminating the ETL pipeline entirely. Flourine has the converter machinery in `flourine-core` (Avro→Arrow→Parquet) but hasn't wired it into the data path yet. This is the most compelling v2 feature.

---

## Schema Enforcement

| System | Where enforced | Formats | Broker-side validation? |
|---|---|---|---|
| Kafka | Client-side (Confluent SR advisory) | Avro, Protobuf, JSON Schema | No |
| Redpanda | Built-in SR, client-side enforcement | Avro, Protobuf, JSON Schema | No |
| WarpStream | External SR | Same as Kafka | No |
| Bufstream | **Broker-side** (Buf SR integration) | Protobuf-native, semantic validation | **Yes — can reject bad data** |
| Ursa | Client-side | Standard Kafka SR | No |
| **Flourine** | **Separate Avro registry, out-of-band** | **Avro only** | **No** |

Bufstream is unique in offering broker-side schema enforcement with semantic validation (Protovalidate rules). Flourine's schema registry does backward compatibility checks at registration time but doesn't validate records on the append path — the broker treats payloads as opaque bytes.

---

## Operational Model

| System | Deploy complexity | External dependencies | Scaling model |
|---|---|---|---|
| Kafka | High (JVM tuning, ZK/KRaft, ISR) | ZooKeeper (legacy) | Partition rebalance |
| Redpanda | Medium (single binary, no JVM) | None | Partition rebalance |
| WarpStream | Low (single stateless binary) | S3 + managed metadata SaaS | Add/remove agents |
| Bufstream | Low (stateless, Helm chart) | S3 + Postgres/Spanner/etcd | Add/remove brokers |
| Ursa | Low (stateless) | S3 + Oxia | Add/remove brokers |
| **Flourine** | **Low (single binary)** | **S3 + Postgres** | **Add/remove agents** |

Flourine's operational model is comparable to Bufstream with Postgres as the metadata backend. The dependency set (S3 + Postgres) is simpler than Kafka's but creates a hard dependency on Postgres availability.

---

## Where Flourine Stands

**Strengths relative to the field:**
- Simplest metadata model — single Postgres transaction per flush, no Raft, no custom consensus
- Custom protocol avoids inheriting Kafka protocol bugs (KAFKA-17754, split-brain edge cases)
- Avro-native schema registry with backward compatibility
- Clean codebase with Jepsen-style test suite (crash, linearizability, offset, partition, reader group tests)
- Per-writer record ordering guarantee

**Gaps relative to the field:**
1. **No Kafka protocol compatibility** — every other system has it; this is the biggest ecosystem barrier
2. **Postgres SPOF** — WarpStream uses managed HA databases, Bufstream supports Spanner, Ursa has Oxia
3. **No transactions** — Kafka, Redpanda, WarpStream, and Bufstream all have them
4. **No native Iceberg/Parquet** — Bufstream and Ursa have zero-copy lakehouse; Flourine has the infra but hasn't connected it
5. **Single assignment strategy** — no cooperative rebalance, no sticky assignments
6. **Dedup is weaker** — single `last_seq` vs Kafka's per-partition window of 5

**Architectural positioning:**
Flourine occupies roughly the same design space as WarpStream and Bufstream (stateless brokers, S3 for data, external metadata store) but without Kafka compatibility. If it were to add a Kafka protocol adapter, it would compete directly with those systems. Without it, its value proposition is "a simpler, purpose-built event bus for teams willing to use Flourine-native SDKs" — which is a viable niche but a smaller market.
