# Topics and Partitions

---

## Topic Model

```sql
CREATE TABLE topics (
    topic_id SERIAL PRIMARY KEY,
    topic_name TEXT UNIQUE NOT NULL,
    partition_count INT NOT NULL DEFAULT 1,
    retention_micros BIGINT NOT NULL DEFAULT 172800000000,  -- 2 days
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

**Fields:**
- `topic_id`: Internal integer ID (used in segment metadata for efficiency)
- `topic_name`: Human-readable name (e.g., "orders", "clickstream")
- `partition_count`: Number of partitions (immutable after creation)
- `retention_micros`: How long to keep data (default 2 days — long-term storage in Iceberg)

---

## Topic Creation

Topics are created explicitly via admin API (not auto-created on first write).

```
POST /admin/topics
{
  "name": "orders",
  "partitions": 16,
  "retention_days": 2
}
```

**Validation:**
- Name must be unique
- Partition count must be 1-1024
- Retention must be 1 hour - 7 days (long-term retention handled by Iceberg)

**Response:**
```json
{
  "topic_id": 42,
  "name": "orders",
  "partitions": 16,
  "retention_days": 2
}
```

---

## Partition Assignment

**Partitions are logical, not physical.** There's no broker ownership or rebalancing.

| Kafka | This System |
|-------|-------------|
| Partitions assigned to brokers | Partitions are just numbers |
| Rebalancing on broker add/remove | No rebalancing needed |
| Leader election | No leaders |
| ISR (in-sync replicas) | S3 handles durability |

**Any agent can write to any partition.** Agents are stateless gateways.

**Producer chooses partition:**
```rust
// Producer-side partition selection
let partition = match record.key {
    Some(key) => hash(key) % topic.partition_count,
    None => round_robin(),
};
```

---

## Producer Registration

Producers are **self-registered** with a client-generated UUID.

```rust
// Producer initialization (client-side)
let producer_id = Uuid::new_v4();
let seq_num = 0;
```

**No server-side registration needed.** The producer_id is:
- Used for exactly-once deduplication
- Tracked in `producer_state` table on first write
- Client's responsibility to persist across restarts (for exactly-once guarantees)

**Producer state cleanup:** Producers inactive for > 2 days can be garbage collected:

```sql
DELETE FROM producer_state
WHERE updated_at < NOW() - INTERVAL '2 days';
```

---

## Partition Count Changes

**Partition count is immutable.** Increasing partitions would break ordering guarantees for keyed messages.

If more throughput is needed:
1. Create a new topic with more partitions
2. Migrate producers to the new topic
3. Consumers read from both during migration
4. Delete old topic after drain

---

## Agent Routing

Agents accept writes to any topic/partition. No coordination needed.

```
Producer A ──┐
Producer B ──┼──► Agent 1 ──┐
Producer C ──┘              │
                            ├──► S3 + Postgres
Producer D ──┐              │
Producer E ──┼──► Agent 2 ──┘
Producer F ──┘
```

**Load balancing:** Standard L4/L7 load balancer in front of agents.

**Failure handling:** If an agent dies, producers retry to any other agent. No partition reassignment.

---

## Consumer Partition Assignment

Consumers choose which partitions to read. Two patterns:

### Pattern 1: Exclusive Assignment (Consumer Groups)

For parallel processing with exactly-once per partition:

```
Consumer Group "order-processor":
  Consumer A: partitions [0, 1, 2, 3]
  Consumer B: partitions [4, 5, 6, 7]
  Consumer C: partitions [8, 9, 10, 11, 12, 13, 14, 15]
```

Coordination handled by consumer group protocol (future spec).

### Pattern 2: Broadcast (All Partitions)

For analytics or replication:

```
Consumer X: reads all partitions [0..15]
Consumer Y: reads all partitions [0..15]  (independent)
```

---

## Metadata Queries

### List Topics

```sql
SELECT topic_id, topic_name, partition_count, retention_micros
FROM topics
ORDER BY topic_name;
```

### Get Topic

```sql
SELECT topic_id, topic_name, partition_count, retention_micros
FROM topics
WHERE topic_name = $1;
```

### Get Partition Offsets

```sql
-- Latest offset per partition
SELECT partition_id, next_offset
FROM partition_offsets
WHERE topic_id = $1
ORDER BY partition_id;
```

---

## Example Flow

```
1. Admin creates topic:
   POST /admin/topics {"name": "orders", "partitions": 4}
   → topic_id = 1

2. Producer initializes:
   producer_id = "abc-123" (self-generated UUID)
   seq_num = 0

3. Producer sends record with key="user-42":
   partition = hash("user-42") % 4 = 2

   ProduceRequest {
     producer_id: "abc-123",
     seq_num: 0,
     segments: [{
       topic_id: 1,
       partition_id: 2,
       schema_id: 100,
       records: [...]
     }]
   }

4. Agent writes to S3, commits to Postgres

5. Consumer reads:
   SELECT * FROM topic_batches
   WHERE topic_id = 1 AND partition_id = 2 AND start_offset >= 0
```
