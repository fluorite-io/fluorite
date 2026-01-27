# Agent-Level Micro-Batching

This document specifies how Agents batch writes across multiple producers and topics before committing to the metadata store.

---

## Core Concept: The Segment

A **Segment** is the fundamental unit of data throughout the system:

```rust
struct Segment {
    topic_id: u32,
    partition_id: u32,
    schema_id: u32,
    records: Vec<Record>,
}

struct Record {
    key: Option<Bytes>,   // Optional partition key
    value: Bytes,         // Avro-encoded payload (may contain event_time in schema)
}
```

**Timestamps are agent-assigned at ingest time:**
- Agent stamps `ingest_time` when it receives the request
- All records in a producer request get the same ingest timestamp
- Used for GC, retention, and time-based queries
- If producers need event time, they include it in their Avro payload

**Segment is the universal unit at every layer:**

| Layer | Role |
|-------|------|
| Producer | Groups records by (topic, partition, schema) before sending |
| Wire protocol | Request contains `Vec<Segment>`, response contains `Vec<SegmentAck>` |
| Agent buffer | Aggregates segments from multiple producers |
| S3 file | One Avro block per segment |
| Postgres | One row in `topic_batches` per segment |
| Consumer | Fetches and deserializes segments |

---

## Producer API

### Request

```rust
struct ProduceRequest {
    producer_id: UUID,
    seq_num: u64,           // Monotonic per producer, for exactly-once
    segments: Vec<Segment>, // Mixed batch: multiple topics/partitions/schemas
}
```

### Response

```rust
struct ProduceResponse {
    segment_acks: Vec<SegmentAck>,
}

struct SegmentAck {
    topic_id: u32,
    partition_id: u32,
    schema_id: u32,
    start_offset: u64,
    end_offset: u64,
}
```

**Example:**

```
Request:
  producer_id: "abc-123"
  seq_num: 42
  segments:
    - topic=orders, partition=0, schema=100, records=[r1, r2, r3]
    - topic=orders, partition=1, schema=100, records=[r4, r5]
    - topic=clicks, partition=0, schema=200, records=[r6, r7, r8, r9]

Response:
  segment_acks:
    - topic=orders, partition=0, schema=100, start=1000, end=1002
    - topic=orders, partition=1, schema=100, start=500,  end=501
    - topic=clicks, partition=0, schema=200, start=2000, end=2003
```

---

## Motivation for Agent Batching

The original design commits each producer request individually:
- One S3 write per producer
- One DB transaction per producer (with row-level locks)

At 10k producers × 1 request/sec = 10k DB transactions/sec with lock contention.

**Solution:** Agent buffers incoming requests for a short window (default 400ms), then:
1. Merges segments from all producers
2. Writes a single multi-segment file to S3
3. Commits all offset increments in one DB transaction

---

## Agent Batching Flow

```
Producer A                    Producer B                    Producer C
    │                             │                             │
    │ ProduceRequest {            │ ProduceRequest {            │ ProduceRequest {
    │   segments: [               │   segments: [               │   segments: [
    │     (t=1,p=0,s=100)         │     (t=1,p=0,s=100)         │     (t=2,p=0,s=200)
    │     (t=1,p=1,s=100)         │   ]                         │   ]
    │   ]                         │ }                           │ }
    │ }                           │                             │
    └─────────────┬───────────────┴──────────────┬──────────────┘
                  │                              │
                  ▼                              ▼
        ┌─────────────────────────────────────────────────┐
        │                    AGENT                        │
        │                                                 │
        │  Buffer: Map<(topic, partition, schema), Vec>   │
        │    (1,0,100) → [r1,r2,r3] ++ [r4,r5]  (merged)  │
        │    (1,1,100) → [r6,r7]                          │
        │    (2,0,200) → [r8,r9,r10,r11]                  │
        │                                                 │
        │  PendingAcks: [(producer_a, seq=42, segments),  │
        │                (producer_b, seq=17, segments),  │
        │                (producer_c, seq=8,  segments)]  │
        │                                                 │
        │  ─────────── 400ms or size threshold ───────────│
        │                       │                         │
        └───────────────────────┼─────────────────────────┘
                                │
          ┌─────────────────────┴─────────────────────┐
          ▼                                           ▼
   ┌─────────────┐                           ┌─────────────┐
   │     S3      │                           │  Postgres   │
   │ Single file │                           │ Bulk commit │
   │ 4 Avro      │                           │ 4 segment   │
   │ blocks      │                           │ rows        │
   └─────────────┘                           └─────────────┘
```

---

## S3 File Format

Each flush produces a single file containing multiple Avro blocks, one per segment. Blocks are sorted by `(topic_id, partition_id, schema_id)`.

```
┌─────────────────────────────────────────────────────────────────────────┐
│ File Header                                                             │
│   - magic bytes                                                         │
│   - segment index (byte offsets to each Avro block)                     │
├─────────────────────────────────────────────────────────────────────────┤
│ Avro Block: topic=1, partition=0, schema=100                            │
│   - Avro container (schema embedded)                                    │
│   - records: [{key, value}, {key, value}, ...]                          │
├─────────────────────────────────────────────────────────────────────────┤
│ Avro Block: topic=1, partition=0, schema=101  ← same (topic, partition) │
│   - Avro container (schema embedded)              different schema      │
│   - records: [{key, value}, ...]                                        │
├─────────────────────────────────────────────────────────────────────────┤
│ Avro Block: topic=1, partition=1, schema=100                            │
│   - Avro container (schema embedded)                                    │
│   - records: [{key, value}, ...]                                        │
├─────────────────────────────────────────────────────────────────────────┤
│ Avro Block: topic=2, partition=0, schema=200                            │
│   - Avro container (schema embedded)                                    │
│   - records: [{key, value}, ...]                                        │
└─────────────────────────────────────────────────────────────────────────┘
```

**File Metadata (in header):**

```json
{
  "agent_id": "agent-7a3b",
  "ingest_time": 1705312200000000,
  "segments": [
    {
      "topic_id": 1, "partition_id": 0, "schema_id": 100,
      "record_count": 100,
      "byte_offset": 0, "byte_length": 4096
    },
    {
      "topic_id": 1, "partition_id": 0, "schema_id": 101,
      "record_count": 25,
      "byte_offset": 4096, "byte_length": 1024
    }
  ]
}
```

**Note:** `ingest_time` is per-file (all segments in one flush share the same timestamp). This is the agent's wall clock at flush time.

---

## Offset Assignment

Offsets are assigned per `(topic_id, partition_id)`, **independent of schema**. Segments for the same (topic, partition) but different schemas get contiguous offset ranges:

```
File contains:
  segment A: topic=1, partition=0, schema=100, 100 records
  segment B: topic=1, partition=0, schema=101, 25 records   ← same (topic, partition)
  segment C: topic=1, partition=1, schema=100, 50 records

Offset assignment:
  topic=1, partition=0: current offset = 1000
    segment A: offsets 1000-1099 (100 records)
    segment B: offsets 1100-1124 (25 records)   ← continues from A

  topic=1, partition=1: current offset = 500
    segment C: offsets 500-549 (50 records)     ← separate offset space
```

---

## Database Schema

### Table: `partition_offsets`

Dedicated offset counter per (topic, partition).

```sql
CREATE TABLE partition_offsets (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    next_offset BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (topic_id, partition_id)
) WITH (fillfactor = 70);  -- HOT updates
```

### Table: `topic_batches` (Segment Index)

One row per segment. Partitioned by day for efficient retention (drop old partitions instantly).

```sql
-- Parent table (partitioned by ingest_time)
CREATE TABLE topic_batches (
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    start_offset BIGINT NOT NULL,
    end_offset BIGINT NOT NULL,
    schema_id INT NOT NULL,
    s3_key TEXT NOT NULL,
    byte_offset BIGINT NOT NULL,    -- Offset within S3 file
    byte_length BIGINT NOT NULL,
    record_count INT NOT NULL,
    ingest_time BIGINT NOT NULL,    -- Agent-assigned timestamp (epoch micros)
    PRIMARY KEY (topic_id, partition_id, start_offset, ingest_time)
) PARTITION BY RANGE (ingest_time);

-- Daily partitions created automatically by pg_partman or cron job
-- Example partition (bounds in microseconds):
CREATE TABLE topic_batches_2024_01_15 PARTITION OF topic_batches
    FOR VALUES FROM (1705276800000000) TO (1705363200000000);

-- Indexes (inherited by partitions)
CREATE INDEX idx_segments_by_offset
ON topic_batches (topic_id, partition_id, start_offset);

CREATE INDEX idx_segments_by_ingest_time
ON topic_batches (topic_id, partition_id, ingest_time);
```

**Retention:** Drop old partitions (instant, no vacuum):

```sql
DROP TABLE topic_batches_2024_01_08;
```

**Partition management:** Use pg_partman (available on RDS, Cloud SQL, Azure) or a simple cron job:

```sql
-- Daily cron: create tomorrow's partition, drop old ones
SELECT create_daily_partition('topic_batches', NOW() + INTERVAL '1 day');
SELECT drop_partitions_older_than('topic_batches', INTERVAL '7 days');
```

### Table: `producer_state`

Tracks last committed sequence per producer for exactly-once.

```sql
CREATE TABLE producer_state (
    producer_id UUID PRIMARY KEY,
    last_seq_num BIGINT NOT NULL DEFAULT -1,
    last_segment_acks JSONB,  -- Full acks from last commit for idempotent retry
    updated_at TIMESTAMPTZ DEFAULT NOW()
) WITH (fillfactor = 70);
```

---

## Bulk Commit Stored Procedure

Single transaction that:
1. Increments offsets for each (topic, partition)
2. Inserts segment rows with timestamp bounds
3. Validates producer sequences (exactly-once)
4. Updates producer state

```sql
CREATE OR REPLACE FUNCTION commit_agent_batch(
    p_s3_key TEXT,
    p_ingest_time BIGINT,
    p_segments JSONB,
    p_producers JSONB
) RETURNS TABLE (
    status TEXT,
    segment_acks JSONB,
    duplicate_producers JSONB
) AS $$
DECLARE
    v_seg JSONB;
    v_prod JSONB;
    v_topic INT;
    v_partition INT;
    v_schema INT;
    v_count INT;
    v_byte_off BIGINT;
    v_byte_len BIGINT;
    v_next_offset BIGINT;
    v_acks JSONB := '[]'::JSONB;
    v_dups JSONB := '[]'::JSONB;
    v_offset_map JSONB := '{}'::JSONB;
    v_key TEXT;
    v_last_seq BIGINT;
BEGIN
    -- Phase 1: Assign offsets and insert segments
    FOR v_seg IN SELECT * FROM jsonb_array_elements(p_segments)
    LOOP
        v_topic := (v_seg->>'topic_id')::INT;
        v_partition := (v_seg->>'partition_id')::INT;
        v_schema := (v_seg->>'schema_id')::INT;
        v_count := (v_seg->>'record_count')::INT;
        v_byte_off := (v_seg->>'byte_offset')::BIGINT;
        v_byte_len := (v_seg->>'byte_length')::BIGINT;

        -- Ensure partition exists, lock and get offset
        INSERT INTO partition_offsets (topic_id, partition_id, next_offset)
        VALUES (v_topic, v_partition, 0)
        ON CONFLICT DO NOTHING;

        SELECT next_offset INTO v_next_offset
        FROM partition_offsets
        WHERE topic_id = v_topic AND partition_id = v_partition
        FOR UPDATE;

        -- Increment offset
        UPDATE partition_offsets
        SET next_offset = next_offset + v_count, updated_at = NOW()
        WHERE topic_id = v_topic AND partition_id = v_partition;

        -- Insert segment row
        INSERT INTO topic_batches (
            topic_id, partition_id, start_offset, end_offset, schema_id,
            s3_key, byte_offset, byte_length, record_count, ingest_time
        ) VALUES (
            v_topic, v_partition, v_next_offset, v_next_offset + v_count - 1,
            v_schema, p_s3_key, v_byte_off, v_byte_len, v_count, p_ingest_time
        );

        -- Build ack
        v_acks := v_acks || jsonb_build_object(
            'topic_id', v_topic,
            'partition_id', v_partition,
            'schema_id', v_schema,
            'start_offset', v_next_offset,
            'end_offset', v_next_offset + v_count - 1
        );

        -- Track in map for producer lookups
        v_key := v_topic || ':' || v_partition || ':' || v_schema;
        v_offset_map := jsonb_set(v_offset_map, ARRAY[v_key],
            jsonb_build_object('start', v_next_offset, 'end', v_next_offset + v_count - 1));
    END LOOP;

    -- Phase 2: Validate producers and update state
    FOR v_prod IN SELECT * FROM jsonb_array_elements(p_producers)
    LOOP
        DECLARE
            v_pid UUID := (v_prod->>'producer_id')::UUID;
            v_seq BIGINT := (v_prod->>'seq_num')::BIGINT;
            v_prod_segments JSONB := v_prod->'segments';
            v_prod_acks JSONB := '[]'::JSONB;
            v_seg_ref JSONB;
        BEGIN
            -- Build this producer's segment acks
            FOR v_seg_ref IN SELECT * FROM jsonb_array_elements(v_prod_segments)
            LOOP
                v_key := (v_seg_ref->>'topic_id') || ':' ||
                         (v_seg_ref->>'partition_id') || ':' ||
                         (v_seg_ref->>'schema_id');
                v_prod_acks := v_prod_acks || jsonb_build_object(
                    'topic_id', (v_seg_ref->>'topic_id')::INT,
                    'partition_id', (v_seg_ref->>'partition_id')::INT,
                    'schema_id', (v_seg_ref->>'schema_id')::INT,
                    'start_offset', (v_offset_map->v_key->>'start')::BIGINT,
                    'end_offset', (v_offset_map->v_key->>'end')::BIGINT
                );
            END LOOP;

            -- Check for duplicate
            SELECT last_seq_num INTO v_last_seq
            FROM producer_state WHERE producer_id = v_pid FOR UPDATE;

            IF v_last_seq IS NOT NULL AND v_seq <= v_last_seq THEN
                -- Duplicate: return stored acks
                v_dups := v_dups || jsonb_build_object(
                    'producer_id', v_pid,
                    'seq_num', v_seq,
                    'segment_acks', (SELECT last_segment_acks FROM producer_state WHERE producer_id = v_pid)
                );
            ELSE
                -- New: update state
                INSERT INTO producer_state (producer_id, last_seq_num, last_segment_acks)
                VALUES (v_pid, v_seq, v_prod_acks)
                ON CONFLICT (producer_id) DO UPDATE
                SET last_seq_num = EXCLUDED.last_seq_num,
                    last_segment_acks = EXCLUDED.last_segment_acks,
                    updated_at = NOW();
            END IF;
        END;
    END LOOP;

    -- Return
    IF jsonb_array_length(v_dups) > 0 THEN
        RETURN QUERY SELECT 'PARTIAL'::TEXT, v_acks, v_dups;
    ELSE
        RETURN QUERY SELECT 'COMMITTED'::TEXT, v_acks, '[]'::JSONB;
    END IF;
END;
$$ LANGUAGE plpgsql;
```

---

## Agent Pseudocode

```
CONFIG:
  BATCH_WINDOW = 400ms
  MAX_BATCH_SIZE = 10MB
  MAX_BATCH_RECORDS = 100_000

STATE:
  # Segments merged across producers
  buffer: Map<(TopicID, PartitionID, SchemaID), Vec<TimestampedRecord>>

  # Track each producer's original request for acking
  pending: Vec<PendingProducer>

struct PendingProducer {
    producer_id: UUID,
    seq_num: u64,
    segments: Vec<SegmentKey>,  // Which segments this producer contributed to
    channel: ResponseChannel,
}

struct SegmentKey {
    topic_id: u32,
    partition_id: u32,
    schema_id: u32,
}


FUNCTION HANDLE_PRODUCE(req: ProduceRequest) -> ProduceResponse:
    # 1. Validate all schemas
    FOR segment IN req.segments:
        IF NOT REGISTRY.VALIDATE(segment.schema_id, segment.records):
            RETURN Error("Invalid schema")

    # 2. Buffer records, merging with existing
    segment_keys = []
    FOR segment IN req.segments:
        key = (segment.topic_id, segment.partition_id, segment.schema_id)
        buffer[key].extend(segment.records)
        segment_keys.push(SegmentKey(key))

    # 3. Track this producer's contribution
    pending.push(PendingProducer {
        producer_id: req.producer_id,
        seq_num: req.seq_num,
        segments: segment_keys,
        channel: response_channel,
    })

    # 4. Check flush triggers
    IF buffer.total_bytes() >= MAX_BATCH_SIZE OR buffer.total_records() >= MAX_BATCH_RECORDS:
        FLUSH()
    ELSE IF NOT timer.running():
        timer.start(BATCH_WINDOW, FLUSH)

    # 5. Wait for flush
    RETURN AWAIT response_channel


FUNCTION FLUSH():
    timer.stop()

    snapshot = buffer.drain()
    producers = pending.drain()

    IF snapshot.is_empty():
        RETURN

    # Assign ingest_time once for entire flush
    ingest_time = NOW_MICROS()

    # 1. Build segments sorted by (topic, partition, schema)
    segments = []
    byte_offset = 0

    FOR key IN snapshot.keys().sorted():
        records = snapshot[key]
        avro_bytes = AVRO_SERIALIZE(key.schema_id, records)

        segments.push({
            topic_id: key.0,
            partition_id: key.1,
            schema_id: key.2,
            record_count: records.len(),
            byte_offset: byte_offset,
            byte_length: avro_bytes.len(),
            data: avro_bytes,
        })
        byte_offset += avro_bytes.len()

    # 2. Build producer list for DB
    db_producers = []
    FOR p IN producers:
        db_producers.push({
            producer_id: p.producer_id,
            seq_num: p.seq_num,
            segments: p.segments,  # SegmentKeys this producer contributed
        })

    # 3. Write file to S3
    file = BUILD_FILE(segments)
    s3_key = S3.PUT(BUCKET, UUID(), file)

    # 4. Commit to DB (with retry)
    result = None
    FOR attempt IN 1..=MAX_DB_RETRIES:
        TRY:
            result = DB.CALL("commit_agent_batch", s3_key, ingest_time, segments, db_producers)
            BREAK
        CATCH RetryableError:
            IF attempt == MAX_DB_RETRIES:
                FOR p IN producers:
                    p.channel.send(HTTP_503)
                RETURN
            SLEEP(BACKOFF(attempt))
        CATCH NonRetryableError:
            FOR p IN producers:
                p.channel.send(HTTP_500)
            RETURN

    # 5. Build ack lookup: (topic, partition, schema) -> SegmentAck
    ack_map = {}
    FOR ack IN result.segment_acks:
        ack_map[(ack.topic_id, ack.partition_id, ack.schema_id)] = ack

    dup_map = {}
    FOR dup IN result.duplicate_producers:
        dup_map[dup.producer_id] = dup.segment_acks

    # 6. Send responses
    FOR p IN producers:
        IF p.producer_id IN dup_map:
            # Idempotent retry - return stored acks
            p.channel.send(ProduceResponse { segment_acks: dup_map[p.producer_id] })
        ELSE:
            # Build response from this flush's acks
            acks = []
            FOR seg_key IN p.segments:
                acks.push(ack_map[(seg_key.topic_id, seg_key.partition_id, seg_key.schema_id)])
            p.channel.send(ProduceResponse { segment_acks: acks })
```

---

## Flush Triggers

| Trigger | Threshold | Rationale |
|---------|-----------|-----------|
| **Time** | 400ms | Bounds latency for low-throughput topics |
| **Size** | 10MB | Prevents unbounded memory growth |
| **Records** | 100k | Keeps files reasonable for compaction |

---

## Failure Modes

| Scenario | State | Resolution |
|----------|-------|------------|
| Agent crash before S3 write | Nothing persisted | Producers timeout, retry to another agent |
| Agent crash after S3, before DB | Ghost file in S3 | GC deletes after TTL; producers retry |
| Agent crash after DB, before ack | Committed but producers don't know | Producers retry with same seq_num; DB returns stored acks |
| DB transaction fails (retryable) | S3 file written | Agent retries DB commit with exponential backoff |
| DB transaction fails (exhausted) | S3 file orphaned | Agent returns 503; producers retry; GC cleans file |

---

## Performance

**Before (per-producer commits):**
- 10k producers × 1 req/sec = 10k DB txns/sec
- High lock contention on partition_offsets rows

**After (agent batching @ 400ms):**
- 10k producers × 1 req/sec = 10k segments buffered
- 2.5 flushes/sec = 2.5 DB txns/sec
- **4000x reduction in DB transactions**

**Trade-off:** +400ms worst-case latency for dramatically improved throughput.

---

## Consumer Queries

### Fetch by offset

```sql
SELECT s3_key, byte_offset, byte_length, start_offset, end_offset, schema_id
FROM topic_batches
WHERE topic_id = $1 AND partition_id = $2 AND start_offset >= $3
ORDER BY start_offset
LIMIT 100;
```

### Fetch by ingest time

```sql
SELECT s3_key, byte_offset, byte_length, start_offset, end_offset, schema_id
FROM topic_batches
WHERE topic_id = $1 AND partition_id = $2
  AND ingest_time >= $3  -- start_ts
  AND ingest_time <= $4  -- end_ts
ORDER BY start_offset;
```

### Retention / GC

```sql
-- Find segments older than retention period
DELETE FROM topic_batches
WHERE ingest_time < $1;  -- cutoff timestamp in micros
```

---

## Design Decisions

1. **Segment merging:** When multiple producers contribute to the same (topic, partition, schema) in one flush window, their records are merged into a single segment. This is intentional — it maximizes batching efficiency.

2. **Multi-agent contention:** Multiple agents may contend on `partition_offsets` rows, but since we batch writes (one DB transaction per 400ms window, not per producer), contention is acceptable.

3. **Agent-assigned timestamps:** Agents assign `ingest_time` at flush time. All segments in a flush share the same timestamp. If producers need event time, they include it in their Avro payload. This ensures GC/retention is predictable and not subject to producer clock skew.

4. **DB commit retry:** On transient DB failures, agent retries with exponential backoff before returning 503 to producers.
