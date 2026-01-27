PostgreSQL is battle-tested, strictly consistent (ACID), runs everywhere (RDS, Cloud SQL, AlloyDB, or bare metal), and 
handles high throughput surprisingly well if we keep the transactions short.

Here is the **Postgres-backed Design Proposal** for the stateless architecture.

---

### 1. Database Schema Design

We need two core tables: one to track the **Log** (the files in S3) and one to track **Producer State** (for idempotency).

#### **Table A: `producer_state` (The Gatekeeper)**

This table ensures Exactly-Once Semantics by tracking the last successfully committed sequence number for every active producer.

```sql
CREATE TABLE producer_state (
    producer_id UUID PRIMARY KEY,
    last_seq_num BIGINT NOT NULL DEFAULT -1,
    last_offset BIGINT NOT NULL DEFAULT -1,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

```

#### **Table B: `topic_batches` (The Virtual Log)**

This table maps logical offsets to physical S3 files. It is the "Index" that Consumers query.

```sql
CREATE TABLE topic_batches (
    topic_id INT NOT NULL,  -- Map string topic names to INTs for speed
    partition_id INT NOT NULL,
    start_offset BIGINT NOT NULL,
    end_offset BIGINT NOT NULL,
    batch_uuid UUID NOT NULL, -- The file name in S3
    record_count INT NOT NULL,
    s3_key TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Constraint: Ensure ranges don't overlap (simplified)
    PRIMARY KEY (topic_id, partition_id, start_offset)
);

-- Index for fast Consumer lookups ("Give me batches after offset X")
CREATE INDEX idx_batches_lookup 
ON topic_batches (topic_id, partition_id, start_offset);

```

---

### 2. The "Atomic Commit" Logic (PL/pgSQL)

To achieve high throughput and safety, do **not** do this in application code (Select -> Check -> Insert). The round-trips will kill performance.

Instead, use a **Stored Procedure** (or a single transaction block). This pushes the logic into the database engine, locking the row for the minimum possible time.

**The `commit_batch` Stored Procedure:**

```sql
CREATE OR REPLACE FUNCTION commit_batch(
    p_producer_id UUID,
    p_seq_num BIGINT,
    p_topic_id INT,
    p_partition_id INT,
    p_s3_key TEXT,
    p_record_count INT
) RETURNS TABLE (status TEXT, offset BIGINT) AS $$
DECLARE
    v_last_seq BIGINT;
    v_last_offset BIGINT;
    v_next_offset BIGINT;
BEGIN
    -- 1. Lock the Producer Row (Pessimistic Lock)
    -- This serializes all writes for this specific producer.
    SELECT last_seq_num, last_offset INTO v_last_seq, v_last_offset
    FROM producer_state
    WHERE producer_id = p_producer_id
    FOR UPDATE; -- Critical: Locks row until transaction end

    -- 2. Idempotency Check (The "Dedup")
    IF v_last_seq IS NOT NULL AND p_seq_num <= v_last_seq THEN
        -- We have seen this before! Return the OLD result.
        RETURN QUERY SELECT 'DUPLICATE', v_last_offset;
        RETURN;
    END IF;

    -- 3. Calculate Global Offset (The "Sequencer")
    -- Lock the partition to get the absolute next offset.
    -- (Alternatively, use a Postgres SEQUENCE for higher concurrency)
    SELECT COALESCE(MAX(end_offset), -1) + 1 INTO v_next_offset
    FROM topic_batches
    WHERE topic_id = p_topic_id AND partition_id = p_partition_id;

    -- 4. Append to Log
    INSERT INTO topic_batches (
        topic_id, partition_id, start_offset, end_offset, 
        batch_uuid, record_count, s3_key
    ) VALUES (
        p_topic_id, p_partition_id, v_next_offset, 
        v_next_offset + p_record_count - 1, 
        gen_random_uuid(), p_record_count, p_s3_key
    );

    -- 5. Update Producer State
    INSERT INTO producer_state (producer_id, last_seq_num, last_offset)
    VALUES (p_producer_id, p_seq_num, v_next_offset)
    ON CONFLICT (producer_id) DO UPDATE 
    SET last_seq_num = EXCLUDED.last_seq_num,
        last_offset = EXCLUDED.last_offset;

    -- 6. Success!
    RETURN QUERY SELECT 'COMMITTED', v_next_offset;
END;
$$ LANGUAGE plpgsql;

```

---

### 3. Agent Logic with Postgres

The Agent code becomes very thin. It simply calls the stored procedure.

**Concise Pseudo-Code:**

```text
FUNCTION HANDLE_PRODUCE(Producer_ID, Seq, Data):
  
  # 1. Optimistic S3 Write
  S3_Key = S3.PUT(Bucket, UUID(), Data)

  # 2. Call Postgres Stored Procedure
  TRY:
    Result = DB.CALL("commit_batch", Producer_ID, Seq, ..., S3_Key)
  CATCH DB_Error:
    RETURN HTTP_500 # Client will retry

  # 3. Handle Result
  IF Result.Status == 'COMMITTED':
    RETURN HTTP_200(Result.Offset)
    
  ELSE IF Result.Status == 'DUPLICATE':
    # Clean up the ghost file
    S3.DELETE(S3_Key) 
    RETURN HTTP_200(Result.Offset)

```

---

### 4. Performance Tuning for Postgres

To make this scale to 10k+ writes/sec:

1. **Partitioning:** Use **Declarative Partitioning** on the `topic_batches` table. Partition by `topic_id` or `created_at`. This keeps the indexes small and fast.
```sql
CREATE TABLE topic_batches_p1 PARTITION OF topic_batches 
FOR VALUES IN (1); -- Topic 1 goes here

```


2. **Heap Only Tuples (HOT):** The `producer_state` table will be updated heavily. Ensure `fillfactor` is set to ~70% to allow in-page updates without rewriting indexes.
```sql
ALTER TABLE producer_state SET (fillfactor = 70);

```


3. **Connection Pooling:** You **must** use **PgBouncer** (in Transaction Mode) between your Agents and Postgres. Stateless Agents will open thousands of connections; Postgres cannot handle that natively. PgBouncer multiplexes them down to ~50-100 active connections.

### 5. Failure Mode: "The Database is Down"

Since Postgres is a single primary (usually):

* **Scenario:** The Primary node fails.
* **Impact:** **Global Write Outage.** No Agent can commit.
* **Behavior:** Agents return HTTP 503. Producers buffer in RAM.
* **Recovery:** Automated failover (AWS RDS Multi-AZ) promotes a replica (~30-60s). Writes resume.
* **Data Safety:** **Zero Data Loss.** Committed transactions are on the replica (Synchronous Replication recommended for strict safety, though it adds latency).

### 6. Next Step

Do you want to discuss the **Compaction Strategy** in SQL? (e.g., How to query for small batches and replace them with a large batch transactionally?)