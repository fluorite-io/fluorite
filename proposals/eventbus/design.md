This is a formal design proposal for the **Stateless Avro Log Architecture**. This design decouples compute from storage, enforces strict data governance via Avro, and achieves exactly-once semantics through optimistic concurrency.

---

# Design Proposal: The Avro-Enforced Stateless Log

### 1. Architectural Topology

The system removes the concept of a stateful "Broker." Instead, it uses stateless Agents that translate the stream protocol into immutable object storage files.

**A. Smart Producer (The Client)**

* **Role:** Serializes data to Avro, batches records, and handles retries.
* **Constraint:** Must maintain a monotonically increasing `Sequence_Num` per `Producer_ID`.

**B. Stateless Agent (The Gateway)**

* **Role:** Ephemeral compute layer.
* **Responsibilities:**
1. **Validation:** Enforces Schema Registry checks on incoming bytes.
2. **Buffering:** Aggregates records into a Batch (RAM).
3. **Optimistic I/O:** flushes batches to S3 *before* committing.
4. **Coordination:** Commits S3 references to the Metadata Store.



**C. Metadata Store (The Sequencer)**

* **Role:** The Atomic Source of Truth (e.g., FoundationDB, CockroachDB).
* **Responsibilities:**
1. **Log Virtualization:** Maps logical offsets (0, 1, 2) to physical S3 keys.
2. **Deduplication:** Tracks `Producer_ID` + `Last_Seq` to prevent duplicates.
3. **Ordering:** Assigns the dense integer offsets.



**D. Object Storage (The Swamp)**

* **Role:** Dumb, cheap, infinite persistence (S3/GCS).
* **State:** Contains both "Valid" files (referenced by Metadata) and "Ghost" files (orphaned by crashes).

---

### 2. Core Workflows & Pseudo-Code

#### Workflow A: The Optimistic Write (Exactly-Once)

*Goal: Maximize throughput by writing to S3 immediately, using the Metadata Store only for finalization and deduplication.*

**Producer Logic**

```text
INIT Producer_ID = METADATA.REGISTER()
INIT Seq = 0

LOOP Forever:
  Batch = BUFFER_RECORDS()
  
  # Retry Loop: Send same Seq until explicit Ack
  LOOP:
    Response = CALL AGENT.PRODUCE(Producer_ID, Seq, Batch)
    
    CASE Response:
      HTTP_200: 
        Seq++
        BREAK LOOP
      HTTP_500/TIMEOUT:
        SLEEP(Backoff)
        CONTINUE LOOP # Retry with SAME Seq

```

**Agent Logic**

```text
FUNCTION HANDLE_PRODUCE(P_ID, Seq, Batch):

  # 1. Schema Enforcement
  Schema_ID = EXTRACT_HEADER(Batch)
  IF NOT REGISTRY.VALIDATE(Schema_ID):
    RETURN HTTP_400("Invalid Schema")

  # 2. Optimistic Write (The "Risk")
  # We write to S3 immediately. If we crash after this, it's a "Ghost File".
  S3_Key = S3.PUT(Bucket, UUID(), Batch)

  # 3. Atomic Commit (The "Gatekeeper")
  # We ask Metadata Store to finalize.
  Result = METADATA.CAS_COMMIT(
    Key: P_ID,
    Check_Seq: Seq - 1,
    Set_Seq: Seq,
    Payload: S3_Key
  )

  CASE Result:
    SUCCESS:
      # Happy Path: Metadata Store assigned offsets 1000-1099
      RETURN HTTP_200(Offset=1000)
      
    DUPLICATE_SEQ:
      # Failover Detected: A previous Agent wrote this Seq but crashed.
      # 1. Fetch the offset assigned to the ORIGINAL write
      Old_Offset = METADATA.GET_OFFSET(P_ID, Seq)
      # 2. Delete our redundant "Ghost File"
      S3.DELETE(S3_Key)
      # 3. Ack the client
      RETURN HTTP_200(Offset=Old_Offset)
      
    FAILURE:
      RETURN HTTP_500

```

---

#### Workflow B: The Smart Read (Virtual Log)

*Goal: Offload bandwidth from the Agent by allowing Consumers to read directly from S3.*

**Consumer Logic**

```text
INIT Current_Offset = FETCH_COMMITTED()

LOOP Forever:
  # 1. Get the "Map" from Agent
  Manifest = AGENT.FETCH(Topic, Current_Offset)
  
  FOR Batch IN Manifest:
    # 2. Direct S3 Read (High Bandwidth)
    Raw_Data = HTTP_GET(Batch.Signed_URL)
    
    # 3. Client-Side Projection
    # Project Writer_Schema (from batch) -> Reader_Schema (local)
    Records = AVRO_PROJECT(Raw_Data, Batch.Schema_ID, My_Schema)
    
    PROCESS(Records)
    Current_Offset = Batch.End_Offset + 1

```

**Agent Logic (Fetch Handler)**

```text
FUNCTION FETCH(Topic, Offset):
  # Query Metadata: "Which files contain offsets >= Offset?"
  Files = METADATA.QUERY_INDEX(Topic, Offset)
  
  Response = []
  FOR File IN Files:
    # Generate temporary access URL
    Url = S3.SIGN_URL(File.Key, Expires=60s)
    Response.APPEND(Url, File.Min_Offset, File.Max_Offset)
    
  RETURN Response

```

---

### 3. Failure Analysis Matrix

| Scenario | System State | Resolution Mechanism |
| --- | --- | --- |
| **Agent Crash (Post-S3 Write, Pre-Commit)** | S3 has file; Metadata has nothing. | **"Ghost File":** Producer retries. New Agent writes new file. Old file is ignored by Compactor and deleted by async Garbage Collector (GC). |
| **Agent Crash (Post-Commit, Pre-Ack)** | S3 has file; Metadata has record. Producer thinks fail. | **Idempotency:** Producer retries. New Agent attempts commit. Metadata Store detects `Duplicate Seq`. Agent returns original offset. |
| **Metadata Store Outage** | S3 works; Metadata unavailable. | **System Halt:** Agents return HTTP 503. Producers buffer locally until service restores. Ensures consistency over availability. |
| **Schema Drift** | Producer on V2, Consumer on V1. | **Avro Resolution:** Consumer downloads V2 file. Avro library ignores new fields (V2) when projecting to Reader Schema (V1). |

---

### 4. Background Processes

#### **The Garbage Collector (GC)**

Since the "Optimistic Write" creates orphaned files during crashes, we need a janitor.

**Logic:** Cross-reference S3 listing with metadata store (on read replica).

```sql
-- Find orphaned files: in S3 but not referenced by any segment
-- Run against read replica to avoid impacting write path
SELECT s3_key
FROM s3_file_listing
WHERE created_at < NOW() - INTERVAL '2 hours'
  AND s3_key NOT IN (SELECT DISTINCT s3_key FROM topic_batches);
```

**Action:** Delete orphaned S3 files.

**Parameters:**
- `GC_AGE_THRESHOLD`: 2 hours (only consider files older than this)
- `GC_INTERVAL`: Run hourly

**Why 2 hours?** Provides safety margin for:
- Agent batch window (400ms)
- DB commit retries (seconds)
- Clock skew between agents
- Any in-flight operations

**Performance:** GC runs on read replica, so no impact on write path. S3 listing is paginated. Query is a simple anti-join on indexed `s3_key` column.

#### **Retention**

Deletes segments older than the retention period. No compaction — analytical workloads use a separate Iceberg/Parquet pipeline.

* **Logic:** Drop old time partitions from `topic_batches` table.
* **Action:** `DROP TABLE topic_batches_2024_01` (instant, no row-by-row delete).
* **S3 Cleanup:** GC deletes orphaned files on next run.

```sql
-- Example: drop partitions older than 7 days
DO $$
DECLARE
    partition_name TEXT;
BEGIN
    FOR partition_name IN
        SELECT tablename FROM pg_tables
        WHERE tablename LIKE 'topic_batches_%'
          AND tablename < 'topic_batches_' || to_char(NOW() - INTERVAL '7 days', 'YYYY_MM_DD')
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || partition_name;
    END LOOP;
END $$;
```

### 5. Final Verdict

This design favors **Consistency** and **Throughput**.

* **Pros:** Strict ordering, Exactly-Once semantics, separation of compute/storage costs.
* **Cons:** Higher latency than local disk (S3 RTT), requires a robust Metadata Store (the bottleneck).