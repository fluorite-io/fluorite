//! Database integration tests.
//!
//! These tests require a running PostgreSQL instance.
//!
//! ```bash
//! # Start Postgres
//! docker run -d --name flourine-postgres \
//!   -e POSTGRES_PASSWORD=postgres \
//!   -p 5433:5432 \
//!   postgres:16
//!
//! # Run tests
//! DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test --test db_integration
//! ```

mod common;

use bytes::Bytes;
use tempfile::TempDir;

use flourine_broker::{LocalFsStore, ObjectStore, TbinWriter};
use flourine_common::ids::{PartitionId, SchemaId, TopicId};
use flourine_common::types::{Record, RecordBatch};

use common::TestDb;

/// Test basic topic and partition creation.
#[tokio::test]
async fn test_create_topic_with_partitions() {
    let db = TestDb::new().await;

    // Create topic with 3 partitions
    let topic_id = db.create_topic("test-topic", 3).await;
    assert!(topic_id > 0);

    // Verify partitions exist
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM partition_offsets WHERE topic_id = $1")
            .bind(topic_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();

    assert_eq!(count, 3);

    // Verify all partitions start at offset 0
    let offsets: Vec<(i32, i64)> = sqlx::query_as(
        "SELECT partition_id, next_offset FROM partition_offsets WHERE topic_id = $1 ORDER BY partition_id",
    )
    .bind(topic_id)
    .fetch_all(&db.pool)
    .await
    .unwrap();

    assert_eq!(offsets.len(), 3);
    for (partition_id, offset) in offsets {
        assert_eq!(
            offset, 0,
            "Partition {} should start at offset 0",
            partition_id
        );
    }
}

/// Test inserting and querying topic_batches.
#[tokio::test]
async fn test_insert_topic_batch() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("batch-test", 1).await;

    // Insert a batch
    let ingest_time = chrono::Utc::now();
    sqlx::query(
        r#"
        INSERT INTO topic_batches (
            topic_id, partition_id, schema_id,
            start_offset, end_offset, record_count,
            s3_key, byte_offset, byte_length, ingest_time, crc32
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        "#,
    )
    .bind(topic_id)
    .bind(0i32)
    .bind(100i32)
    .bind(0i64)
    .bind(10i64)
    .bind(10i32)
    .bind("data/2024-01-15/batch-001.tbin")
    .bind(0i64) // byte_offset
    .bind(0i64) // byte_length
    .bind(ingest_time)
    .bind(0i64) // crc32
    .execute(&db.pool)
    .await
    .unwrap();

    // Query it back
    let batch: (i32, i64, i64, String) = sqlx::query_as(
        "SELECT schema_id, start_offset, end_offset, s3_key FROM topic_batches WHERE topic_id = $1",
    )
    .bind(topic_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(batch.0, 100); // schema_id
    assert_eq!(batch.1, 0); // start_offset
    assert_eq!(batch.2, 10); // end_offset
    assert_eq!(batch.3, "data/2024-01-15/batch-001.tbin");
}

/// Test offset management (get current, update, concurrent updates).
#[tokio::test]
async fn test_offset_management() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("offset-test", 2).await;

    // Get initial offset
    let offset: i64 = sqlx::query_scalar(
        "SELECT next_offset FROM partition_offsets WHERE topic_id = $1 AND partition_id = 0",
    )
    .bind(topic_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(offset, 0);

    // Update offset (simulate append)
    let record_count = 100i64;
    sqlx::query(
        "UPDATE partition_offsets SET next_offset = next_offset + $3 WHERE topic_id = $1 AND partition_id = $2",
    )
    .bind(topic_id)
    .bind(0i32)
    .bind(record_count)
    .execute(&db.pool)
    .await
    .unwrap();

    // Verify new offset
    let new_offset: i64 = sqlx::query_scalar(
        "SELECT next_offset FROM partition_offsets WHERE topic_id = $1 AND partition_id = 0",
    )
    .bind(topic_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(new_offset, 100);
}

/// Test writer state for deduplication.
#[tokio::test]
async fn test_writer_state_dedup() {
    let db = TestDb::new().await;
    let writer_id = uuid::Uuid::new_v4();

    // Insert initial state
    sqlx::query(
        "INSERT INTO writer_state (writer_id, last_seq, last_acks) VALUES ($1, $2, $3)",
    )
    .bind(writer_id)
    .bind(1i64)
    .bind(serde_json::json!([]))
    .execute(&db.pool)
    .await
    .unwrap();

    // Check if sequence is duplicate
    let last_seq: i64 =
        sqlx::query_scalar("SELECT last_seq FROM writer_state WHERE writer_id = $1")
            .bind(writer_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();

    assert_eq!(last_seq, 1);

    // Update sequence (upsert)
    sqlx::query(
        r#"
        INSERT INTO writer_state (writer_id, last_seq, last_acks, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (writer_id)
        DO UPDATE SET last_seq = $2, last_acks = $3, updated_at = NOW()
        "#,
    )
    .bind(writer_id)
    .bind(2i64)
    .bind(serde_json::json!([]))
    .execute(&db.pool)
    .await
    .unwrap();

    let updated_seq: i64 =
        sqlx::query_scalar("SELECT last_seq FROM writer_state WHERE writer_id = $1")
            .bind(writer_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();

    assert_eq!(updated_seq, 2);
}

/// Test full append flow with database.
#[tokio::test]
async fn test_produce_flow_with_db() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());

    // Create topic
    let topic_id = db.create_topic("append-flow-test", 2).await;

    // Create batches
    let batches = vec![
        RecordBatch {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            schema_id: SchemaId(100),
            records: vec![
                Record {
                    key: Some(Bytes::from("k1")),
                    value: Bytes::from("v1"),
                },
                Record {
                    key: Some(Bytes::from("k2")),
                    value: Bytes::from("v2"),
                },
            ],
        },
        RecordBatch {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(1),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from("k3")),
                value: Bytes::from("v3"),
            }],
        },
    ];

    // Write TBIN
    let mut writer = TbinWriter::new();
    for batch in &batches {
        writer.add_segment(batch).unwrap();
    }
    let tbin_data = writer.finish();

    // Write to S3
    let s3_key = "data/2024-01-15/batch-001.tbin";
    store.put(s3_key, tbin_data).await.unwrap();

    // Start transaction
    let mut tx = db.pool.begin().await.unwrap();
    let ingest_time = chrono::Utc::now();

    for batch in &batches {
        let partition_id = batch.partition_id.0 as i32;
        let record_count = batch.records.len() as i64;

        // Get current offset with lock
        let current_offset: i64 = sqlx::query_scalar(
            "SELECT next_offset FROM partition_offsets WHERE topic_id = $1 AND partition_id = $2 FOR UPDATE",
        )
        .bind(topic_id)
        .bind(partition_id)
        .fetch_one(&mut *tx)
        .await
        .unwrap();

        let start_offset = current_offset;
        let end_offset = current_offset + record_count;

        // Update partition offset
        sqlx::query(
            "UPDATE partition_offsets SET next_offset = $3 WHERE topic_id = $1 AND partition_id = $2",
        )
        .bind(topic_id)
        .bind(partition_id)
        .bind(end_offset)
        .execute(&mut *tx)
        .await
        .unwrap();

        // Insert batch record
        sqlx::query(
            r#"
            INSERT INTO topic_batches (
                topic_id, partition_id, schema_id,
                start_offset, end_offset, record_count,
                s3_key, byte_offset, byte_length, ingest_time, crc32
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(topic_id)
        .bind(partition_id)
        .bind(batch.schema_id.0 as i32)
        .bind(start_offset)
        .bind(end_offset)
        .bind(record_count as i32)
        .bind(s3_key)
        .bind(0i64) // byte_offset
        .bind(0i64) // byte_length
        .bind(ingest_time)
        .bind(0i64) // crc32
        .execute(&mut *tx)
        .await
        .unwrap();
    }

    tx.commit().await.unwrap();

    // Verify offsets were updated
    let p0_offset: i64 = sqlx::query_scalar(
        "SELECT next_offset FROM partition_offsets WHERE topic_id = $1 AND partition_id = 0",
    )
    .bind(topic_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();

    let p1_offset: i64 = sqlx::query_scalar(
        "SELECT next_offset FROM partition_offsets WHERE topic_id = $1 AND partition_id = 1",
    )
    .bind(topic_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(p0_offset, 2); // 2 records in partition 0
    assert_eq!(p1_offset, 1); // 1 record in partition 1

    // Verify batch records
    let batch_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM topic_batches WHERE topic_id = $1")
            .bind(topic_id)
            .fetch_one(&db.pool)
            .await
            .unwrap();

    assert_eq!(batch_count, 2);
}

/// Test read query (find batches by offset).
#[tokio::test]
async fn test_fetch_query() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("read-test", 1).await;
    let ingest_time = chrono::Utc::now();

    // Insert multiple batches
    for i in 0..5 {
        let start = i * 100;
        let end = (i + 1) * 100;
        sqlx::query(
            r#"
            INSERT INTO topic_batches (
                topic_id, partition_id, schema_id,
                start_offset, end_offset, record_count,
                s3_key, byte_offset, byte_length, ingest_time, crc32
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(topic_id)
        .bind(0i32)
        .bind(100i32)
        .bind(start)
        .bind(end)
        .bind(100i32)
        .bind(format!("data/batch-{:03}.tbin", i))
        .bind(0i64) // byte_offset
        .bind(0i64) // byte_length
        .bind(ingest_time)
        .bind(0i64) // crc32
        .execute(&db.pool)
        .await
        .unwrap();
    }

    // Update partition offset
    sqlx::query(
        "UPDATE partition_offsets SET next_offset = 500 WHERE topic_id = $1 AND partition_id = 0",
    )
    .bind(topic_id)
    .execute(&db.pool)
    .await
    .unwrap();

    // Read from offset 250 (should find batch starting at 200)
    let batches: Vec<(i64, i64, String)> = sqlx::query_as(
        r#"
        SELECT start_offset, end_offset, s3_key
        FROM topic_batches
        WHERE topic_id = $1
          AND partition_id = $2
          AND start_offset <= $3
          AND end_offset > $3
        ORDER BY start_offset
        "#,
    )
    .bind(topic_id)
    .bind(0i32)
    .bind(250i64)
    .fetch_all(&db.pool)
    .await
    .unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].0, 200); // start_offset
    assert_eq!(batches[0].1, 300); // end_offset
    assert_eq!(batches[0].2, "data/batch-002.tbin");

    // Read from offset 0 (should find first batch)
    let batches: Vec<(i64, i64, String)> = sqlx::query_as(
        r#"
        SELECT start_offset, end_offset, s3_key
        FROM topic_batches
        WHERE topic_id = $1
          AND partition_id = $2
          AND start_offset <= $3
          AND end_offset > $3
        ORDER BY start_offset
        "#,
    )
    .bind(topic_id)
    .bind(0i32)
    .bind(0i64)
    .fetch_all(&db.pool)
    .await
    .unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].0, 0);
    assert_eq!(batches[0].1, 100);
}

/// Test concurrent produces to same partition.
#[tokio::test]
async fn test_concurrent_produces() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("concurrent-test", 1).await;
    let pool = db.pool.clone();

    // Spawn multiple concurrent produces
    let mut handles = vec![];
    for i in 0..10 {
        let pool = pool.clone();
        let handle = tokio::spawn(async move {
            let mut tx = pool.begin().await.unwrap();

            // Get offset with lock
            let current: i64 = sqlx::query_scalar(
                "SELECT next_offset FROM partition_offsets WHERE topic_id = $1 AND partition_id = 0 FOR UPDATE",
            )
            .bind(topic_id)
            .bind(0i32)
            .fetch_one(&mut *tx)
            .await
            .unwrap();

            let record_count = 10i64;
            let new_offset = current + record_count;

            // Update offset
            sqlx::query(
                "UPDATE partition_offsets SET next_offset = $3 WHERE topic_id = $1 AND partition_id = $2",
            )
            .bind(topic_id)
            .bind(0i32)
            .bind(new_offset)
            .execute(&mut *tx)
            .await
            .unwrap();

            // Insert batch
            sqlx::query(
                r#"
                INSERT INTO topic_batches (
                    topic_id, partition_id, schema_id,
                    start_offset, end_offset, record_count,
                    s3_key, byte_offset, byte_length, ingest_time, crc32
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), $10)
                "#,
            )
            .bind(topic_id)
            .bind(0i32)
            .bind(100i32)
            .bind(current)
            .bind(new_offset)
            .bind(record_count as i32)
            .bind(format!("data/batch-{:03}.tbin", i))
            .bind(0i64) // byte_offset
            .bind(0i64) // byte_length
            .bind(0i64) // crc32
            .execute(&mut *tx)
            .await
            .unwrap();

            tx.commit().await.unwrap();

            (current, new_offset)
        });
        handles.push(handle);
    }

    // Wait for all to complete
    let mut results = vec![];
    for handle in handles {
        results.push(handle.await.unwrap());
    }

    // Verify final offset is 100 (10 produces x 10 records each)
    let final_offset: i64 = sqlx::query_scalar(
        "SELECT next_offset FROM partition_offsets WHERE topic_id = $1 AND partition_id = 0",
    )
    .bind(topic_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(final_offset, 100);

    // Verify no overlapping ranges
    results.sort_by_key(|(start, _)| *start);
    for i in 1..results.len() {
        assert_eq!(
            results[i - 1].1,
            results[i].0,
            "Ranges should be contiguous: {:?} -> {:?}",
            results[i - 1],
            results[i]
        );
    }
}
