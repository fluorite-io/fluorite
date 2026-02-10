//! Database integration tests.
//!
//! These tests require a running PostgreSQL instance.
//!
//! ```bash
//! # Start Postgres
//! docker run -d --name turbine-postgres \
//!   -e POSTGRES_PASSWORD=turbine \
//!   -p 5433:5432 \
//!   postgres:16
//!
//! # Run tests
//! DATABASE_URL=postgres://postgres:turbine@localhost:5433 cargo test --test db_integration
//! ```

mod common;

use bytes::Bytes;
use tempfile::TempDir;

use turbine_agent::{LocalFsStore, ObjectStore, TbinWriter};
use turbine_common::ids::{PartitionId, SchemaId, TopicId};
use turbine_common::types::{Record, Segment};

use common::TestDb;

/// Skip test if DATABASE_URL is not set.
fn skip_if_no_db() -> bool {
    std::env::var("DATABASE_URL").is_err()
}

/// Test basic topic and partition creation.
#[tokio::test]
async fn test_create_topic_with_partitions() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;

    // Create topic with 3 partitions
    let topic_id = db.create_topic("test-topic", 3).await;
    assert!(topic_id > 0);

    // Verify partitions exist
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM partition_offsets WHERE topic_id = $1",
    )
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
        assert_eq!(offset, 0, "Partition {} should start at offset 0", partition_id);
    }
}

/// Test inserting and querying topic_batches.
#[tokio::test]
async fn test_insert_topic_batch() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("batch-test", 1).await;

    // Insert a batch
    let ingest_time = chrono::Utc::now();
    sqlx::query(
        r#"
        INSERT INTO topic_batches (
            topic_id, partition_id, schema_id,
            start_offset, end_offset, record_count,
            s3_key, ingest_time
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        "#,
    )
    .bind(topic_id)
    .bind(0i32)
    .bind(100i32)
    .bind(0i64)
    .bind(10i64)
    .bind(10i32)
    .bind("data/2024-01-15/batch-001.tbin")
    .bind(ingest_time)
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
    assert_eq!(batch.1, 0);   // start_offset
    assert_eq!(batch.2, 10);  // end_offset
    assert_eq!(batch.3, "data/2024-01-15/batch-001.tbin");
}

/// Test offset management (get current, update, concurrent updates).
#[tokio::test]
async fn test_offset_management() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

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

    // Update offset (simulate produce)
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

/// Test producer state for deduplication.
#[tokio::test]
async fn test_producer_state_dedup() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let producer_id = uuid::Uuid::new_v4();

    // Insert initial state
    sqlx::query(
        "INSERT INTO producer_state (producer_id, last_seq_num) VALUES ($1, $2)",
    )
    .bind(producer_id)
    .bind(1i64)
    .execute(&db.pool)
    .await
    .unwrap();

    // Check if sequence is duplicate
    let last_seq: i64 = sqlx::query_scalar(
        "SELECT last_seq_num FROM producer_state WHERE producer_id = $1",
    )
    .bind(producer_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(last_seq, 1);

    // Update sequence (upsert)
    sqlx::query(
        r#"
        INSERT INTO producer_state (producer_id, last_seq_num, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (producer_id)
        DO UPDATE SET last_seq_num = $2, updated_at = NOW()
        "#,
    )
    .bind(producer_id)
    .bind(2i64)
    .execute(&db.pool)
    .await
    .unwrap();

    let updated_seq: i64 = sqlx::query_scalar(
        "SELECT last_seq_num FROM producer_state WHERE producer_id = $1",
    )
    .bind(producer_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(updated_seq, 2);
}

/// Test full produce flow with database.
#[tokio::test]
async fn test_produce_flow_with_db() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());

    // Create topic
    let topic_id = db.create_topic("produce-flow-test", 2).await;

    // Create segments
    let segments = vec![
        Segment {
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
        Segment {
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
    for segment in &segments {
        writer.add_segment(segment).unwrap();
    }
    let tbin_data = writer.finish();

    // Write to S3
    let s3_key = "data/2024-01-15/batch-001.tbin";
    store.put(s3_key, tbin_data).await.unwrap();

    // Start transaction
    let mut tx = db.pool.begin().await.unwrap();
    let ingest_time = chrono::Utc::now();

    for segment in &segments {
        let partition_id = segment.partition_id.0 as i32;
        let record_count = segment.records.len() as i64;

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
                s3_key, ingest_time
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(topic_id)
        .bind(partition_id)
        .bind(segment.schema_id.0 as i32)
        .bind(start_offset)
        .bind(end_offset)
        .bind(record_count as i32)
        .bind(s3_key)
        .bind(ingest_time)
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
    let batch_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM topic_batches WHERE topic_id = $1",
    )
    .bind(topic_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(batch_count, 2);
}

/// Test fetch query (find batches by offset).
#[tokio::test]
async fn test_fetch_query() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("fetch-test", 1).await;
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
                s3_key, ingest_time
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(topic_id)
        .bind(0i32)
        .bind(100i32)
        .bind(start)
        .bind(end)
        .bind(100i32)
        .bind(format!("data/batch-{:03}.tbin", i))
        .bind(ingest_time)
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

    // Fetch from offset 250 (should find batch starting at 200)
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

    // Fetch from offset 0 (should find first batch)
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
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

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
                    s3_key, ingest_time
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                "#,
            )
            .bind(topic_id)
            .bind(0i32)
            .bind(100i32)
            .bind(current)
            .bind(new_offset)
            .bind(record_count as i32)
            .bind(format!("data/batch-{:03}.tbin", i))
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
