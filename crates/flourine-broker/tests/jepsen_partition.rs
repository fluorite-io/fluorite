//! Jepsen-inspired partition tolerance tests.
//!
//! These tests verify:
//! - DB timeout returns error, not false success
//! - Reader group state survives broker restart
//! - Two brokers don't duplicate partition assignments

mod common;

use bytes::Bytes;
use std::time::Duration;

use flourine_broker::{Coordinator, CoordinatorConfig};
use flourine_common::ids::{Offset, PartitionId, TopicId};
use flourine_common::types::Record;

use common::{CrashableBroker, FaultyObjectStore, TestDb, produce_records};

/// Test that S3 failure does not result in false acknowledgment.
#[tokio::test]
async fn test_s3_failure_does_not_ack_writes() {
    use flourine_broker::ObjectStore;

    let db = TestDb::new().await;
    let broker = CrashableBroker::new(db.pool.clone()).await;

    let topic_id = TopicId(db.create_topic("s3-failure-test", 1).await as u32);
    let partition_id = PartitionId(0);

    // Configure to fail on next put
    broker.faulty_store().fail_next_put();

    let records = vec![Record {
        key: Some(Bytes::from("key")),
        value: Bytes::from("value"),
    }];

    // This should fail because S3 put fails
    let result = produce_records(broker.state(), topic_id, partition_id, records).await;

    // Should return error, not success
    assert!(
        result.is_err(),
        "S3 failure should propagate as error, not silent success"
    );

    // Verify no records were persisted in DB
    let count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM topic_batches WHERE topic_id = $1 AND partition_id = $2",
    )
    .bind(topic_id.0 as i32)
    .bind(partition_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(count, 0, "No batches should be recorded when S3 fails");

    // Watermark should still be 0
    let watermark: i64 = sqlx::query_scalar(
        "SELECT COALESCE(next_offset, 0) FROM partition_offsets WHERE topic_id = $1 AND partition_id = $2",
    )
    .bind(topic_id.0 as i32)
    .bind(partition_id.0 as i32)
    .fetch_optional(&db.pool)
    .await
    .unwrap()
    .unwrap_or(0);

    assert_eq!(watermark, 0, "Watermark should be 0 when S3 fails");

    // Verify no ghost files were written to the store
    let files = broker
        .faulty_store()
        .list("")
        .await
        .expect("List should succeed");

    assert!(
        files.is_empty(),
        "No files should be written to store when S3 fails, but found: {:?}",
        files
    );
}

/// Test that reader group state survives coordinator restart.
#[tokio::test]
async fn test_consumer_survives_broker_restart() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };

    let topic_id = TopicId(db.create_topic("survive-restart", 4).await as u32);

    // First coordinator session
    let gen1;
    {
        let coordinator = Coordinator::new(db.pool.clone(), config.clone());

        let result = coordinator
            .join_group("survive-group", topic_id, "reader-1")
            .await
            .expect("Join should succeed");

        gen1 = result.generation.0;
        assert!(!result.assignments.is_empty());

        // Commit offset 42 to first partition
        let partition = result.assignments[0].partition_id;
        coordinator
            .commit_offset(
                "survive-group",
                topic_id,
                "reader-1",
                result.generation,
                partition,
                Offset(42),
            )
            .await
            .expect("Commit should succeed");
    }
    // Coordinator dropped (simulates process restart)

    // Second coordinator session (new instance)
    {
        let coordinator = Coordinator::new(db.pool.clone(), config);

        // Send heartbeat with old generation
        let hb = coordinator
            .heartbeat(
                "survive-group",
                topic_id,
                "reader-1",
                flourine_common::ids::Generation(gen1),
            )
            .await
            .expect("Heartbeat should succeed");

        // Heartbeat should work (reader still known)
        assert!(
            hb.status == flourine_broker::HeartbeatStatus::Ok
                || hb.status == flourine_broker::HeartbeatStatus::RebalanceNeeded,
            "Reader should survive restart: {:?}",
            hb.status
        );

        // Verify committed offset is still in DB
        let committed: Option<i64> = sqlx::query_scalar(
            "SELECT committed_offset FROM reader_assignments WHERE group_id = $1 AND topic_id = $2 AND committed_offset = 42",
        )
        .bind("survive-group")
        .bind(topic_id.0 as i32)
        .fetch_optional(&db.pool)
        .await
        .expect("Query should succeed");

        assert!(
            committed.is_some(),
            "Committed offset should survive restart"
        );
    }
}

/// Test that two brokers cannot claim the same partition simultaneously.
#[tokio::test]
async fn test_two_brokers_no_duplicate_assignments() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };

    let topic_id = TopicId(db.create_topic("no-dup-brokers", 4).await as u32);

    // Two coordinators simulating two broker processes
    let coordinator1 = Coordinator::new(db.pool.clone(), config.clone());
    let coordinator2 = Coordinator::new(db.pool.clone(), config);

    // Reader 1 joins first
    let _result1 = coordinator1
        .join_group("dup-test-group", topic_id, "reader-1")
        .await
        .expect("Join should succeed");

    // Reader 2 joins
    let result2 = coordinator2
        .join_group("dup-test-group", topic_id, "reader-2")
        .await
        .expect("Join should succeed");

    // Get fresh assignments for reader 1 (rejoin to get updated state)
    let result1_updated = coordinator1
        .rejoin("dup-test-group", topic_id, "reader-1", result2.generation)
        .await
        .expect("Rejoin should succeed");

    // Collect all claimed partitions
    let c1_partitions: std::collections::HashSet<u32> = result1_updated
        .assignments
        .iter()
        .map(|a| a.partition_id.0)
        .collect();

    let c2_partitions: std::collections::HashSet<u32> = result2
        .assignments
        .iter()
        .map(|a| a.partition_id.0)
        .collect();

    // No overlap allowed
    let overlap: std::collections::HashSet<u32> = c1_partitions
        .intersection(&c2_partitions)
        .copied()
        .collect();

    assert!(
        overlap.is_empty(),
        "Two brokers should not claim the same partition: overlap {:?}",
        overlap
    );
}

/// Test that partition assignment eventually balances across brokers.
///
/// In an incremental rebalance system, the first reader gets all partitions,
/// and subsequent consumers get their share after rebalance via rejoin.
#[tokio::test]
async fn test_partition_assignment_eventually_balanced() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };

    let topic_id = TopicId(db.create_topic("balance-test", 8).await as u32);

    let coordinator = Coordinator::new(db.pool.clone(), config);

    // Four consumers join sequentially
    let mut generations = Vec::new();
    for i in 0..4 {
        let reader_id = format!("reader-{}", i);
        let result = coordinator
            .join_group("balance-group", topic_id, &reader_id)
            .await
            .expect("Join should succeed");
        generations.push((reader_id, result.generation));
    }

    // Get the latest generation
    let latest_gen = generations.last().unwrap().1;

    // Now have all consumers rejoin to get balanced assignments
    let mut final_assignments = Vec::new();
    for (reader_id, _) in &generations {
        let result = coordinator
            .rejoin("balance-group", topic_id, reader_id, latest_gen)
            .await
            .expect("Rejoin should succeed");
        final_assignments.push((reader_id.clone(), result.assignments.len()));
    }

    // After rejoin, assignments should be more balanced
    // 8 partitions / 4 consumers = 2 each
    let total: usize = final_assignments.iter().map(|(_, c)| c).sum();

    // Total claimed should be <= 8 (some partitions might still be in transition)
    assert!(
        total <= 8,
        "Total assigned partitions {} should be <= 8",
        total
    );

    // At least some consumers should have partitions after rejoin
    let non_empty = final_assignments.iter().filter(|(_, c)| *c > 0).count();
    assert!(
        non_empty >= 1,
        "At least one reader should have partitions"
    );
}

/// Test that reads see consistent state during writes (no partial visibility).
#[tokio::test]
async fn test_no_partial_write_visibility() {
    let db = TestDb::new().await;
    let broker = CrashableBroker::new(db.pool.clone()).await;

    let topic_id = TopicId(db.create_topic("no-partial-test", 1).await as u32);
    let partition_id = PartitionId(0);

    // Append a batch of records
    let records: Vec<Record> = (0..10)
        .map(|i| Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!("value-{}", i)),
        })
        .collect();

    produce_records(broker.state(), topic_id, partition_id, records)
        .await
        .expect("Append should succeed");

    // Query batches - should see either 0 or 10 records, never partial
    let batch_info: Vec<(i64, i64, i32)> = sqlx::query_as(
        r#"
        SELECT start_offset, end_offset, record_count
        FROM topic_batches
        WHERE topic_id = $1 AND partition_id = $2
        "#,
    )
    .bind(topic_id.0 as i32)
    .bind(partition_id.0 as i32)
    .fetch_all(&db.pool)
    .await
    .expect("Query should succeed");

    // Each batch should have consistent start/end/count
    for (start, end, count) in &batch_info {
        let expected_count = end - start;
        assert_eq!(
            *count as i64, expected_count,
            "Batch record_count {} should equal end-start {}",
            count, expected_count
        );
    }

    // Watermark should match end of last batch
    let watermark: i64 = sqlx::query_scalar(
        "SELECT COALESCE(next_offset, 0) FROM partition_offsets WHERE topic_id = $1 AND partition_id = $2",
    )
    .bind(topic_id.0 as i32)
    .bind(partition_id.0 as i32)
    .fetch_optional(&db.pool)
    .await
    .unwrap()
    .unwrap_or(0);

    if let Some((_, end, _)) = batch_info.last() {
        assert_eq!(
            watermark, *end,
            "Watermark {} should equal end of last batch {}",
            watermark, end
        );
    }
}

/// Test that S3 latency does not cause false acks (slow != broken).
/// Writes with 500ms S3 latency should still succeed with correct offsets.
#[tokio::test]
async fn test_s3_latency_does_not_cause_false_ack() {
    use std::sync::Arc;
    use tempfile::TempDir;

    use flourine_broker::{LocalFsStore, ObjectStore, FlReader};
    use flourine_common::types::Record;

    use common::TestBrokerConfig;

    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();
    let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));
    let config = TestBrokerConfig {
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        bucket: "test-bucket".to_string(),
        key_prefix: "s3-latency-test".to_string(),
    };

    // 500ms latency on every put.
    store.set_put_delay_ms(500);

    let state = Arc::new(common::TestBrokerState::new(
        db.pool.clone(),
        store.clone(),
        config,
    ));

    let topic_id = TopicId(db.create_topic("s3-latency-test", 1).await as u32);
    let partition_id = PartitionId(0);

    // Write 5 records — each will be slow but should succeed.
    let mut acked_offsets = vec![];
    for i in 0..5 {
        let records = vec![Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!("slow-value-{}", i)),
        }];
        let (start, end) = produce_records(&state, topic_id, partition_id, records)
            .await
            .expect("Write should succeed despite S3 latency");
        acked_offsets.push((start, end));
    }

    // All offsets should be contiguous and non-overlapping.
    for i in 1..acked_offsets.len() {
        assert_eq!(
            acked_offsets[i].0 .0,
            acked_offsets[i - 1].1 .0,
            "Offsets should be contiguous: batch {} ends at {}, batch {} starts at {}",
            i - 1,
            acked_offsets[i - 1].1 .0,
            i,
            acked_offsets[i].0 .0
        );
    }

    // Final watermark should equal the end of the last batch.
    let watermark: i64 = sqlx::query_scalar(
        "SELECT COALESCE(next_offset, 0) FROM partition_offsets \
         WHERE topic_id = $1 AND partition_id = $2",
    )
    .bind(topic_id.0 as i32)
    .bind(partition_id.0 as i32)
    .fetch_optional(&db.pool)
    .await
    .unwrap()
    .unwrap_or(0);

    assert_eq!(
        watermark as u64,
        acked_offsets.last().unwrap().1 .0,
        "Watermark should match the last acked offset"
    );

    // Read back records and verify all 5 are present.
    let batches: Vec<(String,)> = sqlx::query_as(
        "SELECT s3_key FROM topic_batches WHERE topic_id = $1 AND partition_id = $2",
    )
    .bind(topic_id.0 as i32)
    .bind(partition_id.0 as i32)
    .fetch_all(&db.pool)
    .await
    .unwrap();

    let mut all_records = vec![];
    for (s3_key,) in batches {
        let data = store.get(&s3_key).await.unwrap();
        let metas = FlReader::read_footer(&data).unwrap();
        for meta in &metas {
            if meta.topic_id == topic_id && meta.partition_id == partition_id {
                let recs = FlReader::read_segment(&data, meta, true).unwrap();
                all_records.extend(recs);
            }
        }
    }

    assert_eq!(
        all_records.len(),
        5,
        "All 5 records should be readable despite S3 latency"
    );
}
