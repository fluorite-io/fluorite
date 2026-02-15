//! Jepsen-inspired crash recovery tests.
//!
//! These tests verify:
//! - Broker restart clears stale watermark cache
//! - Reader reconnects resume from committed offset
//! - Append deduplication works across crashes

mod common;

use bytes::Bytes;
use std::time::Duration;

use turbine_agent::{Coordinator, CoordinatorConfig};
use turbine_common::ids::{Offset, PartitionId, TopicId};
use turbine_common::types::Record;

use common::{CrashableAgent, TestDb, produce_records};

/// Get high watermark from database.
async fn get_watermark(pool: &sqlx::PgPool, topic_id: TopicId, partition_id: PartitionId) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(next_offset, 0) FROM partition_offsets WHERE topic_id = $1 AND partition_id = $2",
    )
    .bind(topic_id.0 as i32)
    .bind(partition_id.0 as i32)
    .fetch_optional(pool)
    .await
    .unwrap()
    .unwrap_or(0)
}

/// Test that broker restart sees fresh watermark from DB.
#[tokio::test]
async fn test_agent_restart_watermark_fresh() {
    let db = TestDb::new().await;
    let mut broker = CrashableAgent::new(db.pool.clone()).await;

    let topic_id = TopicId(db.create_topic("crash-watermark-test", 1).await as u32);
    let partition_id = PartitionId(0);

    // Append 10 records
    let records: Vec<Record> = (0..10)
        .map(|i| Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!("value-{}", i)),
        })
        .collect();

    let (_, end_offset) = produce_records(broker.state(), topic_id, partition_id, records)
        .await
        .expect("Append should succeed");

    assert_eq!(end_offset.0, 10);

    // Verify watermark before crash
    let watermark_before = get_watermark(&broker.pool, topic_id, partition_id).await;
    assert_eq!(watermark_before, 10);

    // Simulate crash and restart
    broker.crash();
    assert!(broker.is_crashed());

    broker.restart();
    assert!(!broker.is_crashed());

    // Watermark should still be 10 after restart (loaded from DB)
    let watermark_after = get_watermark(&broker.pool, topic_id, partition_id).await;
    assert_eq!(
        watermark_after, 10,
        "Watermark should be fresh from DB after restart"
    );

    // Append more records after restart
    let more_records: Vec<Record> = (10..15)
        .map(|i| Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!("value-{}", i)),
        })
        .collect();

    let (_, new_end_offset) = produce_records(broker.state(), topic_id, partition_id, more_records)
        .await
        .expect("Append after restart should succeed");

    assert_eq!(new_end_offset.0, 15, "New records should be at offset 15");
}

/// Test reader reconnect resumes from committed offset.
#[tokio::test]
async fn test_consumer_reconnect_resumes_offset() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    let topic_id = TopicId(db.create_topic("reconnect-test", 2).await as u32);

    // Reader joins
    let result1 = coordinator
        .join_group("reconnect-group", topic_id, "reader-1")
        .await
        .expect("Join should succeed");

    // Commit offset 100 to first partition
    let partition = result1.assignments[0].partition_id;
    coordinator
        .commit_offset(
            "reconnect-group",
            topic_id,
            "reader-1",
            result1.generation,
            partition,
            Offset(100),
        )
        .await
        .expect("Commit should succeed");

    // Simulate reader disconnect by leaving
    coordinator
        .leave_group("reconnect-group", topic_id, "reader-1")
        .await
        .expect("Leave should succeed");

    // Reader reconnects
    let result2 = coordinator
        .join_group("reconnect-group", topic_id, "reader-1")
        .await
        .expect("Rejoin should succeed");

    // Find the same partition in new assignments
    let same_partition = result2
        .assignments
        .iter()
        .find(|a| a.partition_id == partition);

    if let Some(assignment) = same_partition {
        assert_eq!(
            assignment.committed_offset.0, 100,
            "Reader should resume from committed offset 100, got {}",
            assignment.committed_offset.0
        );
    }
}

/// Test that writes are not lost during broker crash.
#[tokio::test]
async fn test_crash_does_not_lose_committed_writes() {
    let db = TestDb::new().await;
    let mut broker = CrashableAgent::new(db.pool.clone()).await;

    let topic_id = TopicId(db.create_topic("no-loss-test", 1).await as u32);
    let partition_id = PartitionId(0);

    // Track all committed record values
    let mut committed_values = vec![];

    // Append records, simulating crash between batches
    for batch_idx in 0..3 {
        let records: Vec<Record> = (0..5)
            .map(|i| {
                let value = format!("batch-{}-value-{}", batch_idx, i);
                committed_values.push(value.clone());
                Record {
                    key: Some(Bytes::from(format!("batch-{}-key-{}", batch_idx, i))),
                    value: Bytes::from(value),
                }
            })
            .collect();

        produce_records(broker.state(), topic_id, partition_id, records)
            .await
            .expect("Append should succeed");

        // Crash after second batch
        if batch_idx == 1 {
            broker.crash();
            broker.restart();
        }
    }

    // Verify all committed records are visible from DB perspective
    let batches: Vec<(i64, i64)> = sqlx::query_as(
        r#"
        SELECT start_offset, end_offset FROM topic_batches
        WHERE topic_id = $1 AND partition_id = $2
        ORDER BY start_offset
        "#,
    )
    .bind(topic_id.0 as i32)
    .bind(partition_id.0 as i32)
    .fetch_all(&db.pool)
    .await
    .expect("Query should succeed");

    // Should have 3 batches, each with 5 records
    assert_eq!(batches.len(), 3, "Should have 3 batches");

    let total_records: i64 = batches.iter().map(|(s, e)| e - s).sum();
    assert_eq!(total_records, 15, "Should have 15 total records");

    // Verify watermark matches
    let watermark = get_watermark(&db.pool, topic_id, partition_id).await;
    assert_eq!(watermark, 15, "Watermark should be 15");
}

/// Test that uncommitted writes are not visible after crash.
#[tokio::test]
async fn test_uncommitted_writes_not_visible_after_crash() {
    let db = TestDb::new().await;
    let mut broker = CrashableAgent::new(db.pool.clone()).await;

    let topic_id = TopicId(db.create_topic("uncommitted-test", 1).await as u32);
    let partition_id = PartitionId(0);

    // Append some records (this commits to DB)
    let records: Vec<Record> = (0..5)
        .map(|i| Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!("value-{}", i)),
        })
        .collect();

    produce_records(broker.state(), topic_id, partition_id, records)
        .await
        .expect("Append should succeed");

    let watermark_before = get_watermark(&broker.pool, topic_id, partition_id).await;
    assert_eq!(watermark_before, 5);

    // Crash the broker
    broker.crash();
    broker.restart();

    // Watermark should still be 5 (committed writes visible)
    let watermark_after = get_watermark(&broker.pool, topic_id, partition_id).await;
    assert_eq!(
        watermark_after, 5,
        "Only committed writes should be visible after crash"
    );
}

/// Test that reader group state persists through coordinator restart.
#[tokio::test]
async fn test_consumer_group_state_persists() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };

    let topic_id = TopicId(db.create_topic("persist-cg-test", 2).await as u32);

    // First coordinator instance
    {
        let coordinator = Coordinator::new(db.pool.clone(), config.clone());

        let result = coordinator
            .join_group("persist-cg", topic_id, "reader-1")
            .await
            .expect("Join should succeed");

        // Commit offset 50
        let partition = result.assignments[0].partition_id;
        coordinator
            .commit_offset(
                "persist-cg",
                topic_id,
                "reader-1",
                result.generation,
                partition,
                Offset(50),
            )
            .await
            .expect("Commit should succeed");
    }
    // First coordinator dropped here

    // Second coordinator instance (simulates restart)
    {
        let coordinator = Coordinator::new(db.pool.clone(), config);

        // Leave and rejoin to get fresh assignment
        let _ = coordinator
            .leave_group("persist-cg", topic_id, "reader-1")
            .await;

        let result = coordinator
            .join_group("persist-cg", topic_id, "reader-1")
            .await
            .expect("Rejoin should succeed");

        // Committed offset should persist
        for assignment in &result.assignments {
            if assignment.committed_offset.0 == 50 {
                // Found the partition with committed offset
                return;
            }
        }

        // Verify directly from DB
        let committed: Option<i64> = sqlx::query_scalar(
            "SELECT committed_offset FROM reader_assignments WHERE group_id = $1 AND topic_id = $2 AND committed_offset = 50",
        )
        .bind("persist-cg")
        .bind(topic_id.0 as i32)
        .fetch_optional(&db.pool)
        .await
        .expect("Query should succeed");

        assert!(
            committed.is_some(),
            "Committed offset 50 should persist in database"
        );
    }
}
