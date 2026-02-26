//! Jepsen-inspired reader churn tests.
//!
//! Verifies that reader churn (repeated leave/join cycles) doesn't lose
//! committed offsets or create assignment gaps.

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use flourine_broker::{Coordinator, CoordinatorConfig};
use flourine_common::ids::{Generation, Offset, TopicId};

use common::TestDb;

/// 1 producer equivalent (pre-populated offsets), 4 readers in a group.
/// In a loop (10 iterations): one reader leaves, a new reader joins,
/// commit offsets, verify assignments. At the end, verify: all committed
/// offsets monotonically increase, no gaps in assignment coverage.
/// Invariant: reader churn doesn't lose committed offsets or create assignment gaps.
#[tokio::test]
async fn test_sustained_reader_churn_offset_continuity() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Arc::new(Coordinator::new(db.pool.clone(), config));

    let num_partitions = 4u32;
    let topic_id = TopicId(db.create_topic("reader-churn", num_partitions as i32).await as u32);

    // Initial 4 readers join
    let mut active_readers: Vec<String> = (0..4).map(|i| format!("reader-{}", i)).collect();
    let mut last_generation = Generation(0);

    for reader_id in &active_readers {
        let result = coordinator
            .join_group("churn-group", topic_id, reader_id)
            .await
            .expect("initial join should succeed");
        last_generation = result.generation;
    }

    // Settle assignments
    for reader_id in &active_readers {
        let _ = coordinator
            .rejoin("churn-group", topic_id, reader_id, last_generation)
            .await;
    }

    // Track committed offsets per partition (should monotonically increase)
    let mut committed_offsets: std::collections::HashMap<u32, Vec<u64>> =
        std::collections::HashMap::new();
    let mut offset_counter = 1u64;
    let mut next_reader_id = 4u32;

    for iteration in 0..10 {
        // Pick a reader to leave (rotating)
        let leave_idx = iteration % active_readers.len();
        let leaving_reader = active_readers[leave_idx].clone();

        // Before leaving, commit offsets for owned partitions
        let current_gen: i64 = sqlx::query_scalar(
            "SELECT generation FROM reader_groups WHERE group_id = $1 AND topic_id = $2",
        )
        .bind("churn-group")
        .bind(topic_id.0 as i32)
        .fetch_one(&db.pool)
        .await
        .expect("query gen");
        let generation = Generation(current_gen as u64);

        // Get current assignments for the leaving reader
        let rejoin_result = coordinator
            .rejoin("churn-group", topic_id, &leaving_reader, generation)
            .await
            .expect("rejoin to get assignments");

        for assignment in &rejoin_result.assignments {
            let status = coordinator
                .commit_offset(
                    "churn-group",
                    topic_id,
                    &leaving_reader,
                    generation,
                    assignment.partition_id,
                    Offset(offset_counter),
                )
                .await
                .expect("commit should not error");

            if status == flourine_broker::CommitStatus::Ok {
                committed_offsets
                    .entry(assignment.partition_id.0)
                    .or_default()
                    .push(offset_counter);
            }
            offset_counter += 1;
        }

        // Leave
        coordinator
            .leave_group("churn-group", topic_id, &leaving_reader)
            .await
            .expect("leave should succeed");

        // New reader joins
        let new_reader = format!("reader-{}", next_reader_id);
        next_reader_id += 1;
        active_readers[leave_idx] = new_reader.clone();

        let result = coordinator
            .join_group("churn-group", topic_id, &new_reader)
            .await
            .expect("new reader join should succeed");
        last_generation = result.generation;

        // Settle: all active readers rejoin
        for reader_id in &active_readers {
            let _ = coordinator
                .rejoin("churn-group", topic_id, reader_id, last_generation)
                .await;
        }
    }

    // Verify: committed offsets monotonically increase per partition
    for (partition_id, offsets) in &committed_offsets {
        for window in offsets.windows(2) {
            assert!(
                window[1] > window[0],
                "Partition {} committed offsets should monotonically increase: {} -> {}",
                partition_id,
                window[0],
                window[1]
            );
        }
    }

    // Verify: final assignments cover all partitions with no overlap
    let current_gen: i64 = sqlx::query_scalar(
        "SELECT generation FROM reader_groups WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("churn-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("query final gen");
    let generation = Generation(current_gen as u64);

    let mut all_assignments: Vec<(String, u32)> = vec![];
    for reader_id in &active_readers {
        let result = coordinator
            .rejoin("churn-group", topic_id, reader_id, generation)
            .await
            .expect("final rejoin");
        for a in &result.assignments {
            all_assignments.push((reader_id.clone(), a.partition_id.0));
        }
    }

    // No duplicate partitions
    let assigned_partitions: Vec<u32> = all_assignments.iter().map(|(_, p)| *p).collect();
    let unique_partitions: HashSet<u32> = assigned_partitions.iter().copied().collect();
    assert_eq!(
        assigned_partitions.len(),
        unique_partitions.len(),
        "No duplicate partition assignments after churn"
    );

    // All partitions covered
    let expected: HashSet<u32> = (0..num_partitions).collect();
    assert_eq!(
        unique_partitions, expected,
        "All partitions should be assigned after churn"
    );

    // Verify committed offsets persist in DB
    for partition_id in 0..num_partitions {
        let db_offset: Option<i64> = sqlx::query_scalar(
            "SELECT committed_offset FROM reader_assignments \
             WHERE group_id = $1 AND topic_id = $2 AND partition_id = $3",
        )
        .bind("churn-group")
        .bind(topic_id.0 as i32)
        .bind(partition_id as i32)
        .fetch_optional(&db.pool)
        .await
        .expect("query committed offset");

        if let Some(offset) = db_offset {
            // The offset should be one of the values we committed
            if let Some(history) = committed_offsets.get(&partition_id) {
                assert!(
                    offset >= 0,
                    "Committed offset for partition {} should be >= 0",
                    partition_id
                );
                // The latest committed offset should be in our history
                if !history.is_empty() {
                    assert!(
                        history.contains(&(offset as u64)),
                        "DB offset {} for partition {} should be in commit history {:?}",
                        offset,
                        partition_id,
                        history
                    );
                }
            }
        }
    }
}
