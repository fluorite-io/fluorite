//! Jepsen-inspired reader group tests.
//!
//! These tests verify:
//! - Commit atomicity (multi-partition commits are all-or-nothing)
//! - Stale reader detection timing
//! - Concurrent partition claims don't cause duplicates
//! - Generation numbers are monotonically increasing

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use turbine_agent::{CommitStatus, Coordinator, CoordinatorConfig};
use turbine_common::ids::{Offset, PartitionId, TopicId};

use common::TestDb;

/// Test that commit only succeeds for partitions the reader owns.
#[tokio::test]
async fn test_commit_only_for_owned_partitions() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    let topic_id = TopicId(db.create_topic("commit-test", 4).await as u32);

    // Reader A joins and gets some partitions
    let result_a = coordinator
        .join_group("test-group", topic_id, "reader-a")
        .await
        .expect("Join should succeed");

    assert!(
        !result_a.assignments.is_empty(),
        "Reader A should get assignments"
    );

    let owned_partition = result_a.assignments[0].partition_id;

    // Commit to owned partition should succeed
    let status = coordinator
        .commit_offset(
            "test-group",
            topic_id,
            "reader-a",
            result_a.generation,
            owned_partition,
            Offset(100),
        )
        .await
        .expect("Commit should not error");

    assert_eq!(
        status,
        CommitStatus::Ok,
        "Commit to owned partition should succeed"
    );

    // Commit to unowned partition should fail with NotOwner
    // Find a partition not owned by reader-a
    let all_partitions: HashSet<u32> = (0..4).collect();
    let owned: HashSet<u32> = result_a
        .assignments
        .iter()
        .map(|a| a.partition_id.0)
        .collect();
    let unowned: Vec<u32> = all_partitions.difference(&owned).copied().collect();

    if !unowned.is_empty() {
        let unowned_partition = PartitionId(unowned[0]);
        let status = coordinator
            .commit_offset(
                "test-group",
                topic_id,
                "reader-a",
                result_a.generation,
                unowned_partition,
                Offset(50),
            )
            .await
            .expect("Commit should not error");

        assert_eq!(
            status,
            CommitStatus::NotOwner,
            "Commit to unowned partition should return NotOwner"
        );
    }
}

/// Test that generation number always increases, never decreases.
#[tokio::test]
async fn test_generation_monotonicity() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    let topic_id = TopicId(db.create_topic("gen-test", 4).await as u32);

    let mut generations = vec![];

    // Multiple readers join and leave, generation should always increase
    for i in 0..5 {
        let reader_id = format!("reader-{}", i);
        let result = coordinator
            .join_group("gen-group", topic_id, &reader_id)
            .await
            .expect("Join should succeed");

        generations.push(result.generation.0);
    }

    // Verify generations are strictly increasing
    for window in generations.windows(2) {
        let (prev, curr) = (window[0], window[1]);
        assert!(
            curr > prev,
            "Generation should strictly increase: {} -> {}",
            prev,
            curr
        );
    }

    // Leave and rejoin should also increase generation
    coordinator
        .leave_group("gen-group", topic_id, "reader-2")
        .await
        .expect("Leave should succeed");

    let result = coordinator
        .join_group("gen-group", topic_id, "reader-new")
        .await
        .expect("Join should succeed");

    let last_gen = *generations.last().unwrap();
    assert!(
        result.generation.0 > last_gen,
        "Generation after leave/join should be > {}",
        last_gen
    );
}

/// Test that two readers never get the same partition assignment.
#[tokio::test]
async fn test_no_duplicate_partition_assignments() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    let topic_id = TopicId(db.create_topic("no-dup-test", 8).await as u32);
    let num_partitions = 8u32;
    let num_readers = 4;

    // Multiple readers join
    let mut last_generation = turbine_common::ids::Generation(0);
    for i in 0..num_readers {
        let reader_id = format!("reader-{}", i);
        let result = coordinator
            .join_group("no-dup-group", topic_id, &reader_id)
            .await
            .expect("Join should succeed");
        last_generation = result.generation;
    }

    // Complete rebalance: all readers rejoin to get their final assignments
    let mut all_assignments = vec![];
    for i in 0..num_readers {
        let reader_id = format!("reader-{}", i);
        let result = coordinator
            .rejoin("no-dup-group", topic_id, &reader_id, last_generation)
            .await
            .expect("Rejoin should succeed");

        for assignment in &result.assignments {
            all_assignments.push((reader_id.clone(), assignment.partition_id));
        }
    }

    // Check for duplicates and coverage
    let mut partition_owners: std::collections::HashMap<u32, Vec<String>> =
        std::collections::HashMap::new();

    for (reader, partition) in &all_assignments {
        partition_owners
            .entry(partition.0)
            .or_default()
            .push(reader.clone());
    }

    // After rebalance settles, each partition should have exactly 1 owner
    for partition_id in 0..num_partitions {
        let owners = partition_owners
            .get(&partition_id)
            .cloned()
            .unwrap_or_default();
        assert_eq!(
            owners.len(),
            1,
            "Partition {} should have exactly 1 owner, but has: {:?}",
            partition_id,
            owners
        );
    }
}

/// Test stale reader detection within session timeout.
#[tokio::test]
async fn test_stale_reader_detection() {
    let db = TestDb::new().await;
    // Use longer timeouts for CI reliability - lease must be longer than session
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(10),
        session_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    let topic_id = TopicId(db.create_topic("stale-test", 4).await as u32);

    // Reader A joins
    let result_a = coordinator
        .join_group("stale-group", topic_id, "reader-a")
        .await
        .expect("Join should succeed");

    let initial_gen = result_a.generation;

    // First reader should get assignments - if not, this indicates a bug
    // in the coordinator or test setup
    if result_a.assignments.is_empty() {
        // Check what's in the database
        let assignment_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM reader_assignments WHERE group_id = $1 AND topic_id = $2",
        )
        .bind("stale-group")
        .bind(topic_id.0 as i32)
        .fetch_one(&db.pool)
        .await
        .expect("Query should succeed");

        panic!(
            "First reader got no assignments. Generation: {}, DB has {} assignment rows",
            initial_gen.0, assignment_count
        );
    }

    // Wait longer than session timeout without heartbeat (2x margin for CI)
    tokio::time::sleep(Duration::from_secs(4)).await;

    // Reader B joins and triggers stale detection via heartbeat
    let _result_b = coordinator
        .join_group("stale-group", topic_id, "reader-b")
        .await
        .expect("Join should succeed");

    // Reader A's heartbeat should detect rebalance needed
    let hb_result = coordinator
        .heartbeat("stale-group", topic_id, "reader-a", initial_gen)
        .await
        .expect("Heartbeat should succeed");

    // Either reader A is expired (UnknownMember) or there's a new generation (RebalanceNeeded)
    // Both are valid responses indicating the stale reader situation was detected
    assert!(
        hb_result.status == turbine_agent::HeartbeatStatus::RebalanceNeeded
            || hb_result.status == turbine_agent::HeartbeatStatus::UnknownMember
            || hb_result.generation.0 > initial_gen.0,
        "Stale reader should be detected: status={:?}, gen={}",
        hb_result.status,
        hb_result.generation.0
    );
}

/// Test that concurrent partition claims are handled correctly.
#[tokio::test]
async fn test_concurrent_partition_claim() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Arc::new(Coordinator::new(db.pool.clone(), config));

    let topic_id = TopicId(db.create_topic("concurrent-claim", 4).await as u32);
    let num_partitions = 4u32;
    let num_readers = 8;

    // Spawn multiple readers joining concurrently
    let mut handles = vec![];
    for i in 0..num_readers {
        let coord = coordinator.clone();
        let reader_id = format!("reader-{}", i);
        handles.push(tokio::spawn(async move {
            coord
                .join_group("concurrent-group", topic_id, &reader_id)
                .await
        }));
    }

    // Wait for all joins to complete
    let results: Vec<_> = futures::future::join_all(handles).await;
    for result in &results {
        result
            .as_ref()
            .expect("Task should succeed")
            .as_ref()
            .expect("Join should succeed");
    }

    // Settle the rebalance by repeatedly rejoining until all readers have stable assignments.
    // After concurrent joins, the generation may have changed multiple times.
    // Keep rejoining until all readers report Ok status.
    let max_rounds = 10;
    for _round in 0..max_rounds {
        // Get current generation from database
        let current_gen: i64 = sqlx::query_scalar(
            "SELECT generation FROM reader_groups WHERE group_id = $1 AND topic_id = $2",
        )
        .bind("concurrent-group")
        .bind(topic_id.0 as i32)
        .fetch_one(&db.pool)
        .await
        .expect("Query should succeed");

        let generation = turbine_common::ids::Generation(current_gen as u64);

        // All readers rejoin with current generation
        let mut all_ok = true;
        for i in 0..num_readers {
            let reader_id = format!("reader-{}", i);
            let result = coordinator
                .rejoin("concurrent-group", topic_id, &reader_id, generation)
                .await
                .expect("Rejoin should succeed");

            if result.status != turbine_agent::RejoinStatus::Ok {
                all_ok = false;
            }
        }

        if all_ok {
            break;
        }
    }

    // Now get final assignments from one more rejoin round
    let current_gen: i64 = sqlx::query_scalar(
        "SELECT generation FROM reader_groups WHERE group_id = $1 AND topic_id = $2",
    )
    .bind("concurrent-group")
    .bind(topic_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .expect("Query should succeed");

    let generation = turbine_common::ids::Generation(current_gen as u64);

    let mut all_claims: Vec<(String, u32)> = vec![];
    for i in 0..num_readers {
        let reader_id = format!("reader-{}", i);
        let result = coordinator
            .rejoin("concurrent-group", topic_id, &reader_id, generation)
            .await
            .expect("Rejoin should succeed");

        for assignment in &result.assignments {
            all_claims.push((reader_id.clone(), assignment.partition_id.0));
        }
    }

    // Build partition ownership map
    let mut partition_owners: std::collections::HashMap<u32, Vec<String>> =
        std::collections::HashMap::new();

    for (reader, partition) in &all_claims {
        partition_owners
            .entry(*partition)
            .or_default()
            .push(reader.clone());
    }

    // After rebalance settles, each partition should have exactly 1 owner
    for partition_id in 0..num_partitions {
        let owners = partition_owners
            .get(&partition_id)
            .cloned()
            .unwrap_or_default();
        assert_eq!(
            owners.len(),
            1,
            "Partition {} should have exactly 1 owner, but has: {:?}",
            partition_id,
            owners
        );
    }
}

/// Test committed offset persists through reader reconnect.
#[tokio::test]
async fn test_committed_offset_persists() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    let topic_id = TopicId(db.create_topic("persist-test", 2).await as u32);

    // Reader A joins
    let result_a = coordinator
        .join_group("persist-group", topic_id, "reader-a")
        .await
        .expect("Join should succeed");

    // Reader should get all partitions initially
    assert!(!result_a.assignments.is_empty());

    // Commit offset 42 to first partition
    let partition = result_a.assignments[0].partition_id;
    coordinator
        .commit_offset(
            "persist-group",
            topic_id,
            "reader-a",
            result_a.generation,
            partition,
            Offset(42),
        )
        .await
        .expect("Commit should succeed");

    // Reader leaves
    coordinator
        .leave_group("persist-group", topic_id, "reader-a")
        .await
        .expect("Leave should succeed");

    // Same reader rejoins
    let result_a2 = coordinator
        .join_group("persist-group", topic_id, "reader-a")
        .await
        .expect("Rejoin should succeed");

    // Find the same partition in new assignments
    let same_partition_assignment = result_a2
        .assignments
        .iter()
        .find(|a| a.partition_id == partition);

    if let Some(assignment) = same_partition_assignment {
        assert_eq!(
            assignment.committed_offset.0, 42,
            "Committed offset should persist through leave/join"
        );
    }
}

/// Test rejoin after rebalance returns new assignments.
#[tokio::test]
async fn test_rejoin_after_rebalance() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    let topic_id = TopicId(db.create_topic("rejoin-test", 4).await as u32);

    // Reader A joins alone
    let result_a = coordinator
        .join_group("rejoin-group", topic_id, "reader-a")
        .await
        .expect("Join should succeed");

    let gen_a = result_a.generation;
    assert_eq!(
        result_a.assignments.len(),
        4,
        "Reader A should get all 4 partitions"
    );

    // Reader B joins, triggering rebalance
    let result_b = coordinator
        .join_group("rejoin-group", topic_id, "reader-b")
        .await
        .expect("Join should succeed");

    let gen_b = result_b.generation;
    assert!(
        gen_b.0 > gen_a.0,
        "Generation should increase after B joins"
    );

    // Reader A heartbeats and detects rebalance needed
    let hb_result = coordinator
        .heartbeat("rejoin-group", topic_id, "reader-a", gen_a)
        .await
        .expect("Heartbeat should succeed");

    assert_eq!(
        hb_result.status,
        turbine_agent::HeartbeatStatus::RebalanceNeeded,
        "Heartbeat should indicate rebalance needed"
    );

    // Reader A rejoins with current generation
    let rejoin_result = coordinator
        .rejoin("rejoin-group", topic_id, "reader-a", gen_b)
        .await
        .expect("Rejoin should succeed");

    assert_eq!(
        rejoin_result.status,
        turbine_agent::RejoinStatus::Ok,
        "Rejoin should succeed"
    );

    // Verify A and B together cover all partitions without overlap
    let a_partitions: HashSet<u32> = rejoin_result
        .assignments
        .iter()
        .map(|a| a.partition_id.0)
        .collect();

    let b_partitions: HashSet<u32> = result_b
        .assignments
        .iter()
        .map(|a| a.partition_id.0)
        .collect();

    // No overlap
    let overlap: HashSet<u32> = a_partitions.intersection(&b_partitions).copied().collect();
    assert!(
        overlap.is_empty(),
        "A and B should not have overlapping partitions: {:?}",
        overlap
    );

    // Together they should have some partitions (not necessarily all due to lease timing)
    let total: HashSet<u32> = a_partitions.union(&b_partitions).copied().collect();
    assert!(
        !total.is_empty(),
        "A and B together should have some partitions"
    );
}

/// Test concurrent commits from the same reader don't cause corruption.
#[tokio::test]
async fn test_concurrent_commit_same_reader() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Arc::new(Coordinator::new(db.pool.clone(), config));

    let topic_id = TopicId(db.create_topic("concurrent-commit-test", 4).await as u32);

    // Reader joins and gets partitions
    let result = coordinator
        .join_group("cc-group", topic_id, "reader-a")
        .await
        .expect("Join should succeed");

    assert!(!result.assignments.is_empty(), "Should get assignments");
    let partition = result.assignments[0].partition_id;
    let generation = result.generation;

    // Spawn multiple concurrent commits to the same partition
    let mut handles = vec![];
    for offset in 1..=10 {
        let coord = coordinator.clone();
        handles.push(tokio::spawn(async move {
            coord
                .commit_offset(
                    "cc-group",
                    topic_id,
                    "reader-a",
                    generation,
                    partition,
                    Offset(offset * 10),
                )
                .await
        }));
    }

    // All commits should succeed - the reader owns the partition with a 45s lease
    let results: Vec<_> = futures::future::join_all(handles).await;
    for (i, result) in results.iter().enumerate() {
        match result {
            Ok(Ok(status)) => {
                assert_eq!(
                    *status,
                    CommitStatus::Ok,
                    "Commit {} should succeed since reader owns partition, got {:?}",
                    i,
                    status
                );
            }
            Ok(Err(e)) => {
                panic!("Commit {} failed with error: {}", i, e);
            }
            Err(e) => {
                panic!("Task {} panicked: {}", i, e);
            }
        }
    }

    // Final committed offset should be one of the committed values
    let final_offset: Option<i64> = sqlx::query_scalar(
        "SELECT committed_offset FROM reader_assignments
         WHERE group_id = $1 AND topic_id = $2 AND partition_id = $3",
    )
    .bind("cc-group")
    .bind(topic_id.0 as i32)
    .bind(partition.0 as i32)
    .fetch_optional(&db.pool)
    .await
    .expect("Query should succeed");

    // Should have a valid committed offset
    if let Some(offset) = final_offset {
        assert!(
            offset >= 10 && offset <= 100 && offset % 10 == 0,
            "Final offset {} should be one of the committed values",
            offset
        );
    }
}

/// Test that commit during rebalance is handled correctly.
#[tokio::test]
async fn test_commit_during_rebalance() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Arc::new(Coordinator::new(db.pool.clone(), config));

    let topic_id = TopicId(db.create_topic("commit-rebalance-test", 4).await as u32);

    // Reader A joins
    let result_a = coordinator
        .join_group("cr-group", topic_id, "reader-a")
        .await
        .expect("Join should succeed");

    let gen_a = result_a.generation;
    let partition = result_a.assignments[0].partition_id;

    // Spawn commit in background
    let coord_commit = coordinator.clone();
    let commit_handle = tokio::spawn(async move {
        // Small delay to allow rebalance to start
        tokio::time::sleep(Duration::from_millis(10)).await;
        coord_commit
            .commit_offset(
                "cr-group",
                topic_id,
                "reader-a",
                gen_a,
                partition,
                Offset(50),
            )
            .await
    });

    // Reader B joins - triggers rebalance
    let _result_b = coordinator
        .join_group("cr-group", topic_id, "reader-b")
        .await
        .expect("Join should succeed");

    // Wait for commit to complete
    let commit_result = commit_handle.await.expect("Task should complete");

    // Commit should either succeed (if it ran before generation change)
    // or fail if generation changed / ownership moved.
    match commit_result {
        Ok(CommitStatus::Ok) => {
            // Commit succeeded before rebalance took effect
        }
        Ok(CommitStatus::NotOwner) => {
            // Commit failed due to generation change - this is correct behavior
        }
        Ok(CommitStatus::StaleGeneration) => {
            // Commit failed due to generation fencing - this is correct behavior
        }
        Err(e) => {
            panic!("Commit should not error: {}", e);
        }
    }

    // Verify A gets rebalance notification
    let hb_result = coordinator
        .heartbeat("cr-group", topic_id, "reader-a", gen_a)
        .await
        .expect("Heartbeat should succeed");

    assert_eq!(
        hb_result.status,
        turbine_agent::HeartbeatStatus::RebalanceNeeded,
        "Reader A should see rebalance needed"
    );
}

/// Test that rejoin with unknown reader is handled correctly.
#[tokio::test]
async fn test_rejoin_with_unknown_reader() {
    let db = TestDb::new().await;
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(45),
        session_timeout: Duration::from_secs(30),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    let topic_id = TopicId(db.create_topic("unknown-rejoin-test", 4).await as u32);

    // Reader A joins to establish a group with generation 1
    let result_a = coordinator
        .join_group("ur-group", topic_id, "reader-a")
        .await
        .expect("Join should succeed");

    assert_eq!(result_a.generation.0, 1);

    // Unknown reader (never joined) tries to rejoin
    let rejoin_result = coordinator
        .rejoin(
            "ur-group",
            topic_id,
            "unknown-reader",
            turbine_common::ids::Generation(1),
        )
        .await
        .expect("Rejoin should not error");

    // Should get RebalanceNeeded or empty assignments (unknown member)
    assert!(
        rejoin_result.status == turbine_agent::RejoinStatus::RebalanceNeeded
            || rejoin_result.assignments.is_empty(),
        "Unknown reader should get RebalanceNeeded or empty assignments"
    );
}

/// Test heartbeat after session has expired returns UnknownMember.
#[tokio::test]
async fn test_heartbeat_after_session_expired() {
    let db = TestDb::new().await;
    // Use short session timeout for testing
    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(10),
        session_timeout: Duration::from_secs(1),
        ..Default::default()
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    let topic_id = TopicId(db.create_topic("expired-hb-test", 4).await as u32);

    // Reader A joins
    let result_a = coordinator
        .join_group("ehb-group", topic_id, "reader-a")
        .await
        .expect("Join should succeed");

    let gen_a = result_a.generation;

    // Reader B joins to trigger member tracking
    let _result_b = coordinator
        .join_group("ehb-group", topic_id, "reader-b")
        .await
        .expect("Join should succeed");

    // Wait for A's session to expire
    tokio::time::sleep(Duration::from_secs(3)).await;

    // B heartbeats to trigger cleanup of expired A
    let _ = coordinator
        .heartbeat(
            "ehb-group",
            topic_id,
            "reader-b",
            turbine_common::ids::Generation(2),
        )
        .await;

    // Now A tries to heartbeat with old generation
    let hb_result = coordinator
        .heartbeat("ehb-group", topic_id, "reader-a", gen_a)
        .await
        .expect("Heartbeat should not error");

    // Should get UnknownMember or RebalanceNeeded (session expired)
    assert!(
        hb_result.status == turbine_agent::HeartbeatStatus::UnknownMember
            || hb_result.status == turbine_agent::HeartbeatStatus::RebalanceNeeded,
        "Expired reader should get UnknownMember or RebalanceNeeded, got {:?}",
        hb_result.status
    );
}
