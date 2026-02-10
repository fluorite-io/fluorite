//! Integration tests for the consumer group coordinator.
//!
//! These tests require a running PostgreSQL instance.
//!
//! ```bash
//! DATABASE_URL=postgres://postgres:turbine@localhost:5433 cargo test --test coordinator_integration
//! ```

mod common;

use std::time::Duration;
use turbine_agent::{
    CommitStatus, Coordinator, CoordinatorConfig, HeartbeatStatus, RejoinStatus,
};
use turbine_common::ids::{Generation, Offset, PartitionId, TopicId};

use common::TestDb;

/// Skip test if DATABASE_URL is not set.
fn skip_if_no_db() -> bool {
    std::env::var("DATABASE_URL").is_err()
}

/// Test single consumer joins and gets all partitions.
#[tokio::test]
async fn test_single_consumer_join() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-test-1", 4).await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());

    let result = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "consumer-a")
        .await
        .expect("join_group failed");

    assert_eq!(result.generation.0, 1);
    assert_eq!(result.assignments.len(), 4); // Single consumer gets all partitions

    let mut partitions: Vec<u32> = result
        .assignments
        .iter()
        .map(|a| a.partition_id.0)
        .collect();
    partitions.sort();
    assert_eq!(partitions, vec![0, 1, 2, 3]);
}

/// Test two consumers join and partitions are split.
#[tokio::test]
async fn test_two_consumers_join() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-test-2", 4).await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());

    // Consumer A joins first
    let result_a = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "consumer-a")
        .await
        .expect("join_group failed");

    assert_eq!(result_a.generation.0, 1);
    assert_eq!(result_a.assignments.len(), 4); // Gets all initially

    // Consumer B joins - triggers rebalance
    let result_b = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "consumer-b")
        .await
        .expect("join_group failed");

    assert_eq!(result_b.generation.0, 2);
    // B initially gets 0 partitions (A still holds them)
    // B will get partitions after A rejoins

    // A sends heartbeat - should get REBALANCE_NEEDED
    let hb_result = coordinator
        .heartbeat(
            "test-group",
            TopicId(topic_id as u32),
            "consumer-a",
            Generation(1),
        )
        .await
        .expect("heartbeat failed");

    assert_eq!(hb_result.status, HeartbeatStatus::RebalanceNeeded);
    assert_eq!(hb_result.generation.0, 2);
}

/// Test rejoin after rebalance notification.
#[tokio::test]
async fn test_rejoin_after_rebalance() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-test-3", 10).await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());

    // Consumer A joins and gets all 10 partitions
    let _ = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "consumer-a")
        .await
        .expect("join_group failed");

    // Consumer B joins
    let _ = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "consumer-b")
        .await
        .expect("join_group failed");

    // A rejoins at generation 2 - should release [5-9], keep [0-4]
    let rejoin_a = coordinator
        .rejoin(
            "test-group",
            TopicId(topic_id as u32),
            "consumer-a",
            Generation(2),
        )
        .await
        .expect("rejoin failed");

    assert_eq!(rejoin_a.status, RejoinStatus::Ok);
    assert_eq!(rejoin_a.assignments.len(), 5);

    let mut a_partitions: Vec<u32> = rejoin_a
        .assignments
        .iter()
        .map(|a| a.partition_id.0)
        .collect();
    a_partitions.sort();
    assert_eq!(a_partitions, vec![0, 1, 2, 3, 4]);

    // B rejoins - should now claim [5-9]
    let rejoin_b = coordinator
        .rejoin(
            "test-group",
            TopicId(topic_id as u32),
            "consumer-b",
            Generation(2),
        )
        .await
        .expect("rejoin failed");

    assert_eq!(rejoin_b.status, RejoinStatus::Ok);
    assert_eq!(rejoin_b.assignments.len(), 5);

    let mut b_partitions: Vec<u32> = rejoin_b
        .assignments
        .iter()
        .map(|a| a.partition_id.0)
        .collect();
    b_partitions.sort();
    assert_eq!(b_partitions, vec![5, 6, 7, 8, 9]);
}

/// Test leave group releases partitions.
#[tokio::test]
async fn test_leave_group() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-test-4", 4).await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());

    // Consumer A joins
    let _ = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "consumer-a")
        .await
        .expect("join_group failed");

    // Consumer B joins
    let _ = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "consumer-b")
        .await
        .expect("join_group failed");

    // A rejoins to settle assignment
    let _ = coordinator
        .rejoin(
            "test-group",
            TopicId(topic_id as u32),
            "consumer-a",
            Generation(2),
        )
        .await
        .expect("rejoin failed");

    // B rejoins
    let _ = coordinator
        .rejoin(
            "test-group",
            TopicId(topic_id as u32),
            "consumer-b",
            Generation(2),
        )
        .await
        .expect("rejoin failed");

    // A leaves
    coordinator
        .leave_group("test-group", TopicId(topic_id as u32), "consumer-a")
        .await
        .expect("leave_group failed");

    // B's heartbeat should see rebalance needed (generation bumped)
    let hb_result = coordinator
        .heartbeat(
            "test-group",
            TopicId(topic_id as u32),
            "consumer-b",
            Generation(2),
        )
        .await
        .expect("heartbeat failed");

    assert_eq!(hb_result.status, HeartbeatStatus::RebalanceNeeded);
    assert_eq!(hb_result.generation.0, 3);

    // B rejoins and should get all 4 partitions
    let rejoin_b = coordinator
        .rejoin(
            "test-group",
            TopicId(topic_id as u32),
            "consumer-b",
            Generation(3),
        )
        .await
        .expect("rejoin failed");

    assert_eq!(rejoin_b.assignments.len(), 4);
}

/// Test commit offset succeeds when consumer owns partition.
#[tokio::test]
async fn test_commit_offset_success() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-test-5", 4).await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());

    // Consumer joins and gets all partitions
    let result = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "consumer-a")
        .await
        .expect("join_group failed");

    assert_eq!(result.assignments.len(), 4);

    // Commit offset for partition 0
    let status = coordinator
        .commit_offset(
            "test-group",
            TopicId(topic_id as u32),
            "consumer-a",
            PartitionId(0),
            Offset(100),
        )
        .await
        .expect("commit_offset failed");

    assert_eq!(status, CommitStatus::Ok);

    // Verify offset was committed by rejoining
    let rejoin = coordinator
        .rejoin(
            "test-group",
            TopicId(topic_id as u32),
            "consumer-a",
            Generation(1),
        )
        .await
        .expect("rejoin failed");

    let partition_0 = rejoin
        .assignments
        .iter()
        .find(|a| a.partition_id.0 == 0)
        .expect("partition 0 not found");
    assert_eq!(partition_0.committed_offset.0, 100);
}

/// Test commit offset fails when consumer doesn't own partition.
#[tokio::test]
async fn test_commit_offset_not_owner() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-test-6", 4).await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());

    // Consumer A joins
    let _ = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "consumer-a")
        .await
        .expect("join_group failed");

    // Consumer B tries to commit (never joined)
    let status = coordinator
        .commit_offset(
            "test-group",
            TopicId(topic_id as u32),
            "consumer-b",
            PartitionId(0),
            Offset(100),
        )
        .await
        .expect("commit_offset failed");

    assert_eq!(status, CommitStatus::NotOwner);
}

/// Test heartbeat for unknown member.
#[tokio::test]
async fn test_heartbeat_unknown_member() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-test-7", 4).await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());

    // Consumer A joins
    let _ = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "consumer-a")
        .await
        .expect("join_group failed");

    // Unknown consumer sends heartbeat
    let result = coordinator
        .heartbeat(
            "test-group",
            TopicId(topic_id as u32),
            "unknown-consumer",
            Generation(1),
        )
        .await
        .expect("heartbeat failed");

    assert_eq!(result.status, HeartbeatStatus::UnknownMember);
}

/// Test rejoin with stale generation.
#[tokio::test]
async fn test_rejoin_stale_generation() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-test-8", 4).await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());

    // Consumer A joins
    let _ = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "consumer-a")
        .await
        .expect("join_group failed");

    // Consumer B joins (bumps to gen 2)
    let _ = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "consumer-b")
        .await
        .expect("join_group failed");

    // Consumer C joins (bumps to gen 3)
    let _ = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "consumer-c")
        .await
        .expect("join_group failed");

    // A tries to rejoin with gen 2 (stale)
    let result = coordinator
        .rejoin(
            "test-group",
            TopicId(topic_id as u32),
            "consumer-a",
            Generation(2),
        )
        .await
        .expect("rejoin failed");

    assert_eq!(result.status, RejoinStatus::RebalanceNeeded);
    assert_eq!(result.generation.0, 3);
    assert!(result.assignments.is_empty());
}

/// Test heartbeat detects and removes expired members.
#[tokio::test]
async fn test_heartbeat_removes_expired_members() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-test-9", 4).await;

    // Use very short session timeout for testing
    let config = CoordinatorConfig {
        session_timeout: Duration::from_millis(100),
        lease_duration: Duration::from_secs(45),
    };
    let coordinator = Coordinator::new(db.pool.clone(), config);

    // Consumer A joins
    let _ = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "consumer-a")
        .await
        .expect("join_group failed");

    // Consumer B joins
    let _ = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "consumer-b")
        .await
        .expect("join_group failed");

    // Wait for A's session to expire
    tokio::time::sleep(Duration::from_millis(200)).await;

    // B heartbeats - should detect A expired and get rebalance notification
    let result = coordinator
        .heartbeat(
            "test-group",
            TopicId(topic_id as u32),
            "consumer-b",
            Generation(2),
        )
        .await
        .expect("heartbeat failed");

    // B's heartbeat should detect A expired and bump generation
    assert_eq!(result.status, HeartbeatStatus::RebalanceNeeded);
    assert_eq!(result.generation.0, 3);

    // B rejoins and should get all partitions
    let rejoin = coordinator
        .rejoin(
            "test-group",
            TopicId(topic_id as u32),
            "consumer-b",
            Generation(3),
        )
        .await
        .expect("rejoin failed");

    assert_eq!(rejoin.status, RejoinStatus::Ok);
    assert_eq!(rejoin.assignments.len(), 4);
}

/// Test three consumers with uneven partition distribution.
#[tokio::test]
async fn test_three_consumers_uneven_partitions() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-test-10", 10).await;

    let coordinator = Coordinator::new(db.pool.clone(), CoordinatorConfig::default());

    // All three consumers join
    let _ = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "a")
        .await
        .expect("join failed");

    let _ = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "b")
        .await
        .expect("join failed");

    let _ = coordinator
        .join_group("test-group", TopicId(topic_id as u32), "c")
        .await
        .expect("join failed");

    // All rejoin at generation 3
    let ra = coordinator
        .rejoin("test-group", TopicId(topic_id as u32), "a", Generation(3))
        .await
        .expect("rejoin failed");

    let rb = coordinator
        .rejoin("test-group", TopicId(topic_id as u32), "b", Generation(3))
        .await
        .expect("rejoin failed");

    let rc = coordinator
        .rejoin("test-group", TopicId(topic_id as u32), "c", Generation(3))
        .await
        .expect("rejoin failed");

    // Collect all assigned partitions
    let mut all_partitions: Vec<u32> = Vec::new();
    all_partitions.extend(ra.assignments.iter().map(|a| a.partition_id.0));
    all_partitions.extend(rb.assignments.iter().map(|a| a.partition_id.0));
    all_partitions.extend(rc.assignments.iter().map(|a| a.partition_id.0));

    all_partitions.sort();

    // All 10 partitions should be covered exactly once
    assert_eq!(all_partitions, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);

    // Verify balance: 4 + 3 + 3 = 10
    let counts = [
        ra.assignments.len(),
        rb.assignments.len(),
        rc.assignments.len(),
    ];
    assert!(counts.iter().all(|&c| c >= 3 && c <= 4));
}
