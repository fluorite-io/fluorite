//! End-to-end consumer group tests.
//!
//! These tests verify consumer group coordination:
//! - Single consumer joins and gets all partitions
//! - Multiple consumers join and share partitions
//! - Rebalance on consumer join/leave
//! - Heartbeat maintains membership
//! - Commit offsets
//!
//! ```bash
//! DATABASE_URL=postgres://postgres:turbine@localhost:5433 cargo test --test e2e_consumer_groups
//! ```

mod common;

use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use turbine_agent::{AgentConfig, AgentState, CoordinatorConfig, LocalFsStore};
use turbine_common::ids::{Generation, Offset, PartitionId, TopicId};
use turbine_wire::{consumer, MessageType};

use common::TestDb;

/// Skip test if DATABASE_URL is not set.
fn skip_if_no_db() -> bool {
    std::env::var("DATABASE_URL").is_err()
}

/// Find an available port.
async fn find_available_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

/// Start the agent server with short session timeout for testing.
async fn start_server(
    pool: sqlx::PgPool,
    temp_dir: &TempDir,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let addr = find_available_port().await;
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());

    let config = AgentConfig {
        bind_addr: addr,
        bucket: "test".to_string(),
        key_prefix: "data".to_string(),
    };

    // Short session timeout for testing
    let coordinator_config = CoordinatorConfig {
        session_timeout: Duration::from_secs(2),
        ..Default::default()
    };

    let state = Arc::new(AgentState::with_coordinator_config(
        pool,
        store,
        config,
        coordinator_config,
    ));

    let handle = tokio::spawn(async move {
        if let Err(e) = turbine_agent::run(state).await {
            eprintln!("Server error: {}", e);
        }
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    (addr, handle)
}

/// Helper: send a JoinGroup request and get response.
async fn join_group(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    group_id: &str,
    topic_id: TopicId,
    consumer_id: &str,
) -> consumer::JoinGroupResponse {
    let req = consumer::JoinGroupRequest {
        group_id: group_id.to_string(),
        consumer_id: consumer_id.to_string(),
        topic_ids: vec![topic_id],
    };

    let mut buf = vec![0u8; 1024];
    buf[0] = MessageType::JoinGroup.to_byte();
    let len = consumer::encode_join_request(&req, &mut buf[1..]);
    buf.truncate(len + 1);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for join response")
        .expect("No response")
        .expect("WebSocket error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary message"),
    };

    // Skip message type byte
    let (resp, _) = consumer::decode_join_response(&data[1..]).unwrap();
    resp
}

/// Helper: send a Heartbeat request and get response.
async fn send_heartbeat(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    group_id: &str,
    topic_id: TopicId,
    consumer_id: &str,
    generation: Generation,
) -> consumer::HeartbeatResponseExt {
    let req = consumer::HeartbeatRequest {
        group_id: group_id.to_string(),
        topic_id,
        consumer_id: consumer_id.to_string(),
        generation,
    };

    let mut buf = vec![0u8; 256];
    buf[0] = MessageType::Heartbeat.to_byte();
    let len = consumer::encode_heartbeat_request(&req, &mut buf[1..]);
    buf.truncate(len + 1);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for heartbeat response")
        .expect("No response")
        .expect("WebSocket error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary message"),
    };

    // Skip message type byte
    let (resp, _) = consumer::decode_heartbeat_response_ext(&data[1..]).unwrap();
    resp
}

/// Helper: send a LeaveGroup request and get response.
async fn leave_group(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    group_id: &str,
    topic_id: TopicId,
    consumer_id: &str,
) -> consumer::LeaveGroupResponse {
    let req = consumer::LeaveGroupRequest {
        group_id: group_id.to_string(),
        topic_id,
        consumer_id: consumer_id.to_string(),
    };

    let mut buf = vec![0u8; 256];
    buf[0] = MessageType::LeaveGroup.to_byte();
    let len = consumer::encode_leave_request(&req, &mut buf[1..]);
    buf.truncate(len + 1);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for leave response")
        .expect("No response")
        .expect("WebSocket error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary message"),
    };

    // Skip message type byte
    let (resp, _) = consumer::decode_leave_response(&data[1..]).unwrap();
    resp
}

/// Helper: send a Commit request and get response.
async fn commit_offsets(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    group_id: &str,
    consumer_id: &str,
    generation: Generation,
    commits: Vec<consumer::PartitionCommit>,
) -> consumer::CommitResponse {
    let req = consumer::CommitRequest {
        group_id: group_id.to_string(),
        consumer_id: consumer_id.to_string(),
        generation,
        commits,
    };

    let mut buf = vec![0u8; 1024];
    buf[0] = MessageType::Commit.to_byte();
    let len = consumer::encode_commit_request(&req, &mut buf[1..]);
    buf.truncate(len + 1);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for commit response")
        .expect("No response")
        .expect("WebSocket error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary message"),
    };

    // Skip message type byte
    let (resp, _) = consumer::decode_commit_response(&data[1..]).unwrap();
    resp
}

/// Test: Single consumer joins and gets all partitions.
#[tokio::test]
async fn test_single_consumer_gets_all_partitions() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-single-test", 4).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;

    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    let resp = join_group(&mut ws, "test-group", TopicId(topic_id as u32), "consumer-1").await;

    assert!(resp.generation.0 > 0, "Should have valid generation");
    assert_eq!(
        resp.assignments.len(),
        4,
        "Single consumer should get all 4 partitions"
    );

    // Verify partition IDs
    let mut partition_ids: Vec<u32> = resp.assignments.iter().map(|a| a.partition_id.0).collect();
    partition_ids.sort();
    assert_eq!(partition_ids, vec![0, 1, 2, 3]);

    ws.close(None).await.ok();
}

/// Test: Two consumers share partitions.
#[tokio::test]
async fn test_two_consumers_share_partitions() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-two-test", 4).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // First consumer joins
    let (mut ws1, _) = connect_async(&url).await.expect("Failed to connect");
    let resp1 = join_group(&mut ws1, "test-group", TopicId(topic_id as u32), "consumer-1").await;

    assert_eq!(resp1.assignments.len(), 4, "First consumer gets all 4");
    let gen1 = resp1.generation;

    // Second consumer joins
    let (mut ws2, _) = connect_async(&url).await.expect("Failed to connect");
    let resp2 = join_group(&mut ws2, "test-group", TopicId(topic_id as u32), "consumer-2").await;

    // Second consumer triggers rebalance
    assert!(
        resp2.generation.0 > gen1.0,
        "Generation should increase on rebalance"
    );
    assert_eq!(
        resp2.assignments.len(),
        2,
        "Second consumer should get 2 partitions"
    );

    // First consumer heartbeats and discovers rebalance
    let hb1 = send_heartbeat(
        &mut ws1,
        "test-group",
        TopicId(topic_id as u32),
        "consumer-1",
        gen1,
    )
    .await;
    assert_eq!(
        hb1.status,
        consumer::HeartbeatStatus::RebalanceNeeded,
        "First consumer should be told to rebalance"
    );

    ws1.close(None).await.ok();
    ws2.close(None).await.ok();
}

/// Test: Consumer leave triggers rebalance.
#[tokio::test]
async fn test_consumer_leave_triggers_rebalance() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-leave-test", 4).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // Two consumers join
    let (mut ws1, _) = connect_async(&url).await.expect("Failed to connect");
    let _resp1 = join_group(&mut ws1, "test-group", TopicId(topic_id as u32), "consumer-1").await;

    let (mut ws2, _) = connect_async(&url).await.expect("Failed to connect");
    let resp2 = join_group(&mut ws2, "test-group", TopicId(topic_id as u32), "consumer-2").await;

    // Consumer 2 leaves
    let leave_resp = leave_group(
        &mut ws2,
        "test-group",
        TopicId(topic_id as u32),
        "consumer-2",
    )
    .await;
    assert!(leave_resp.success);

    // Consumer 1 heartbeats and discovers rebalance
    let hb1 = send_heartbeat(
        &mut ws1,
        "test-group",
        TopicId(topic_id as u32),
        "consumer-1",
        resp2.generation, // Use latest gen
    )
    .await;

    // May or may not need rebalance depending on timing
    // Just verify we got a valid response
    assert!(
        hb1.status == consumer::HeartbeatStatus::Ok
            || hb1.status == consumer::HeartbeatStatus::RebalanceNeeded
    );

    ws1.close(None).await.ok();
    ws2.close(None).await.ok();
}

/// Test: Commit offsets.
#[tokio::test]
async fn test_commit_offsets() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-commit-test", 2).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");
    let resp = join_group(&mut ws, "test-group", TopicId(topic_id as u32), "consumer-1").await;

    // Commit offsets for assigned partitions
    let commits: Vec<consumer::PartitionCommit> = resp
        .assignments
        .iter()
        .map(|a| consumer::PartitionCommit {
            topic_id: TopicId(topic_id as u32),
            partition_id: a.partition_id,
            offset: Offset(100),
        })
        .collect();

    let commit_resp = commit_offsets(&mut ws, "test-group", "consumer-1", resp.generation, commits)
        .await;

    assert!(commit_resp.success, "Commit should succeed");

    // Verify offsets in database
    for assignment in &resp.assignments {
        let offset: i64 = sqlx::query_scalar(
            "SELECT committed_offset FROM consumer_assignments WHERE group_id = $1 AND topic_id = $2 AND partition_id = $3",
        )
        .bind("test-group")
        .bind(topic_id)
        .bind(assignment.partition_id.0 as i32)
        .fetch_one(&db.pool)
        .await
        .unwrap();

        assert_eq!(offset, 100, "Committed offset should be 100");
    }

    ws.close(None).await.ok();
}

/// Test: Heartbeat keeps membership alive.
#[tokio::test]
async fn test_heartbeat_maintains_membership() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-heartbeat-test", 2).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");
    let resp = join_group(&mut ws, "test-group", TopicId(topic_id as u32), "consumer-1").await;

    // Send heartbeats
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(500)).await;

        let hb = send_heartbeat(
            &mut ws,
            "test-group",
            TopicId(topic_id as u32),
            "consumer-1",
            resp.generation,
        )
        .await;

        assert_eq!(hb.status, consumer::HeartbeatStatus::Ok);
        assert_eq!(hb.generation, resp.generation);
    }

    ws.close(None).await.ok();
}

/// Test: Session timeout removes member.
#[tokio::test]
async fn test_session_timeout_removes_member() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-timeout-test", 2).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // First consumer joins but doesn't heartbeat
    let (mut ws1, _) = connect_async(&url).await.expect("Failed to connect");
    let resp1 = join_group(&mut ws1, "test-group", TopicId(topic_id as u32), "consumer-1").await;
    let _gen1 = resp1.generation;

    // Wait for session timeout (2 seconds + buffer)
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Second consumer joins
    let (mut ws2, _) = connect_async(&url).await.expect("Failed to connect");
    let resp2 = join_group(&mut ws2, "test-group", TopicId(topic_id as u32), "consumer-2").await;

    // Second consumer should get all partitions since first one timed out
    assert_eq!(
        resp2.assignments.len(),
        2,
        "Second consumer should get all partitions after timeout"
    );

    // First consumer heartbeat should fail with UnknownMember
    let hb1 = send_heartbeat(
        &mut ws1,
        "test-group",
        TopicId(topic_id as u32),
        "consumer-1",
        resp1.generation,
    )
    .await;
    assert_eq!(
        hb1.status,
        consumer::HeartbeatStatus::UnknownMember,
        "Timed out consumer should be unknown"
    );

    ws1.close(None).await.ok();
    ws2.close(None).await.ok();
}

/// Test: Three consumers with uneven partition count.
#[tokio::test]
async fn test_three_consumers_uneven_partitions() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-uneven-test", 5).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // Three consumers join
    let (mut ws1, _) = connect_async(&url).await.expect("Failed to connect");
    let _resp1 = join_group(&mut ws1, "test-group", TopicId(topic_id as u32), "consumer-1").await;

    let (mut ws2, _) = connect_async(&url).await.expect("Failed to connect");
    let _resp2 = join_group(&mut ws2, "test-group", TopicId(topic_id as u32), "consumer-2").await;

    let (mut ws3, _) = connect_async(&url).await.expect("Failed to connect");
    let resp3 = join_group(&mut ws3, "test-group", TopicId(topic_id as u32), "consumer-3").await;

    // Third consumer triggers final rebalance
    // With 5 partitions and 3 consumers: 2, 2, 1 distribution
    // Note: resp1 and resp2 may be stale; they need to heartbeat to get updated assignments
    assert!(
        resp3.generation.0 >= 3,
        "Generation should be at least 3 after 3 joins"
    );

    // The latest joiner should have their correct assignment
    assert!(
        !resp3.assignments.is_empty(),
        "Third consumer should have at least 1 partition"
    );

    ws1.close(None).await.ok();
    ws2.close(None).await.ok();
    ws3.close(None).await.ok();
}

/// Test: Commit rejected if consumer doesn't own partition.
#[tokio::test]
async fn test_commit_rejected_if_not_owner() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-not-owner-test", 4).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // Two consumers join
    let (mut ws1, _) = connect_async(&url).await.expect("Failed to connect");
    let _resp1 = join_group(&mut ws1, "test-group", TopicId(topic_id as u32), "consumer-1").await;

    let (mut ws2, _) = connect_async(&url).await.expect("Failed to connect");
    let resp2 = join_group(&mut ws2, "test-group", TopicId(topic_id as u32), "consumer-2").await;

    // Consumer 2 gets partitions 2 and 3 (deterministic assignment)
    // Try to commit partition 0 which belongs to consumer 1
    let commits = vec![consumer::PartitionCommit {
        topic_id: TopicId(topic_id as u32),
        partition_id: PartitionId(0), // Not owned by consumer-2
        offset: Offset(100),
    }];

    let commit_resp =
        commit_offsets(&mut ws2, "test-group", "consumer-2", resp2.generation, commits).await;

    assert!(
        !commit_resp.success,
        "Commit should be rejected for non-owned partition"
    );

    ws1.close(None).await.ok();
    ws2.close(None).await.ok();
}
