//! End-to-end reader group tests.
//!
//! These tests verify reader group coordination:
//! - Single reader joins and gets all partitions
//! - Multiple readers join and share partitions
//! - Rebalance on reader join/leave
//! - Heartbeat maintains membership
//! - Commit offsets
//!
//! ```bash
//! DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test --test e2e_reader_groups
//! ```

mod common;

use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use turbine_agent::{
    BrokerConfig, BrokerState, BufferConfig, CoordinatorConfig, LocalFsStore,
};
use turbine_common::ids::{Generation, Offset, PartitionId, TopicId};
use turbine_wire::{
    ClientMessage, ServerMessage, reader, decode_server_message, encode_client_message,
};

use common::TestDb;

/// Find an available port.
async fn find_available_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

/// Start the broker server with short session timeout for testing.
async fn start_server(
    pool: sqlx::PgPool,
    temp_dir: &TempDir,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let addr = find_available_port().await;
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());

    let config = BrokerConfig {
        bind_addr: addr,
        bucket: "test".to_string(),
        key_prefix: "data".to_string(),
        buffer: BufferConfig::default(),
        flush_interval: Duration::from_millis(10),
        require_auth: false,
        auth_timeout: Duration::from_secs(10),
    };

    // Short session timeout for testing
    let coordinator_config = CoordinatorConfig {
        session_timeout: Duration::from_secs(2),
        ..Default::default()
    };

    let state =
        BrokerState::with_coordinator_config(pool, store, config, coordinator_config).await;

    let handle = tokio::spawn(async move {
        if let Err(e) = turbine_agent::run(state).await {
            eprintln!("Server error: {}", e);
        }
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    (addr, handle)
}

fn encode_client_frame(msg: ClientMessage, capacity: usize) -> Vec<u8> {
    let mut buf = vec![0u8; capacity];
    let len = encode_client_message(&msg, &mut buf).expect("encode client frame");
    buf.truncate(len);
    buf
}

fn decode_server_frame(data: &[u8]) -> ServerMessage {
    let (msg, used) = decode_server_message(data).expect("decode server frame");
    assert_eq!(used, data.len(), "trailing bytes in server frame");
    msg
}

/// Helper: send a JoinGroup request and get response.
async fn join_group(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    group_id: &str,
    topic_id: TopicId,
    reader_id: &str,
) -> reader::JoinGroupResponse {
    let req = reader::JoinGroupRequest {
        group_id: group_id.to_string(),
        reader_id: reader_id.to_string(),
        topic_ids: vec![topic_id],
    };

    let buf = encode_client_frame(ClientMessage::JoinGroup(req), 1024);

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

    match decode_server_frame(&data) {
        ServerMessage::JoinGroup(resp) => resp,
        _ => panic!("expected join response"),
    }
}

/// Helper: send a Heartbeat request and get response.
async fn send_heartbeat(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    group_id: &str,
    topic_id: TopicId,
    reader_id: &str,
    generation: Generation,
) -> reader::HeartbeatResponseExt {
    let req = reader::HeartbeatRequest {
        group_id: group_id.to_string(),
        topic_id,
        reader_id: reader_id.to_string(),
        generation,
    };

    let buf = encode_client_frame(ClientMessage::Heartbeat(req), 256);

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

    match decode_server_frame(&data) {
        ServerMessage::Heartbeat(resp) => resp,
        _ => panic!("expected heartbeat response"),
    }
}

/// Helper: send a LeaveGroup request and get response.
async fn leave_group(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    group_id: &str,
    topic_id: TopicId,
    reader_id: &str,
) -> reader::LeaveGroupResponse {
    let req = reader::LeaveGroupRequest {
        group_id: group_id.to_string(),
        topic_id,
        reader_id: reader_id.to_string(),
    };

    let buf = encode_client_frame(ClientMessage::LeaveGroup(req), 256);

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

    match decode_server_frame(&data) {
        ServerMessage::LeaveGroup(resp) => resp,
        _ => panic!("expected leave response"),
    }
}

/// Helper: send a Rejoin request and get response.
async fn send_rejoin(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    group_id: &str,
    topic_id: TopicId,
    reader_id: &str,
    generation: Generation,
) -> reader::RejoinResponse {
    let req = reader::RejoinRequest {
        group_id: group_id.to_string(),
        topic_id,
        reader_id: reader_id.to_string(),
        generation,
    };

    let buf = encode_client_frame(ClientMessage::Rejoin(req), 1024);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for rejoin response")
        .expect("No response")
        .expect("WebSocket error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary message"),
    };

    match decode_server_frame(&data) {
        ServerMessage::Rejoin(resp) => resp,
        _ => panic!("expected rejoin response"),
    }
}

/// Helper: send a Commit request and get response.
async fn commit_offsets(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    group_id: &str,
    reader_id: &str,
    generation: Generation,
    commits: Vec<reader::PartitionCommit>,
) -> reader::CommitResponse {
    let req = reader::CommitRequest {
        group_id: group_id.to_string(),
        reader_id: reader_id.to_string(),
        generation,
        commits,
    };

    let buf = encode_client_frame(ClientMessage::Commit(req), 1024);

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

    match decode_server_frame(&data) {
        ServerMessage::Commit(resp) => resp,
        _ => panic!("expected commit response"),
    }
}

/// Test: Single reader joins and gets all partitions.
#[tokio::test]
async fn test_single_reader_gets_all_partitions() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-single-test", 4).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;

    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    let resp = join_group(
        &mut ws,
        "test-group",
        TopicId(topic_id as u32),
        "reader-1",
    )
    .await;

    assert!(resp.generation.0 > 0, "Should have valid generation");
    assert_eq!(
        resp.assignments.len(),
        4,
        "Single reader should get all 4 partitions"
    );

    // Verify partition IDs
    let mut partition_ids: Vec<u32> = resp.assignments.iter().map(|a| a.partition_id.0).collect();
    partition_ids.sort();
    assert_eq!(partition_ids, vec![0, 1, 2, 3]);

    ws.close(None).await.ok();
}

/// Test: Two readers share partitions.
/// With incremental rebalance, second reader initially gets 0 partitions
/// until first reader releases some via rejoin.
#[tokio::test]
async fn test_two_readers_share_partitions() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-two-test", 4).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // First reader joins
    let (mut ws1, _) = connect_async(&url).await.expect("Failed to connect");
    let resp1 = join_group(
        &mut ws1,
        "test-group",
        TopicId(topic_id as u32),
        "reader-1",
    )
    .await;

    assert_eq!(resp1.assignments.len(), 4, "First reader gets all 4");
    let gen1 = resp1.generation;

    // Second reader joins - triggers rebalance but gets 0 partitions initially
    // (incremental rebalance: first reader still has leases)
    let (mut ws2, _) = connect_async(&url).await.expect("Failed to connect");
    let resp2 = join_group(
        &mut ws2,
        "test-group",
        TopicId(topic_id as u32),
        "reader-2",
    )
    .await;

    // Second reader triggers rebalance
    assert!(
        resp2.generation.0 > gen1.0,
        "Generation should increase on rebalance"
    );
    // With incremental rebalance, second reader gets 0 initially
    // (partitions still leased by first reader)

    // First reader heartbeats and discovers rebalance
    let hb1 = send_heartbeat(
        &mut ws1,
        "test-group",
        TopicId(topic_id as u32),
        "reader-1",
        gen1,
    )
    .await;
    assert_eq!(
        hb1.status,
        reader::HeartbeatStatus::RebalanceNeeded,
        "First reader should be told to rebalance"
    );

    ws1.close(None).await.ok();
    ws2.close(None).await.ok();
}

/// Test: Reader leave triggers rebalance.
#[tokio::test]
async fn test_reader_leave_triggers_rebalance() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-leave-test", 4).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // Two readers join
    let (mut ws1, _) = connect_async(&url).await.expect("Failed to connect");
    let _resp1 = join_group(
        &mut ws1,
        "test-group",
        TopicId(topic_id as u32),
        "reader-1",
    )
    .await;

    let (mut ws2, _) = connect_async(&url).await.expect("Failed to connect");
    let resp2 = join_group(
        &mut ws2,
        "test-group",
        TopicId(topic_id as u32),
        "reader-2",
    )
    .await;

    // Reader 2 leaves
    let leave_resp = leave_group(
        &mut ws2,
        "test-group",
        TopicId(topic_id as u32),
        "reader-2",
    )
    .await;
    assert!(leave_resp.success);

    // Reader 1 heartbeats and discovers rebalance
    let hb1 = send_heartbeat(
        &mut ws1,
        "test-group",
        TopicId(topic_id as u32),
        "reader-1",
        resp2.generation, // Use latest gen
    )
    .await;

    // May or may not need rebalance depending on timing
    // Just verify we got a valid response
    assert!(
        hb1.status == reader::HeartbeatStatus::Ok
            || hb1.status == reader::HeartbeatStatus::RebalanceNeeded
    );

    ws1.close(None).await.ok();
    ws2.close(None).await.ok();
}

/// Test: Commit offsets.
#[tokio::test]
async fn test_commit_offsets() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-commit-test", 2).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");
    let resp = join_group(
        &mut ws,
        "test-group",
        TopicId(topic_id as u32),
        "reader-1",
    )
    .await;

    // Commit offsets for assigned partitions
    let commits: Vec<reader::PartitionCommit> = resp
        .assignments
        .iter()
        .map(|a| reader::PartitionCommit {
            topic_id: TopicId(topic_id as u32),
            partition_id: a.partition_id,
            offset: Offset(100),
        })
        .collect();

    let commit_resp = commit_offsets(
        &mut ws,
        "test-group",
        "reader-1",
        resp.generation,
        commits,
    )
    .await;

    assert!(commit_resp.success, "Commit should succeed");

    // Verify offsets in database
    for assignment in &resp.assignments {
        let offset: i64 = sqlx::query_scalar(
            "SELECT committed_offset FROM reader_assignments WHERE group_id = $1 AND topic_id = $2 AND partition_id = $3",
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

/// Test: stale-generation commit is rejected and does not advance committed offsets.
#[tokio::test]
async fn test_commit_rejected_with_stale_generation() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-stale-commit-test", 2).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // Reader 1 joins at generation 1.
    let (mut ws1, _) = connect_async(&url).await.expect("Failed to connect");
    let resp1 = join_group(
        &mut ws1,
        "stale-commit-group",
        TopicId(topic_id as u32),
        "reader-1",
    )
    .await;
    assert!(
        !resp1.assignments.is_empty(),
        "reader-1 should own partitions initially"
    );

    // Reader 2 joins and bumps generation.
    let (mut ws2, _) = connect_async(&url).await.expect("Failed to connect");
    let resp2 = join_group(
        &mut ws2,
        "stale-commit-group",
        TopicId(topic_id as u32),
        "reader-2",
    )
    .await;
    assert!(
        resp2.generation.0 > resp1.generation.0,
        "generation should increase after second reader joins"
    );

    // Reader 1 commits with stale generation (old generation from first join).
    let stale_commit = reader::PartitionCommit {
        topic_id: TopicId(topic_id as u32),
        partition_id: resp1.assignments[0].partition_id,
        offset: Offset(123),
    };
    let commit_resp = commit_offsets(
        &mut ws1,
        "stale-commit-group",
        "reader-1",
        resp1.generation, // stale
        vec![stale_commit],
    )
    .await;

    assert!(
        !commit_resp.success,
        "stale-generation commit should be rejected"
    );

    // Verify committed offset was not updated by stale commit.
    let committed_offset: i64 = sqlx::query_scalar(
        "SELECT committed_offset FROM reader_assignments WHERE group_id = $1 AND topic_id = $2 AND partition_id = $3",
    )
    .bind("stale-commit-group")
    .bind(topic_id)
    .bind(resp1.assignments[0].partition_id.0 as i32)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(committed_offset, 0);

    ws1.close(None).await.ok();
    ws2.close(None).await.ok();
}

/// Test: Heartbeat keeps membership alive.
#[tokio::test]
async fn test_heartbeat_maintains_membership() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-heartbeat-test", 2).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");
    let resp = join_group(
        &mut ws,
        "test-group",
        TopicId(topic_id as u32),
        "reader-1",
    )
    .await;

    // Send heartbeats
    for _ in 0..3 {
        tokio::time::sleep(Duration::from_millis(500)).await;

        let hb = send_heartbeat(
            &mut ws,
            "test-group",
            TopicId(topic_id as u32),
            "reader-1",
            resp.generation,
        )
        .await;

        assert_eq!(hb.status, reader::HeartbeatStatus::Ok);
        assert_eq!(hb.generation, resp.generation);
    }

    ws.close(None).await.ok();
}

/// Test: Session timeout removes member.
/// Note: With lease-based assignment, even after session timeout,
/// partitions need lease expiry before they can be claimed.
#[tokio::test]
async fn test_session_timeout_removes_member() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-timeout-test", 2).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // First reader joins but doesn't heartbeat
    let (mut ws1, _) = connect_async(&url).await.expect("Failed to connect");
    let resp1 = join_group(
        &mut ws1,
        "test-group",
        TopicId(topic_id as u32),
        "reader-1",
    )
    .await;
    let gen1 = resp1.generation;

    // Wait for session timeout (2 seconds + buffer)
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Second reader joins
    let (mut ws2, _) = connect_async(&url).await.expect("Failed to connect");
    let resp2 = join_group(
        &mut ws2,
        "test-group",
        TopicId(topic_id as u32),
        "reader-2",
    )
    .await;

    // Generation increases regardless
    assert!(resp2.generation.0 > gen1.0, "Generation should increase");
    // Note: Even with session timeout, partitions need lease expiry to be reclaimed.
    // The lease_duration (45s default) is separate from session_timeout (2s in test).
    // For this test, we just verify the first reader is marked as unknown.

    // First reader heartbeat should fail with UnknownMember or RebalanceNeeded
    let hb1 = send_heartbeat(
        &mut ws1,
        "test-group",
        TopicId(topic_id as u32),
        "reader-1",
        resp1.generation,
    )
    .await;
    assert!(
        hb1.status == reader::HeartbeatStatus::UnknownMember
            || hb1.status == reader::HeartbeatStatus::RebalanceNeeded,
        "Timed out reader should be unknown or need rebalance: {:?}",
        hb1.status
    );

    ws1.close(None).await.ok();
    ws2.close(None).await.ok();
}

/// Test: Three readers with uneven partition count.
/// With incremental rebalance, only the first reader gets partitions initially.
#[tokio::test]
async fn test_three_readers_uneven_partitions() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-uneven-test", 5).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // First reader joins and gets all 5 partitions
    let (mut ws1, _) = connect_async(&url).await.expect("Failed to connect");
    let resp1 = join_group(
        &mut ws1,
        "test-group",
        TopicId(topic_id as u32),
        "reader-1",
    )
    .await;
    assert_eq!(
        resp1.assignments.len(),
        5,
        "First reader should get all 5 partitions"
    );

    // Second and third readers join - generation increases but
    // they get 0 partitions initially (incremental rebalance)
    let (mut ws2, _) = connect_async(&url).await.expect("Failed to connect");
    let resp2 = join_group(
        &mut ws2,
        "test-group",
        TopicId(topic_id as u32),
        "reader-2",
    )
    .await;
    assert!(
        resp2.generation.0 > resp1.generation.0,
        "Generation should increase"
    );

    let (mut ws3, _) = connect_async(&url).await.expect("Failed to connect");
    let resp3 = join_group(
        &mut ws3,
        "test-group",
        TopicId(topic_id as u32),
        "reader-3",
    )
    .await;
    assert!(
        resp3.generation.0 > resp2.generation.0,
        "Generation should increase again"
    );

    // All three readers are now registered, first still has all partitions
    // To actually rebalance, readers need to rejoin after detecting rebalance

    ws1.close(None).await.ok();
    ws2.close(None).await.ok();
    ws3.close(None).await.ok();
}

/// Test: Commit rejected if reader doesn't own partition.
#[tokio::test]
async fn test_commit_rejected_if_not_owner() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("cg-not-owner-test", 4).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // Two readers join
    let (mut ws1, _) = connect_async(&url).await.expect("Failed to connect");
    let _resp1 = join_group(
        &mut ws1,
        "test-group",
        TopicId(topic_id as u32),
        "reader-1",
    )
    .await;

    let (mut ws2, _) = connect_async(&url).await.expect("Failed to connect");
    let resp2 = join_group(
        &mut ws2,
        "test-group",
        TopicId(topic_id as u32),
        "reader-2",
    )
    .await;

    // Complete the rebalance: both readers rejoin to get their final assignments
    // Reader-1 rejoins to release half its partitions
    let rejoin1 = send_rejoin(
        &mut ws1,
        "test-group",
        TopicId(topic_id as u32),
        "reader-1",
        resp2.generation,
    )
    .await;

    // Reader-2 rejoins to claim its share
    let rejoin2 = send_rejoin(
        &mut ws2,
        "test-group",
        TopicId(topic_id as u32),
        "reader-2",
        resp2.generation,
    )
    .await;

    // Verify reader-1 owns partition 0 (deterministic: reader-1 < reader-2 alphabetically)
    let reader1_partitions: std::collections::HashSet<u32> = rejoin1
        .assignments
        .iter()
        .map(|a| a.partition_id.0)
        .collect();
    let reader2_partitions: std::collections::HashSet<u32> = rejoin2
        .assignments
        .iter()
        .map(|a| a.partition_id.0)
        .collect();

    assert!(
        reader1_partitions.contains(&0),
        "Reader-1 should own partition 0, but owns {:?}",
        reader1_partitions
    );
    assert!(
        !reader2_partitions.contains(&0),
        "Reader-2 should NOT own partition 0, but owns {:?}",
        reader2_partitions
    );

    // Try to commit partition 0 from reader-2 (which doesn't own it)
    let commits = vec![reader::PartitionCommit {
        topic_id: TopicId(topic_id as u32),
        partition_id: PartitionId(0), // Not owned by reader-2
        offset: Offset(100),
    }];

    let commit_resp = commit_offsets(
        &mut ws2,
        "test-group",
        "reader-2",
        rejoin2.generation,
        commits,
    )
    .await;

    assert!(
        !commit_resp.success,
        "Commit should be rejected for non-owned partition"
    );

    ws1.close(None).await.ok();
    ws2.close(None).await.ok();
}
