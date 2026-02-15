//! End-to-end WebSocket tests.
//!
//! These tests start a real broker server and connect via WebSocket.
//!
//! ```bash
//! DATABASE_URL=postgres://postgres:postgres@localhost:5433 cargo test --test e2e_websocket
//! ```

mod common;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use turbine_agent::{
    AclChecker, ApiKeyValidator, BrokerConfig, BrokerState, LocalFsStore, Operation,
    ResourceType,
};
use turbine_common::ids::{Offset, PartitionId, WriterId, SchemaId, AppendSeq, TopicId};
use turbine_common::types::{Record, RecordBatch};
use turbine_wire::{
    ClientMessage, ERR_AUTHZ_DENIED, ERR_DECODE_ERROR, ServerMessage, auth as wire_auth, reader,
    decode_server_message, encode_client_message, writer,
};

use common::TestDb;
use turbine_agent::buffer::BufferConfig;

/// Find an available port.
async fn find_available_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

/// Start the broker server in background.
async fn start_server(
    pool: sqlx::PgPool,
    temp_dir: &TempDir,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    start_server_with_auth(pool, temp_dir, false).await
}

/// Start the broker server in background with configurable auth.
async fn start_server_with_auth(
    pool: sqlx::PgPool,
    temp_dir: &TempDir,
    require_auth: bool,
) -> (SocketAddr, tokio::task::JoinHandle<()>) {
    let addr = find_available_port().await;
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());

    let config = BrokerConfig {
        bind_addr: addr,
        bucket: "test".to_string(),
        key_prefix: "data".to_string(),
        buffer: BufferConfig::default(),
        flush_interval: Duration::from_millis(50), // Moderate flush interval for tests
        require_auth,
        auth_timeout: Duration::from_secs(10),
    };

    let state = BrokerState::new(pool, store, config).await;

    let handle = tokio::spawn(async move {
        if let Err(e) = turbine_agent::run(state).await {
            eprintln!("Server error: {}", e);
        }
    });

    // Wait for server to start and flush task to be ready
    tokio::time::sleep(Duration::from_millis(200)).await;

    (addr, handle)
}

async fn authenticate_ws(ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>, api_key: &str) {
    let auth_req = wire_auth::AuthRequest {
        api_key: api_key.to_string(),
    };

    let buf = encode_client_frame(ClientMessage::Auth(auth_req), 512);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for auth response")
        .expect("No auth response")
        .expect("WebSocket auth response error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary auth response"),
    };
    let auth_resp = decode_auth_response(&data);
    assert!(
        auth_resp.success,
        "auth failed: {}",
        auth_resp.error_message
    );
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

fn decode_auth_response(data: &[u8]) -> wire_auth::AuthResponse {
    match decode_server_frame(data) {
        ServerMessage::Auth(resp) => resp,
        _ => panic!("expected auth response"),
    }
}

fn decode_produce_response(data: &[u8]) -> writer::AppendResponse {
    match decode_server_frame(data) {
        ServerMessage::Append(resp) => resp,
        _ => panic!("expected append response"),
    }
}

fn decode_read_response(data: &[u8]) -> reader::ReadResponse {
    match decode_server_frame(data) {
        ServerMessage::Read(resp) => resp,
        _ => panic!("expected read response"),
    }
}

fn decode_join_response(data: &[u8]) -> reader::JoinGroupResponse {
    match decode_server_frame(data) {
        ServerMessage::JoinGroup(resp) => resp,
        _ => panic!("expected join response"),
    }
}

fn decode_commit_response(data: &[u8]) -> reader::CommitResponse {
    match decode_server_frame(data) {
        ServerMessage::Commit(resp) => resp,
        _ => panic!("expected commit response"),
    }
}

/// Test basic append and read via WebSocket.
#[tokio::test]
async fn test_produce_fetch_roundtrip() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("ws-test", 1).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;

    // Connect writer
    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    // Create append request
    let writer_id = WriterId::new();
    let records = vec![
        Record {
            key: Some(Bytes::from("key-1")),
            value: Bytes::from(r#"{"msg":"hello"}"#),
        },
        Record {
            key: Some(Bytes::from("key-2")),
            value: Bytes::from(r#"{"msg":"world"}"#),
        },
    ];

    let req = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            schema_id: SchemaId(100),
            records: records.clone(),
        }],
    };

    // Encode and send
    let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);

    ws.send(Message::Binary(buf)).await.unwrap();

    // Receive response with timeout
    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for response")
        .expect("No response")
        .expect("WebSocket error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary message"),
    };

    let resp = decode_produce_response(&data);
    assert_eq!(resp.append_seq.0, 1);
    assert_eq!(resp.append_acks.len(), 1);
    assert_eq!(resp.append_acks[0].start_offset.0, 0);
    assert_eq!(resp.append_acks[0].end_offset.0, 2);

    // Now read the records (ungrouped read - group fields are not used by server)
    let fetch_req = reader::ReadRequest {
        group_id: String::new(),
        reader_id: String::new(),
        generation: turbine_common::ids::Generation(0),
        reads: vec![reader::PartitionRead {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            offset: Offset(0),
            max_bytes: 1024 * 1024,
        }],
    };

    let buf = encode_client_frame(ClientMessage::Read(fetch_req.clone()), 8192);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for read response")
        .expect("No response")
        .expect("WebSocket error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary message"),
    };

    let fetch_resp = decode_read_response(&data);
    assert_eq!(fetch_resp.results.len(), 1);
    assert_eq!(fetch_resp.results[0].records.len(), 2);
    assert_eq!(fetch_resp.results[0].high_watermark.0, 2);

    // Verify record content
    assert_eq!(
        fetch_resp.results[0].records[0].key,
        Some(Bytes::from("key-1"))
    );
    assert_eq!(
        fetch_resp.results[0].records[1].key,
        Some(Bytes::from("key-2"))
    );

    ws.close(None).await.ok();
}

/// Test read response keeps schema IDs correct when partition contains mixed-schema batches.
#[tokio::test]
async fn test_fetch_returns_schema_homogeneous_partition_results() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("ws-mixed-schema-read", 1).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    let writer_id = WriterId::new();

    let first = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from("k-100")),
                value: Bytes::from(r#"{"v":100}"#),
            }],
        }],
    };
    ws.send(Message::Binary(encode_client_frame(
        ClientMessage::Append(first),
        8192,
    )))
    .await
    .unwrap();
    let first_resp = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for first append response")
        .expect("No first append response")
        .expect("WebSocket error on first append");
    let first_data = match first_resp {
        Message::Binary(data) => data,
        _ => panic!("Expected binary response"),
    };
    let first_resp = decode_produce_response(&first_data);
    assert!(first_resp.success);
    assert_eq!(first_resp.error_code, 0);

    let second = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(2),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            schema_id: SchemaId(101),
            records: vec![Record {
                key: Some(Bytes::from("k-101")),
                value: Bytes::from(r#"{"v":101}"#),
            }],
        }],
    };
    ws.send(Message::Binary(encode_client_frame(
        ClientMessage::Append(second),
        8192,
    )))
    .await
    .unwrap();
    let second_resp = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for second append response")
        .expect("No second append response")
        .expect("WebSocket error on second append");
    let second_data = match second_resp {
        Message::Binary(data) => data,
        _ => panic!("Expected binary response"),
    };
    let second_resp = decode_produce_response(&second_data);
    assert!(second_resp.success);
    assert_eq!(second_resp.error_code, 0);

    let fetch_req = reader::ReadRequest {
        group_id: String::new(),
        reader_id: String::new(),
        generation: turbine_common::ids::Generation(0),
        reads: vec![reader::PartitionRead {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            offset: Offset(0),
            max_bytes: 1024 * 1024,
        }],
    };
    ws.send(Message::Binary(encode_client_frame(
        ClientMessage::Read(fetch_req),
        8192,
    )))
    .await
    .unwrap();

    let fetch_response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for read response")
        .expect("No read response")
        .expect("WebSocket error on read");
    let fetch_data = match fetch_response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary response"),
    };
    let fetch_resp = decode_read_response(&fetch_data);
    assert!(fetch_resp.success);
    assert_eq!(fetch_resp.error_code, 0);
    assert_eq!(
        fetch_resp.results.len(),
        2,
        "expected one PartitionResult per schema in mixed-schema read"
    );

    let mut schemas: Vec<u32> = fetch_resp.results.iter().map(|r| r.schema_id.0).collect();
    schemas.sort_unstable();
    assert_eq!(schemas, vec![100, 101]);
    assert!(fetch_resp.results.iter().all(|r| r.records.len() == 1));

    ws.close(None).await.ok();
}

/// Test that duplicate sequence dedup survives broker restart (cache miss path).
#[tokio::test]
async fn test_duplicate_seq_dedup_survives_agent_restart() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("dedup-restart-test", 1).await;
    let temp_dir = TempDir::new().unwrap();

    // Start first broker instance
    let (addr1, server_handle1) = start_server(db.pool.clone(), &temp_dir).await;
    let url1 = format!("ws://{}", addr1);
    let (mut ws1, _) = connect_async(&url1).await.expect("Failed to connect");

    let writer_id = WriterId::new();
    let req = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            schema_id: SchemaId(100),
            records: vec![
                Record {
                    key: Some(Bytes::from("k1")),
                    value: Bytes::from(r#"{"msg":"first"}"#),
                },
                Record {
                    key: Some(Bytes::from("k2")),
                    value: Bytes::from(r#"{"msg":"second"}"#),
                },
            ],
        }],
    };

    // First append (commits and persists writer_state)
    let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);
    ws1.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws1.next())
        .await
        .expect("Timeout waiting for first append response")
        .expect("No first append response")
        .expect("WebSocket error on first append");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary response"),
    };

    let first_resp = decode_produce_response(&data);
    assert_eq!(first_resp.append_seq.0, 1);
    assert!(first_resp.success);
    assert_eq!(first_resp.error_code, 0);
    assert_eq!(first_resp.append_acks.len(), 1);

    let offset_after_first: i64 = sqlx::query_scalar(
        "SELECT next_offset FROM partition_offsets WHERE topic_id = $1 AND partition_id = 0",
    )
    .bind(topic_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();

    let batch_count_after_first: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM topic_batches WHERE topic_id = $1 AND partition_id = 0",
    )
    .bind(topic_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(batch_count_after_first, 1);

    let (last_seq, last_acks): (i64, serde_json::Value) =
        sqlx::query_as("SELECT last_seq, last_acks FROM writer_state WHERE writer_id = $1")
            .bind(writer_id.0)
            .fetch_one(&db.pool)
            .await
            .unwrap();
    assert_eq!(last_seq, 1);
    assert!(
        last_acks
            .as_array()
            .map(|arr| !arr.is_empty())
            .unwrap_or(false),
        "last_acks should be a non-empty array"
    );

    ws1.close(None).await.ok();

    // Simulate cache miss by restarting the broker (new in-memory dedup cache).
    server_handle1.abort();
    let _ = server_handle1.await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start second broker instance
    let (addr2, server_handle2) = start_server(db.pool.clone(), &temp_dir).await;
    let url2 = format!("ws://{}", addr2);
    let (mut ws2, _) = connect_async(&url2).await.expect("Failed to connect");

    // Retry exact same writer_id + append_seq
    let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);
    ws2.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws2.next())
        .await
        .expect("Timeout waiting for duplicate append response")
        .expect("No duplicate append response")
        .expect("WebSocket error on duplicate append");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary response"),
    };

    let retry_resp = decode_produce_response(&data);
    assert_eq!(retry_resp.append_seq.0, 1);
    assert!(retry_resp.success);
    assert_eq!(retry_resp.error_code, 0);
    assert_eq!(retry_resp.append_acks.len(), 1);
    assert_eq!(retry_resp.append_acks[0], first_resp.append_acks[0]);

    // No second append should happen on duplicate retry.
    let offset_after_retry: i64 = sqlx::query_scalar(
        "SELECT next_offset FROM partition_offsets WHERE topic_id = $1 AND partition_id = 0",
    )
    .bind(topic_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(offset_after_retry, offset_after_first);

    let batch_count_after_retry: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM topic_batches WHERE topic_id = $1 AND partition_id = 0",
    )
    .bind(topic_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(batch_count_after_retry, 1);

    ws2.close(None).await.ok();
    server_handle2.abort();
    let _ = server_handle2.await;
}

/// Test that unprefixed append path enforces ACL when auth is enabled.
#[tokio::test]
async fn test_unprefixed_produce_acl_enforced_with_auth() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("unprefixed-append-authz", 1).await;
    let temp_dir = TempDir::new().unwrap();

    let validator = ApiKeyValidator::new(db.pool.clone());
    let checker = AclChecker::new(db.pool.clone());

    // Create one key with no ACL (default deny) and one with explicit append allow.
    let (deny_key, _) = validator
        .create_key("deny-writer", "service:deny-writer", None)
        .await
        .unwrap();
    let (allow_key, _) = validator
        .create_key("allow-writer", "service:allow-writer", None)
        .await
        .unwrap();

    checker
        .create_acl(
            "service:allow-writer",
            ResourceType::Topic,
            &topic_id.to_string(),
            Operation::Append,
            true,
        )
        .await
        .unwrap();

    let (addr, server_handle) = start_server_with_auth(db.pool.clone(), &temp_dir, true).await;
    let url = format!("ws://{}", addr);

    // Denied writer sends append; should not append records.
    let (mut ws_deny, _) = connect_async(&url).await.unwrap();
    authenticate_ws(&mut ws_deny, &deny_key).await;

    let deny_req = writer::AppendRequest {
        writer_id: WriterId::new(),
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from("deny")),
                value: Bytes::from("should-not-write"),
            }],
        }],
    };

    let buf = encode_client_frame(ClientMessage::Append(deny_req.clone()), 8192);
    ws_deny.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws_deny.next())
        .await
        .expect("Timeout waiting for deny append response")
        .expect("No deny append response")
        .expect("WebSocket error on deny append");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary deny append response"),
    };
    let deny_resp = decode_produce_response(&data);
    assert_eq!(deny_resp.append_seq.0, 0);
    assert!(!deny_resp.success);
    assert_eq!(deny_resp.error_code, ERR_AUTHZ_DENIED);
    assert!(deny_resp.append_acks.is_empty());

    let offset_after_deny: i64 = sqlx::query_scalar(
        "SELECT next_offset FROM partition_offsets WHERE topic_id = $1 AND partition_id = 0",
    )
    .bind(topic_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(offset_after_deny, 0);
    ws_deny.close(None).await.ok();

    // Allowed writer sends append; should append records.
    let (mut ws_allow, _) = connect_async(&url).await.unwrap();
    authenticate_ws(&mut ws_allow, &allow_key).await;

    let allow_req = writer::AppendRequest {
        writer_id: WriterId::new(),
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from("allow")),
                value: Bytes::from("should-write"),
            }],
        }],
    };

    let buf = encode_client_frame(ClientMessage::Append(allow_req.clone()), 8192);
    ws_allow.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws_allow.next())
        .await
        .expect("Timeout waiting for allow append response")
        .expect("No allow append response")
        .expect("WebSocket error on allow append");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary allow append response"),
    };
    let allow_resp = decode_produce_response(&data);
    assert_eq!(allow_resp.append_seq.0, 1);
    assert!(allow_resp.success);
    assert_eq!(allow_resp.error_code, 0);
    assert_eq!(allow_resp.append_acks.len(), 1);

    let offset_after_allow: i64 = sqlx::query_scalar(
        "SELECT next_offset FROM partition_offsets WHERE topic_id = $1 AND partition_id = 0",
    )
    .bind(topic_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(offset_after_allow, 1);

    ws_allow.close(None).await.ok();
    server_handle.abort();
    let _ = server_handle.await;
}

/// Test writer UUID first byte colliding with old tags does not affect union framing.
#[tokio::test]
async fn test_produce_uuid_first_byte_collision_is_safe() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("unprefixed-collision", 1).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    // Set writer UUID first byte to a value that previously matched message tags.
    let mut producer_uuid = [0u8; 16];
    producer_uuid[0] = 5;
    let req = writer::AppendRequest {
        writer_id: WriterId::from_uuid(uuid::Uuid::from_bytes(producer_uuid)),
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from("k")),
                value: Bytes::from("v"),
            }],
        }],
    };

    let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);
    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout")
        .expect("No response")
        .expect("Error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary"),
    };
    let resp = decode_produce_response(&data);
    assert_eq!(resp.append_seq.0, 1);
    assert!(
        resp.success,
        "error {}: {}",
        resp.error_code, resp.error_message
    );
    assert_eq!(resp.error_code, 0);
    assert_eq!(resp.append_acks.len(), 1);

    let next_offset: i64 = sqlx::query_scalar(
        "SELECT next_offset FROM partition_offsets WHERE topic_id = $1 AND partition_id = 0",
    )
    .bind(topic_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(next_offset, 1);

    ws.close(None).await.ok();
}

/// Test that prefixed JoinGroup ACL denial returns JoinGroupResponse wire shape.
#[tokio::test]
async fn test_prefixed_join_acl_denied_returns_typed_error_response() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("prefixed-join-authz", 1).await;
    let temp_dir = TempDir::new().unwrap();

    let validator = ApiKeyValidator::new(db.pool.clone());
    let checker = AclChecker::new(db.pool.clone());
    let (deny_key, _) = validator
        .create_key("deny-reader", "service:deny-reader", None)
        .await
        .unwrap();
    checker
        .create_acl(
            "service:deny-reader",
            ResourceType::Group,
            "authz-denied-group",
            Operation::Consume,
            true,
        )
        .await
        .unwrap();

    let (addr, server_handle) = start_server_with_auth(db.pool.clone(), &temp_dir, true).await;
    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.unwrap();
    authenticate_ws(&mut ws, &deny_key).await;

    let req = reader::JoinGroupRequest {
        group_id: "authz-denied-group".to_string(),
        reader_id: "reader-1".to_string(),
        topic_ids: vec![TopicId(topic_id as u32)],
    };

    let buf = encode_client_frame(ClientMessage::JoinGroup(req.clone()), 1024);
    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for join deny response")
        .expect("No join deny response")
        .expect("WebSocket error on join deny");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary join deny response"),
    };
    let join_resp = decode_join_response(&data);
    assert!(!join_resp.success);
    assert_eq!(join_resp.error_code, ERR_AUTHZ_DENIED);
    assert_eq!(join_resp.generation.0, 0);
    assert!(join_resp.assignments.is_empty());

    ws.close(None).await.ok();
    server_handle.abort();
    let _ = server_handle.await;
}

/// Test that commit requires topic consume ACL in addition to group ACL.
#[tokio::test]
async fn test_prefixed_commit_requires_topic_acl() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("prefixed-commit-authz", 1).await;
    let temp_dir = TempDir::new().unwrap();

    let validator = ApiKeyValidator::new(db.pool.clone());
    let checker = AclChecker::new(db.pool.clone());
    let (api_key, _) = validator
        .create_key("commit-reader", "service:commit-reader", None)
        .await
        .unwrap();

    // Allow group consume only. Topic consume is intentionally not granted.
    checker
        .create_acl(
            "service:commit-reader",
            ResourceType::Group,
            "commit-authz-group",
            Operation::Consume,
            true,
        )
        .await
        .unwrap();

    let (addr, server_handle) = start_server_with_auth(db.pool.clone(), &temp_dir, true).await;
    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.unwrap();
    authenticate_ws(&mut ws, &api_key).await;

    let req = reader::CommitRequest {
        group_id: "commit-authz-group".to_string(),
        reader_id: "reader-1".to_string(),
        generation: turbine_common::ids::Generation(1),
        commits: vec![reader::PartitionCommit {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            offset: Offset(1),
        }],
    };

    let buf = encode_client_frame(ClientMessage::Commit(req.clone()), 1024);
    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for commit deny response")
        .expect("No commit deny response")
        .expect("WebSocket commit deny error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary commit deny response"),
    };
    let commit_resp = decode_commit_response(&data);
    assert!(!commit_resp.success);
    assert_eq!(commit_resp.error_code, ERR_AUTHZ_DENIED);

    ws.close(None).await.ok();
    server_handle.abort();
    let _ = server_handle.await;
}

/// Test that malformed proto-framed JoinGroup returns JoinGroupResponse wire shape.
#[tokio::test]
async fn test_prefixed_join_decode_failure_returns_typed_error_response() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.unwrap();

    // Protobuf key says JoinGroup (field #3), length varint is intentionally malformed.
    ws.send(Message::Binary(vec![26, 0x80])).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for malformed join response")
        .expect("No malformed join response")
        .expect("WebSocket error on malformed join");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary malformed join response"),
    };
    let join_resp = decode_join_response(&data);
    assert!(!join_resp.success);
    assert_eq!(join_resp.error_code, ERR_DECODE_ERROR);
    assert_eq!(join_resp.generation.0, 0);
    assert!(join_resp.assignments.is_empty());

    ws.close(None).await.ok();
    server_handle.abort();
    let _ = server_handle.await;
}

/// Test that in-flight duplicate append_seq requests are coalesced to a single commit.
#[tokio::test]
async fn test_inflight_duplicate_seq_coalesced_single_commit() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("inflight-dup-coalesce", 1).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);
    let (mut ws1, _) = connect_async(&url).await.expect("Failed to connect ws1");
    let (mut ws2, _) = connect_async(&url).await.expect("Failed to connect ws2");

    let writer_id = WriterId::new();
    let req = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from("k")),
                value: Bytes::from("v"),
            }],
        }],
    };

    let buf1 = encode_client_frame(ClientMessage::Append(req.clone()), 8192);
    ws1.send(Message::Binary(buf1)).await.unwrap();

    let buf2 = encode_client_frame(ClientMessage::Append(req.clone()), 8192);
    ws2.send(Message::Binary(buf2)).await.unwrap();

    let resp1 = tokio::time::timeout(Duration::from_secs(5), ws1.next())
        .await
        .expect("Timeout waiting for ws1 response")
        .expect("No ws1 response")
        .expect("WebSocket ws1 error");
    let resp2 = tokio::time::timeout(Duration::from_secs(5), ws2.next())
        .await
        .expect("Timeout waiting for ws2 response")
        .expect("No ws2 response")
        .expect("WebSocket ws2 error");

    let data1 = match resp1 {
        Message::Binary(data) => data,
        _ => panic!("Expected binary ws1 response"),
    };
    let data2 = match resp2 {
        Message::Binary(data) => data,
        _ => panic!("Expected binary ws2 response"),
    };

    let r1 = decode_produce_response(&data1);
    let r2 = decode_produce_response(&data2);
    assert!(r1.success);
    assert!(r2.success);
    assert_eq!(r1.error_code, 0);
    assert_eq!(r2.error_code, 0);
    assert_eq!(r1.append_seq.0, 1);
    assert_eq!(r2.append_seq.0, 1);
    assert_eq!(r1.append_acks.len(), 1);
    assert_eq!(r2.append_acks.len(), 1);
    assert_eq!(r1.append_acks[0], r2.append_acks[0]);

    let next_offset: i64 = sqlx::query_scalar(
        "SELECT next_offset FROM partition_offsets WHERE topic_id = $1 AND partition_id = 0",
    )
    .bind(topic_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();
    assert_eq!(next_offset, 1);

    ws1.close(None).await.ok();
    ws2.close(None).await.ok();
}

/// Test multiple writers to same partition.
#[tokio::test]
async fn test_multiple_producers() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("multi-writer-test", 1).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // Connect multiple writers
    let mut handles = vec![];
    for i in 0..5 {
        let url = url.clone();
        let topic_id = topic_id;

        let handle = tokio::spawn(async move {
            let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

            let writer_id = WriterId::new();
            let req = writer::AppendRequest {
                writer_id,
                append_seq: AppendSeq(1),
                batches: vec![RecordBatch {
                    topic_id: TopicId(topic_id as u32),
                    partition_id: PartitionId(0),
                    schema_id: SchemaId(100),
                    records: vec![Record {
                        key: Some(Bytes::from(format!("writer-{}", i))),
                        value: Bytes::from(format!(r#"{{"id":{}}}"#, i)),
                    }],
                }],
            };

            let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);

            ws.send(Message::Binary(buf)).await.unwrap();

            let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
                .await
                .expect("Timeout")
                .expect("No response")
                .expect("Error");

            let data = match response {
                Message::Binary(data) => data,
                _ => panic!("Expected binary"),
            };

            let resp = decode_produce_response(&data);
            assert!(resp.success);
            assert_eq!(resp.error_code, 0);
            ws.close(None).await.ok();

            resp.append_acks[0].clone()
        });

        handles.push(handle);
    }

    // Collect all append_acks
    let mut append_acks = vec![];
    for handle in handles {
        append_acks.push(handle.await.unwrap());
    }

    // Verify all records were stored (total 5)
    let total_offset: i64 = sqlx::query_scalar(
        "SELECT next_offset FROM partition_offsets WHERE topic_id = $1 AND partition_id = 0",
    )
    .bind(topic_id)
    .fetch_one(&db.pool)
    .await
    .unwrap();

    assert_eq!(total_offset, 5);

    // Read all records (ungrouped read - group fields are not used by server)
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    let fetch_req = reader::ReadRequest {
        group_id: String::new(),
        reader_id: String::new(),
        generation: turbine_common::ids::Generation(0),
        reads: vec![reader::PartitionRead {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            offset: Offset(0),
            max_bytes: 1024 * 1024,
        }],
    };

    let buf = encode_client_frame(ClientMessage::Read(fetch_req.clone()), 8192);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout")
        .expect("No response")
        .expect("Error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary"),
    };

    let fetch_resp = decode_read_response(&data);
    assert!(fetch_resp.success);
    assert_eq!(fetch_resp.error_code, 0);
    assert_eq!(fetch_resp.results[0].records.len(), 5);

    ws.close(None).await.ok();
}

/// Test producing to multiple partitions.
#[tokio::test]
async fn test_multi_partition_produce() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("multi-partition-test", 3).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    // Append to all 3 partitions in one request
    let req = writer::AppendRequest {
        writer_id: WriterId::new(),
        append_seq: AppendSeq(1),
        batches: vec![
            RecordBatch {
                topic_id: TopicId(topic_id as u32),
                partition_id: PartitionId(0),
                schema_id: SchemaId(100),
                records: vec![
                    Record {
                        key: Some(Bytes::from("p0-k1")),
                        value: Bytes::from("v1"),
                    },
                    Record {
                        key: Some(Bytes::from("p0-k2")),
                        value: Bytes::from("v2"),
                    },
                ],
            },
            RecordBatch {
                topic_id: TopicId(topic_id as u32),
                partition_id: PartitionId(1),
                schema_id: SchemaId(100),
                records: vec![Record {
                    key: Some(Bytes::from("p1-k1")),
                    value: Bytes::from("v1"),
                }],
            },
            RecordBatch {
                topic_id: TopicId(topic_id as u32),
                partition_id: PartitionId(2),
                schema_id: SchemaId(100),
                records: vec![
                    Record {
                        key: Some(Bytes::from("p2-k1")),
                        value: Bytes::from("v1"),
                    },
                    Record {
                        key: Some(Bytes::from("p2-k2")),
                        value: Bytes::from("v2"),
                    },
                    Record {
                        key: Some(Bytes::from("p2-k3")),
                        value: Bytes::from("v3"),
                    },
                ],
            },
        ],
    };

    let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout")
        .expect("No response")
        .expect("Error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary"),
    };

    let resp = decode_produce_response(&data);
    assert!(resp.success);
    assert_eq!(resp.error_code, 0);
    assert_eq!(resp.append_acks.len(), 3);

    // Verify partition offsets
    for (partition_id, expected_offset) in [(0, 2), (1, 1), (2, 3)] {
        let offset: i64 = sqlx::query_scalar(
            "SELECT next_offset FROM partition_offsets WHERE topic_id = $1 AND partition_id = $2",
        )
        .bind(topic_id)
        .bind(partition_id)
        .fetch_one(&db.pool)
        .await
        .unwrap();

        assert_eq!(
            offset, expected_offset,
            "Partition {} should have offset {}",
            partition_id, expected_offset
        );
    }

    ws.close(None).await.ok();
}

/// Test read from specific offset.
#[tokio::test]
async fn test_fetch_from_offset() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("read-offset-test", 1).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    // Append 10 records
    let records: Vec<Record> = (0..10)
        .map(|i| Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!(r#"{{"append_seq":{}}}"#, i)),
        })
        .collect();

    let req = writer::AppendRequest {
        writer_id: WriterId::new(),
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            schema_id: SchemaId(100),
            records,
        }],
    };

    let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary"),
    };
    let produce_resp = decode_produce_response(&data);
    assert!(produce_resp.success);
    assert_eq!(produce_resp.error_code, 0);

    // Read from offset 5 (should get records 5-9)
    // Ungrouped read - group fields are not used by server
    let fetch_req = reader::ReadRequest {
        group_id: String::new(),
        reader_id: String::new(),
        generation: turbine_common::ids::Generation(0),
        reads: vec![reader::PartitionRead {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            offset: Offset(5),
            max_bytes: 1024 * 1024,
        }],
    };

    let buf = encode_client_frame(ClientMessage::Read(fetch_req.clone()), 8192);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary"),
    };

    let fetch_resp = decode_read_response(&data);
    assert!(fetch_resp.success);
    assert_eq!(fetch_resp.error_code, 0);
    assert_eq!(fetch_resp.results.len(), 1);
    assert_eq!(fetch_resp.results[0].records.len(), 5); // Records 5-9

    // Verify first record is key-5
    assert_eq!(
        fetch_resp.results[0].records[0].key,
        Some(Bytes::from("key-5"))
    );

    ws.close(None).await.ok();
}
