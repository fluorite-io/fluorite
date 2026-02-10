//! End-to-end WebSocket tests.
//!
//! These tests start a real agent server and connect via WebSocket.
//!
//! ```bash
//! DATABASE_URL=postgres://postgres:turbine@localhost:5433 cargo test --test e2e_websocket
//! ```

mod common;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use turbine_agent::{AgentConfig, AgentState, LocalFsStore};
use turbine_common::ids::{Offset, PartitionId, ProducerId, SchemaId, SeqNum, TopicId};
use turbine_common::types::{Record, Segment};
use turbine_wire::{consumer, producer};

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

/// Start the agent server in background.
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

    let state = Arc::new(AgentState::new(pool, store, config));

    let handle = tokio::spawn(async move {
        if let Err(e) = turbine_agent::run(state).await {
            eprintln!("Server error: {}", e);
        }
    });

    // Wait for server to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    (addr, handle)
}

/// Test basic produce and fetch via WebSocket.
#[tokio::test]
async fn test_produce_fetch_roundtrip() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("ws-test", 1).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;

    // Connect producer
    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    // Create produce request
    let producer_id = ProducerId::new();
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

    let req = producer::ProduceRequest {
        producer_id,
        seq: SeqNum(1),
        segments: vec![Segment {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            schema_id: SchemaId(100),
            records: records.clone(),
        }],
    };

    // Encode and send
    let mut buf = vec![0u8; 8192];
    let len = producer::encode_request(&req, &mut buf);
    buf.truncate(len);

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

    let (resp, _) = producer::decode_response(&data).unwrap();
    assert_eq!(resp.seq.0, 1);
    assert_eq!(resp.acks.len(), 1);
    assert_eq!(resp.acks[0].start_offset.0, 0);
    assert_eq!(resp.acks[0].end_offset.0, 2);

    // Now fetch the records
    let fetch_req = consumer::FetchRequest {
        group_id: "test-group".to_string(),
        consumer_id: "test-consumer".to_string(),
        generation: turbine_common::ids::Generation(1),
        fetches: vec![consumer::PartitionFetch {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            offset: Offset(0),
            max_bytes: 1024 * 1024,
        }],
    };

    let mut buf = vec![0u8; 8192];
    let len = consumer::encode_fetch_request(&fetch_req, &mut buf);
    buf.truncate(len);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for fetch response")
        .expect("No response")
        .expect("WebSocket error");

    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary message"),
    };

    let (fetch_resp, _) = consumer::decode_fetch_response(&data).unwrap();
    assert_eq!(fetch_resp.results.len(), 1);
    assert_eq!(fetch_resp.results[0].records.len(), 2);
    assert_eq!(fetch_resp.results[0].high_watermark.0, 2);

    // Verify record content
    assert_eq!(fetch_resp.results[0].records[0].key, Some(Bytes::from("key-1")));
    assert_eq!(fetch_resp.results[0].records[1].key, Some(Bytes::from("key-2")));

    ws.close(None).await.ok();
}

/// Test multiple producers to same partition.
#[tokio::test]
async fn test_multiple_producers() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("multi-producer-test", 1).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // Connect multiple producers
    let mut handles = vec![];
    for i in 0..5 {
        let url = url.clone();
        let topic_id = topic_id;

        let handle = tokio::spawn(async move {
            let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

            let producer_id = ProducerId::new();
            let req = producer::ProduceRequest {
                producer_id,
                seq: SeqNum(1),
                segments: vec![Segment {
                    topic_id: TopicId(topic_id as u32),
                    partition_id: PartitionId(0),
                    schema_id: SchemaId(100),
                    records: vec![Record {
                        key: Some(Bytes::from(format!("producer-{}", i))),
                        value: Bytes::from(format!(r#"{{"id":{}}}"#, i)),
                    }],
                }],
            };

            let mut buf = vec![0u8; 8192];
            let len = producer::encode_request(&req, &mut buf);
            buf.truncate(len);

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

            let (resp, _) = producer::decode_response(&data).unwrap();
            ws.close(None).await.ok();

            resp.acks[0].clone()
        });

        handles.push(handle);
    }

    // Collect all acks
    let mut acks = vec![];
    for handle in handles {
        acks.push(handle.await.unwrap());
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

    // Fetch all records
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    let fetch_req = consumer::FetchRequest {
        group_id: "test".to_string(),
        consumer_id: "test".to_string(),
        generation: turbine_common::ids::Generation(1),
        fetches: vec![consumer::PartitionFetch {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            offset: Offset(0),
            max_bytes: 1024 * 1024,
        }],
    };

    let mut buf = vec![0u8; 8192];
    let len = consumer::encode_fetch_request(&fetch_req, &mut buf);
    buf.truncate(len);

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

    let (fetch_resp, _) = consumer::decode_fetch_response(&data).unwrap();
    assert_eq!(fetch_resp.results[0].records.len(), 5);

    ws.close(None).await.ok();
}

/// Test producing to multiple partitions.
#[tokio::test]
async fn test_multi_partition_produce() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("multi-partition-test", 3).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    // Produce to all 3 partitions in one request
    let req = producer::ProduceRequest {
        producer_id: ProducerId::new(),
        seq: SeqNum(1),
        segments: vec![
            Segment {
                topic_id: TopicId(topic_id as u32),
                partition_id: PartitionId(0),
                schema_id: SchemaId(100),
                records: vec![
                    Record { key: Some(Bytes::from("p0-k1")), value: Bytes::from("v1") },
                    Record { key: Some(Bytes::from("p0-k2")), value: Bytes::from("v2") },
                ],
            },
            Segment {
                topic_id: TopicId(topic_id as u32),
                partition_id: PartitionId(1),
                schema_id: SchemaId(100),
                records: vec![
                    Record { key: Some(Bytes::from("p1-k1")), value: Bytes::from("v1") },
                ],
            },
            Segment {
                topic_id: TopicId(topic_id as u32),
                partition_id: PartitionId(2),
                schema_id: SchemaId(100),
                records: vec![
                    Record { key: Some(Bytes::from("p2-k1")), value: Bytes::from("v1") },
                    Record { key: Some(Bytes::from("p2-k2")), value: Bytes::from("v2") },
                    Record { key: Some(Bytes::from("p2-k3")), value: Bytes::from("v3") },
                ],
            },
        ],
    };

    let mut buf = vec![0u8; 8192];
    let len = producer::encode_request(&req, &mut buf);
    buf.truncate(len);

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

    let (resp, _) = producer::decode_response(&data).unwrap();
    assert_eq!(resp.acks.len(), 3);

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

/// Test fetch from specific offset.
#[tokio::test]
async fn test_fetch_from_offset() {
    if skip_if_no_db() {
        eprintln!("Skipping test: DATABASE_URL not set");
        return;
    }

    let db = TestDb::new().await;
    let topic_id = db.create_topic("fetch-offset-test", 1).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    // Produce 10 records
    let records: Vec<Record> = (0..10)
        .map(|i| Record {
            key: Some(Bytes::from(format!("key-{}", i))),
            value: Bytes::from(format!(r#"{{"seq":{}}}"#, i)),
        })
        .collect();

    let req = producer::ProduceRequest {
        producer_id: ProducerId::new(),
        seq: SeqNum(1),
        segments: vec![Segment {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            schema_id: SchemaId(100),
            records,
        }],
    };

    let mut buf = vec![0u8; 8192];
    let len = producer::encode_request(&req, &mut buf);
    buf.truncate(len);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await.unwrap().unwrap().unwrap();
    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary"),
    };
    let _ = producer::decode_response(&data).unwrap();

    // Fetch from offset 5 (should get records 5-9)
    let fetch_req = consumer::FetchRequest {
        group_id: "test".to_string(),
        consumer_id: "test".to_string(),
        generation: turbine_common::ids::Generation(1),
        fetches: vec![consumer::PartitionFetch {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(0),
            offset: Offset(5),
            max_bytes: 1024 * 1024,
        }],
    };

    let mut buf = vec![0u8; 8192];
    let len = consumer::encode_fetch_request(&fetch_req, &mut buf);
    buf.truncate(len);

    ws.send(Message::Binary(buf)).await.unwrap();

    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await.unwrap().unwrap().unwrap();
    let data = match response {
        Message::Binary(data) => data,
        _ => panic!("Expected binary"),
    };

    let (fetch_resp, _) = consumer::decode_fetch_response(&data).unwrap();
    assert_eq!(fetch_resp.results.len(), 1);
    assert_eq!(fetch_resp.results[0].records.len(), 5); // Records 5-9

    // Verify first record is key-5
    assert_eq!(
        fetch_resp.results[0].records[0].key,
        Some(Bytes::from("key-5"))
    );

    ws.close(None).await.ok();
}
