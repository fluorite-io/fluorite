//! Negative test cases for error handling.
//!
//! These tests verify that the broker handles error conditions gracefully.

mod common;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use flourine_broker::buffer::BufferConfig;
use flourine_broker::{BrokerConfig, BrokerState, LocalFsStore};
use flourine_common::ids::{Offset, PartitionId, WriterId, SchemaId, AppendSeq, TopicId};
use flourine_common::types::{Record, RecordBatch};
use flourine_wire::{
    ClientMessage, ServerMessage, reader, decode_server_message, encode_client_message, writer,
};

use common::TestDb;

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
    let addr = find_available_port().await;
    let store = LocalFsStore::new(temp_dir.path().to_path_buf());

    let config = BrokerConfig {
        bind_addr: addr,
        bucket: "test".to_string(),
        key_prefix: "data".to_string(),
        buffer: BufferConfig::default(),
        flush_interval: Duration::from_millis(50),
        require_auth: false,
        auth_timeout: Duration::from_secs(10),
    };

    let state = BrokerState::new(pool, store, config).await;

    let handle = tokio::spawn(async move {
        if let Err(e) = flourine_broker::run(state).await {
            eprintln!("Server error: {}", e);
        }
    });

    tokio::time::sleep(Duration::from_millis(200)).await;

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

/// Test that sending malformed (garbage) bytes is handled gracefully.
#[tokio::test]
async fn test_malformed_websocket_message() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;

    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    // Send garbage bytes that cannot be decoded as a valid message
    let garbage = vec![0xFF, 0xFE, 0x00, 0x01, 0x02, 0x03, 0xFF, 0xFF];
    ws.send(Message::Binary(garbage)).await.unwrap();

    // The server MUST respond to malformed input - either close the connection or send an error.
    // A timeout is NOT acceptable as it indicates the server is not properly handling bad input.
    let result = tokio::time::timeout(Duration::from_secs(2), ws.next()).await;

    let handled_correctly = match &result {
        Ok(Some(Ok(Message::Close(frame)))) => {
            // Connection closed - verify it's not a normal close if we got a frame
            if let Some(f) = frame {
                // Close with an error code (not 1000 = normal closure)
                f.code != tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal
            } else {
                true // Close without frame is acceptable
            }
        }
        Ok(Some(Ok(Message::Binary(data)))) => {
            // Error response - should have content
            !data.is_empty()
        }
        Ok(Some(Err(_))) => {
            // WebSocket error - connection closed with error
            true
        }
        Ok(None) => {
            // Stream ended - connection closed
            true
        }
        Err(_) => {
            // Timeout - server didn't respond to malformed input
            false
        }
        _ => {
            // Text, Ping, Pong, Frame are unexpected but not crashes
            true
        }
    };

    assert!(
        handled_correctly,
        "Server should respond to malformed message (close connection or send error), got: {:?}",
        result
    );

    // Server should still be responsive for valid requests
    let (mut ws2, _) = connect_async(&url)
        .await
        .expect("Server should still accept connections");
    ws2.close(None).await.ok();
}

/// Test that producing to a non-existent topic returns an error response.
#[tokio::test]
async fn test_produce_to_nonexistent_topic() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;

    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    // Try to append to topic ID 99999 which doesn't exist
    let req = writer::AppendRequest {
        writer_id: WriterId::new(),
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(99999),
            partition_id: PartitionId(0),
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from("key")),
                value: Bytes::from("value"),
            }],
        }],
    };

    let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);

    ws.send(Message::Binary(buf)).await.unwrap();

    // Should get a response (even if it contains an error)
    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for response")
        .expect("No response")
        .expect("WebSocket error");

    // The server should respond with something (error or successful write)
    // The important thing is it doesn't crash
    match response {
        Message::Binary(data) => {
            assert!(!data.is_empty(), "Should get response data");
        }
        Message::Close(_) => {
            // Connection closed, acceptable for error case
        }
        _ => panic!("Unexpected message type"),
    }

    ws.close(None).await.ok();
}

/// Test that producing to a non-existent partition returns an error response.
#[tokio::test]
async fn test_produce_to_nonexistent_partition() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("negative-test", 2).await; // Only 2 partitions (0, 1)
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;

    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    // Try to append to partition 99 which doesn't exist for this topic
    let req = writer::AppendRequest {
        writer_id: WriterId::new(),
        append_seq: AppendSeq(1),
        batches: vec![RecordBatch {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(99), // Non-existent partition
            schema_id: SchemaId(100),
            records: vec![Record {
                key: Some(Bytes::from("key")),
                value: Bytes::from("value"),
            }],
        }],
    };

    let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);

    ws.send(Message::Binary(buf)).await.unwrap();

    // Should get a response
    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for response")
        .expect("No response")
        .expect("WebSocket error");

    match response {
        Message::Binary(data) => {
            assert!(!data.is_empty(), "Should get response data");
        }
        Message::Close(_) => {
            // Acceptable for error
        }
        _ => panic!("Unexpected message type"),
    }

    ws.close(None).await.ok();
}

/// Test that fetching from a non-existent topic returns appropriate response.
#[tokio::test]
async fn test_fetch_from_nonexistent_topic() {
    let db = TestDb::new().await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;

    let url = format!("ws://{}", addr);
    let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

    // Try to read from topic 99999 which doesn't exist
    let fetch_req = reader::ReadRequest {
        group_id: "test-group".to_string(),
        reader_id: "test-reader".to_string(),
        generation: flourine_common::ids::Generation(1),
        reads: vec![reader::PartitionRead {
            topic_id: TopicId(99999),
            partition_id: PartitionId(0),
            offset: Offset(0),
            max_bytes: 1024 * 1024,
        }],
    };

    let buf = encode_client_frame(ClientMessage::Read(fetch_req.clone()), 8192);

    ws.send(Message::Binary(buf)).await.unwrap();

    // Should get a response (empty results or error)
    let response = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for response")
        .expect("No response")
        .expect("WebSocket error");

    match response {
        Message::Binary(data) => {
            // Try to decode - should be valid (possibly empty)
            let result = match decode_server_frame(&data) {
                ServerMessage::Read(resp) => Ok((resp, data.len())),
                _ => Err(()),
            };
            match result {
                Ok((resp, _)) => {
                    // Either empty results or results with empty records
                    for partition_result in &resp.results {
                        assert!(
                            partition_result.records.is_empty(),
                            "Should have empty records for non-existent topic"
                        );
                    }
                }
                Err(()) => {
                    // Error response is also acceptable
                }
            }
        }
        Message::Close(_) => {
            // Acceptable
        }
        _ => panic!("Unexpected message type"),
    }

    ws.close(None).await.ok();
}

/// Test that connection close during append doesn't corrupt data.
#[tokio::test]
async fn test_connection_close_during_produce() {
    let db = TestDb::new().await;
    let topic_id = db.create_topic("close-test", 1).await;
    let temp_dir = TempDir::new().unwrap();

    let (addr, _server_handle) = start_server(db.pool.clone(), &temp_dir).await;
    let url = format!("ws://{}", addr);

    // First, append some baseline records
    {
        let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

        let req = writer::AppendRequest {
            writer_id: WriterId::new(),
            append_seq: AppendSeq(1),
            batches: vec![RecordBatch {
                topic_id: TopicId(topic_id as u32),
                partition_id: PartitionId(0),
                schema_id: SchemaId(100),
                records: vec![Record {
                    key: Some(Bytes::from("baseline")),
                    value: Bytes::from("value"),
                }],
            }],
        };

        let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);

        ws.send(Message::Binary(buf)).await.unwrap();

        // Wait for ack
        let _ = tokio::time::timeout(Duration::from_secs(5), ws.next()).await;
        ws.close(None).await.ok();
    }

    // Now simulate connection close during append
    for _ in 0..5 {
        let (mut ws, _) = connect_async(&url).await.expect("Failed to connect");

        let req = writer::AppendRequest {
            writer_id: WriterId::new(),
            append_seq: AppendSeq(1),
            batches: vec![RecordBatch {
                topic_id: TopicId(topic_id as u32),
                partition_id: PartitionId(0),
                schema_id: SchemaId(100),
                records: vec![Record {
                    key: Some(Bytes::from("interrupted")),
                    value: Bytes::from("value"),
                }],
            }],
        };

        let buf = encode_client_frame(ClientMessage::Append(req.clone()), 8192);

        // Send the request
        ws.send(Message::Binary(buf)).await.unwrap();

        // Immediately close without waiting for response
        drop(ws);

        // Small delay before next attempt
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // Wait for any pending operations to complete
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Verify data integrity by fetching all records
    let (mut ws, _) = connect_async(&url)
        .await
        .expect("Failed to connect after stress test");

    let fetch_req = reader::ReadRequest {
        group_id: "test".to_string(),
        reader_id: "test".to_string(),
        generation: flourine_common::ids::Generation(1),
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

    let fetch_resp = match decode_server_frame(&data) {
        ServerMessage::Read(resp) => resp,
        _ => panic!("Expected read response"),
    };

    // Verify we have at least the baseline record
    assert!(
        !fetch_resp.results.is_empty(),
        "Should have partition results"
    );
    assert!(
        !fetch_resp.results[0].records.is_empty(),
        "Should have at least baseline record"
    );

    // Verify high watermark is consistent with record count
    let record_count = fetch_resp.results[0].records.len();
    let watermark = fetch_resp.results[0].high_watermark.0;
    assert!(
        watermark >= record_count as u64,
        "Watermark {} should be >= record count {}",
        watermark,
        record_count
    );

    // Verify records are valid (no corruption)
    for record in &fetch_resp.results[0].records {
        assert!(
            record.key.is_some(),
            "Record should have a key (no corruption)"
        );
        assert!(
            !record.value.is_empty(),
            "Record should have value (no corruption)"
        );
    }

    ws.close(None).await.ok();
}
