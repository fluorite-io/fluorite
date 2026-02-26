//! Jepsen-inspired partial commit tests.
//!
//! Verifies that partial failures across partitions don't create cross-partition
//! inconsistency. When a flush fails for one partition, other partitions that
//! already succeeded remain consistent.

mod common;

use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, connect_async};

use flourine_common::ids::*;
use flourine_common::types::{Record, RecordBatch};
use flourine_wire::{ClientMessage, reader, writer};

use common::ws_helpers;
use common::{CrashableWsBroker, TestDb};

type Ws = tokio_tungstenite::WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

async fn ws_connect(addr: std::net::SocketAddr) -> Ws {
    let (ws, _) = connect_async(format!("ws://{}", addr))
        .await
        .expect("WS connect failed");
    ws
}

async fn ws_produce(
    ws: &mut Ws,
    writer_id: WriterId,
    seq: u64,
    topic_id: TopicId,
    partition_id: PartitionId,
    value: &str,
) -> Result<writer::AppendResponse, String> {
    let req = writer::AppendRequest {
        writer_id,
        append_seq: AppendSeq(seq),
        batches: vec![RecordBatch {
            topic_id,
            partition_id,
            schema_id: SchemaId(100),
            records: vec![Record {
                key: None,
                value: Bytes::from(value.to_string()),
            }],
        }],
    };
    let buf = ws_helpers::encode_client_frame(ClientMessage::Append(req), 8192);
    ws.send(Message::Binary(buf))
        .await
        .map_err(|e| format!("send: {}", e))?;

    let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
        .await
        .map_err(|_| "timeout".to_string())?
        .ok_or("stream closed")?
        .map_err(|e| format!("recv: {}", e))?;

    let data = match msg {
        Message::Binary(d) => d,
        _ => return Err("expected binary".to_string()),
    };
    match ws_helpers::decode_server_frame(&data) {
        flourine_wire::ServerMessage::Append(resp) => Ok(resp),
        other => Err(format!("unexpected: {:?}", other)),
    }
}

async fn ws_read_all(
    ws: &mut Ws,
    topic_id: TopicId,
    partition_id: PartitionId,
) -> Result<(Vec<Bytes>, Offset), String> {
    let mut all_values = Vec::new();
    let mut next_offset = Offset(0);
    let mut hwm = Offset(0);

    loop {
        let req = reader::ReadRequest {
            group_id: String::new(),
            reader_id: String::new(),
            generation: Generation(0),
            reads: vec![reader::PartitionRead {
                topic_id,
                partition_id,
                offset: next_offset,
                max_bytes: 10 * 1024 * 1024,
            }],
        };
        let buf = ws_helpers::encode_client_frame(ClientMessage::Read(req), 8192);
        ws.send(Message::Binary(buf))
            .await
            .map_err(|e| format!("send: {}", e))?;

        let msg = tokio::time::timeout(Duration::from_secs(10), ws.next())
            .await
            .map_err(|_| "timeout".to_string())?
            .ok_or("stream closed")?
            .map_err(|e| format!("recv: {}", e))?;

        let data = match msg {
            Message::Binary(d) => d,
            _ => return Err("expected binary".to_string()),
        };
        let resp = match ws_helpers::decode_server_frame(&data) {
            flourine_wire::ServerMessage::Read(resp) => resp,
            other => return Err(format!("unexpected: {:?}", other)),
        };

        if !resp.success {
            return Err(format!("read failed: {}", resp.error_message));
        }

        let mut got = false;
        for r in &resp.results {
            if r.high_watermark.0 > hwm.0 {
                hwm = r.high_watermark;
            }
            if !r.records.is_empty() {
                got = true;
                next_offset = Offset(next_offset.0 + r.records.len() as u64);
                all_values.extend(r.records.iter().map(|rec| rec.value.clone()));
            }
        }
        if !got || next_offset.0 >= hwm.0 {
            break;
        }
    }
    Ok((all_values, hwm))
}

/// Write to partitions 0, 1, 2 in quick succession. Inject fail_on_put_n(2) so
/// the second flush fails. Verify: partition 0 data is readable if acked,
/// partitions 1/2 writes are not acked, offsets are consistent.
/// Invariant: partial failures across partitions don't create cross-partition inconsistency.
#[tokio::test]
async fn test_multi_partition_partial_commit() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("partial-commit", 3).await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Fail from the 2nd S3 put onward
    broker.faulty_store().fail_on_put_n(2);

    // Write to partition 0 — first put succeeds
    let mut ws0 = ws_connect(addr).await;
    let w0 = WriterId::new();
    let resp0 = ws_produce(&mut ws0, w0, 1, topic_id, PartitionId(0), "p0-val").await;

    // Write to partition 1 — second put fails
    let mut ws1 = ws_connect(addr).await;
    let w1 = WriterId::new();
    let resp1 = ws_produce(&mut ws1, w1, 1, topic_id, PartitionId(1), "p1-val").await;

    // Write to partition 2 — also fails
    let mut ws2 = ws_connect(addr).await;
    let w2 = WriterId::new();
    let resp2 = ws_produce(&mut ws2, w2, 1, topic_id, PartitionId(2), "p2-val").await;

    // Reset faults for reads
    broker.faulty_store().reset();

    // Partition 0 should have succeeded
    let p0_acked = resp0.as_ref().map(|r| r.success).unwrap_or(false);

    // Partitions 1 and 2 should have failed
    let p1_failed = resp1.is_err() || !resp1.as_ref().unwrap().success;
    let p2_failed = resp2.is_err() || !resp2.as_ref().unwrap().success;
    assert!(p1_failed, "partition 1 write should fail (2nd S3 put fails)");
    assert!(p2_failed, "partition 2 write should fail (3rd S3 put fails)");

    // If partition 0 was acked, verify it's readable
    if p0_acked {
        let mut ws = ws_connect(addr).await;
        let (values, hwm) = ws_read_all(&mut ws, topic_id, PartitionId(0))
            .await
            .expect("partition 0 read should succeed");
        assert!(
            values.contains(&Bytes::from("p0-val")),
            "partition 0 acked value should be readable"
        );
        assert_eq!(hwm.0, values.len() as u64, "partition 0 offsets should be consistent");
    }

    // Verify partitions 1 and 2 have no committed data (or empty)
    for p in 1..3 {
        let watermark: i64 = sqlx::query_scalar(
            "SELECT COALESCE(next_offset, 0) FROM partition_offsets \
             WHERE topic_id = $1 AND partition_id = $2",
        )
        .bind(topic_id.0 as i32)
        .bind(p)
        .fetch_optional(&db.pool)
        .await
        .expect("query watermark")
        .unwrap_or(0);
        assert_eq!(
            watermark, 0,
            "partition {} watermark should be 0 (write failed)",
            p
        );
    }

    // New writes to all partitions should succeed after fault reset
    let mut ws = ws_connect(addr).await;
    for p in 0..3u32 {
        let w = WriterId::new();
        let resp = ws_produce(
            &mut ws, w, 1, topic_id, PartitionId(p),
            &format!("recover-p{}", p),
        )
        .await
        .expect("recovery write should succeed");
        assert!(resp.success, "partition {} write should succeed after reset", p);
    }
}

/// Write to multiple partitions. One partition's flush fails.
/// Verify the successful partition's offsets are contiguous and consistent.
#[tokio::test]
async fn test_partial_commit_offset_consistency() {
    let db = TestDb::new().await;
    let topic_id = TopicId(db.create_topic("partial-offset", 2).await as u32);

    let broker = CrashableWsBroker::start(db.pool.clone()).await;
    let addr = broker.addr();

    // Write 5 records to partition 0 (all should succeed)
    let mut ws = ws_connect(addr).await;
    let w0 = WriterId::new();
    for i in 0..5 {
        let resp = ws_produce(
            &mut ws, w0, i + 1, topic_id, PartitionId(0),
            &format!("p0-{}", i),
        )
        .await
        .expect("p0 write should succeed");
        assert!(resp.success);
    }

    // Now fail S3 and write to partition 1
    broker.faulty_store().fail_on_put_n(1);
    let w1 = WriterId::new();
    let resp = ws_produce(&mut ws, w1, 1, topic_id, PartitionId(1), "p1-fail").await;
    let p1_failed = resp.is_err() || !resp.as_ref().unwrap().success;
    assert!(p1_failed, "partition 1 should fail with S3 fault");

    // Reset and verify partition 0 is fully consistent
    broker.faulty_store().reset();
    let mut ws = ws_connect(addr).await;
    let (values, hwm) = ws_read_all(&mut ws, topic_id, PartitionId(0))
        .await
        .expect("p0 read should succeed");
    assert_eq!(values.len(), 5, "partition 0 should have all 5 records");
    assert_eq!(hwm.0, 5, "partition 0 watermark should be 5");

    // Partition 1 failure should not have affected partition 0's consistency
    let unique: std::collections::HashSet<&Bytes> = values.iter().collect();
    assert_eq!(values.len(), unique.len(), "no duplicates in partition 0");
}
