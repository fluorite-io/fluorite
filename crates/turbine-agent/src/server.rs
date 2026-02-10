//! WebSocket server for handling produce/fetch requests.

use std::net::SocketAddr;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use sqlx::PgPool;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{error, info, warn};

use turbine_common::ids::{Generation, Offset, SchemaId, TopicId};
use turbine_wire::{consumer, peek_message_type, producer, MessageType};

use crate::coordinator::{Coordinator, CoordinatorConfig};
use crate::object_store::ObjectStore;
use crate::tbin::{TbinReader, TbinWriter};
use crate::AgentError;

/// Agent server configuration.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    /// Address to bind to.
    pub bind_addr: SocketAddr,
    /// S3 bucket for storing TBIN files.
    pub bucket: String,
    /// S3 key prefix for TBIN files.
    pub key_prefix: String,
}

/// Agent server state shared across connections.
pub struct AgentState<S: ObjectStore> {
    pub pool: PgPool,
    pub store: Arc<S>,
    pub config: AgentConfig,
    pub coordinator: Coordinator,
}

impl<S: ObjectStore> AgentState<S> {
    pub fn new(pool: PgPool, store: S, config: AgentConfig) -> Self {
        let coordinator = Coordinator::new(pool.clone(), CoordinatorConfig::default());
        Self {
            pool,
            store: Arc::new(store),
            config,
            coordinator,
        }
    }

    /// Create with custom coordinator config.
    pub fn with_coordinator_config(
        pool: PgPool,
        store: S,
        config: AgentConfig,
        coordinator_config: CoordinatorConfig,
    ) -> Self {
        let coordinator = Coordinator::new(pool.clone(), coordinator_config);
        Self {
            pool,
            store: Arc::new(store),
            config,
            coordinator,
        }
    }
}

/// Run the agent server.
pub async fn run<S: ObjectStore + Send + Sync + 'static>(
    state: Arc<AgentState<S>>,
) -> Result<(), AgentError> {
    let listener = TcpListener::bind(&state.config.bind_addr).await?;
    info!("Agent listening on {}", state.config.bind_addr);

    loop {
        let (stream, addr) = listener.accept().await?;
        let state = state.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_connection(stream, addr, state).await {
                error!("Connection error from {}: {}", addr, e);
            }
        });
    }
}

/// Handle a single WebSocket connection.
async fn handle_connection<S: ObjectStore + Send + Sync + 'static>(
    stream: TcpStream,
    addr: SocketAddr,
    state: Arc<AgentState<S>>,
) -> Result<(), AgentError> {
    let ws_stream = accept_async(stream).await?;
    info!("New WebSocket connection from {}", addr);

    let (mut write, mut read) = ws_stream.split();

    while let Some(msg) = read.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                warn!("WebSocket error from {}: {}", addr, e);
                break;
            }
        };

        match msg {
            Message::Binary(data) => {
                let response = handle_message(&data, &state).await;
                if let Err(e) = write.send(Message::Binary(response)).await {
                    warn!("Failed to send response to {}: {}", addr, e);
                    break;
                }
            }
            Message::Ping(data) => {
                if write.send(Message::Pong(data)).await.is_err() {
                    break;
                }
            }
            Message::Close(_) => {
                info!("Connection closed by {}", addr);
                break;
            }
            _ => {}
        }
    }

    Ok(())
}

/// Handle a binary message and return the response.
async fn handle_message<S: ObjectStore + Send + Sync>(
    data: &[u8],
    state: &AgentState<S>,
) -> Vec<u8> {
    // Peek message type
    let msg_type = match peek_message_type(data) {
        Ok(t) => t,
        Err(_) => {
            // Legacy path: try without message type prefix for backwards compatibility
            if let Ok((req, _)) = producer::decode_request(data) {
                return handle_produce_request(req, state).await;
            }
            if let Ok((req, _)) = consumer::decode_fetch_request(data) {
                return handle_fetch_request(req, state).await;
            }
            return encode_error_response();
        }
    };

    // Skip the message type byte
    let payload = &data[1..];

    match msg_type {
        MessageType::Produce => {
            match producer::decode_request(payload) {
                Ok((req, _)) => handle_produce_request(req, state).await,
                Err(_) => encode_error_response(),
            }
        }
        MessageType::Fetch => {
            match consumer::decode_fetch_request(payload) {
                Ok((req, _)) => handle_fetch_request(req, state).await,
                Err(_) => encode_error_response(),
            }
        }
        MessageType::JoinGroup => {
            match consumer::decode_join_request(payload) {
                Ok((req, _)) => handle_join_group(req, state).await,
                Err(_) => encode_error_response(),
            }
        }
        MessageType::Heartbeat => {
            match consumer::decode_heartbeat_request(payload) {
                Ok((req, _)) => handle_heartbeat(req, state).await,
                Err(_) => encode_error_response(),
            }
        }
        MessageType::Rejoin => {
            match consumer::decode_rejoin_request(payload) {
                Ok((req, _)) => handle_rejoin(req, state).await,
                Err(_) => encode_error_response(),
            }
        }
        MessageType::LeaveGroup => {
            match consumer::decode_leave_request(payload) {
                Ok((req, _)) => handle_leave_group(req, state).await,
                Err(_) => encode_error_response(),
            }
        }
        MessageType::Commit => {
            match consumer::decode_commit_request(payload) {
                Ok((req, _)) => handle_commit(req, state).await,
                Err(_) => encode_error_response(),
            }
        }
        _ => encode_error_response(),
    }
}

fn encode_error_response() -> Vec<u8> {
    let mut buf = vec![0u8; 256];
    let error_resp = producer::ProduceResponse {
        seq: turbine_common::ids::SeqNum(0),
        acks: vec![],
    };
    let len = producer::encode_response(&error_resp, &mut buf);
    buf.truncate(len);
    buf
}

/// Handle a ProduceRequest.
async fn handle_produce_request<S: ObjectStore + Send + Sync>(
    req: producer::ProduceRequest,
    state: &AgentState<S>,
) -> Vec<u8> {
    let result = process_produce(req.clone(), state).await;

    let response = match result {
        Ok(acks) => producer::ProduceResponse {
            seq: req.seq,
            acks,
        },
        Err(e) => {
            error!("Produce error: {}", e);
            producer::ProduceResponse {
                seq: req.seq,
                acks: vec![],
            }
        }
    };

    let mut buf = vec![0u8; 8192];
    let len = producer::encode_response(&response, &mut buf);
    buf.truncate(len);
    buf
}

/// Process a produce request: write to S3, update DB.
async fn process_produce<S: ObjectStore + Send + Sync>(
    req: producer::ProduceRequest,
    state: &AgentState<S>,
) -> Result<Vec<turbine_common::types::SegmentAck>, AgentError> {
    if req.segments.is_empty() {
        return Ok(vec![]);
    }

    // Generate S3 key
    let timestamp = chrono::Utc::now().timestamp_millis();
    let key = format!(
        "{}/{}/{}.tbin",
        state.config.key_prefix,
        chrono::Utc::now().format("%Y-%m-%d"),
        timestamp
    );

    // Build TBIN file
    let mut writer = TbinWriter::new();
    for segment in &req.segments {
        writer.add_segment(segment)?;
    }
    let tbin_data = writer.finish();

    // Write to S3
    state.store.put(&key, tbin_data).await?;

    // Start transaction
    let mut tx = state.pool.begin().await?;

    let mut acks = Vec::with_capacity(req.segments.len());
    let ingest_time = chrono::Utc::now();

    for segment in &req.segments {
        // Get current offset for partition (with lock)
        let current_offset: i64 = sqlx::query_scalar(
            r#"
            SELECT next_offset FROM partition_offsets
            WHERE topic_id = $1 AND partition_id = $2
            FOR UPDATE
            "#,
        )
        .bind(segment.topic_id.0 as i32)
        .bind(segment.partition_id.0 as i32)
        .fetch_optional(&mut *tx)
        .await?
        .unwrap_or(0);

        let record_count = segment.records.len() as i64;
        let start_offset = current_offset;
        let end_offset = current_offset + record_count;

        // Update partition offset
        sqlx::query(
            r#"
            INSERT INTO partition_offsets (topic_id, partition_id, next_offset)
            VALUES ($1, $2, $3)
            ON CONFLICT (topic_id, partition_id)
            DO UPDATE SET next_offset = $3
            "#,
        )
        .bind(segment.topic_id.0 as i32)
        .bind(segment.partition_id.0 as i32)
        .bind(end_offset)
        .execute(&mut *tx)
        .await?;

        // Insert into topic_batches
        sqlx::query(
            r#"
            INSERT INTO topic_batches (
                topic_id, partition_id, schema_id,
                start_offset, end_offset, record_count,
                s3_key, ingest_time
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(segment.topic_id.0 as i32)
        .bind(segment.partition_id.0 as i32)
        .bind(segment.schema_id.0 as i32)
        .bind(start_offset)
        .bind(end_offset)
        .bind(record_count as i32)
        .bind(&key)
        .bind(ingest_time)
        .execute(&mut *tx)
        .await?;

        acks.push(turbine_common::types::SegmentAck {
            topic_id: segment.topic_id,
            partition_id: segment.partition_id,
            schema_id: segment.schema_id,
            start_offset: Offset(start_offset as u64),
            end_offset: Offset(end_offset as u64),
        });
    }

    // Update producer state for deduplication
    sqlx::query(
        r#"
        INSERT INTO producer_state (producer_id, last_seq_num, updated_at)
        VALUES ($1, $2, NOW())
        ON CONFLICT (producer_id)
        DO UPDATE SET last_seq_num = $2, updated_at = NOW()
        "#,
    )
    .bind(req.producer_id.0)
    .bind(req.seq.0 as i64)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok(acks)
}

/// Handle a FetchRequest.
async fn handle_fetch_request<S: ObjectStore + Send + Sync>(
    req: consumer::FetchRequest,
    state: &AgentState<S>,
) -> Vec<u8> {
    let result = process_fetch(req, state).await;

    let response = match result {
        Ok(results) => consumer::FetchResponse { results },
        Err(e) => {
            error!("Fetch error: {}", e);
            consumer::FetchResponse { results: vec![] }
        }
    };

    let mut buf = vec![0u8; 64 * 1024];
    let len = consumer::encode_fetch_response(&response, &mut buf);
    buf.truncate(len);
    buf
}

/// Process a fetch request: query DB, read from S3.
async fn process_fetch<S: ObjectStore + Send + Sync>(
    req: consumer::FetchRequest,
    state: &AgentState<S>,
) -> Result<Vec<consumer::PartitionResult>, AgentError> {
    let mut results = Vec::with_capacity(req.fetches.len());

    for fetch in &req.fetches {
        // Query topic_batches for segments covering the requested offset
        let batches: Vec<(i32, i64, i64, String)> = sqlx::query_as(
            r#"
            SELECT schema_id, start_offset, end_offset, s3_key
            FROM topic_batches
            WHERE topic_id = $1
              AND partition_id = $2
              AND start_offset <= $3
              AND end_offset > $3
            ORDER BY start_offset
            LIMIT 10
            "#,
        )
        .bind(fetch.topic_id.0 as i32)
        .bind(fetch.partition_id.0 as i32)
        .bind(fetch.offset.0 as i64)
        .fetch_all(&state.pool)
        .await?;

        // Get high watermark
        let high_watermark: i64 = sqlx::query_scalar(
            "SELECT COALESCE(next_offset, 0) FROM partition_offsets WHERE topic_id = $1 AND partition_id = $2",
        )
        .bind(fetch.topic_id.0 as i32)
        .bind(fetch.partition_id.0 as i32)
        .fetch_optional(&state.pool)
        .await?
        .unwrap_or(0);

        let mut all_records = vec![];
        let mut schema_id = SchemaId(0);

        for (sid, start_offset, _end_offset, s3_key) in batches {
            schema_id = SchemaId(sid as u32);

            // Read TBIN file from S3
            let data = state.store.get(&s3_key).await?;

            // Parse footer to get segment metadata
            let segment_metas = TbinReader::read_footer(&data)?;

            // Find matching segment and read records
            for seg_meta in &segment_metas {
                if seg_meta.topic_id == fetch.topic_id
                    && seg_meta.partition_id == fetch.partition_id
                {
                    let records = TbinReader::read_segment(&data, seg_meta, true)?;
                    // Skip records before requested offset
                    let skip = (fetch.offset.0 as i64 - start_offset).max(0) as usize;
                    all_records.extend(records.into_iter().skip(skip));
                }
            }

            // Respect max_bytes limit
            let total_bytes: usize = all_records.iter().map(|r| r.value.len()).sum();
            if total_bytes >= fetch.max_bytes as usize {
                break;
            }
        }

        results.push(consumer::PartitionResult {
            topic_id: fetch.topic_id,
            partition_id: fetch.partition_id,
            schema_id,
            high_watermark: Offset(high_watermark as u64),
            records: all_records,
        });
    }

    Ok(results)
}

// ============ Consumer Group Handlers ============

/// Handle a JoinGroupRequest.
async fn handle_join_group<S: ObjectStore + Send + Sync>(
    req: consumer::JoinGroupRequest,
    state: &AgentState<S>,
) -> Vec<u8> {
    // For now, we only support single topic joins
    let topic_id = req.topic_ids.first().copied().unwrap_or(TopicId(0));

    let result = state
        .coordinator
        .join_group(&req.group_id, topic_id, &req.consumer_id)
        .await;

    let response = match result {
        Ok(join_result) => consumer::JoinGroupResponse {
            generation: join_result.generation,
            assignments: join_result
                .assignments
                .into_iter()
                .map(|a| consumer::PartitionAssignment {
                    topic_id,
                    partition_id: a.partition_id,
                    committed_offset: a.committed_offset,
                })
                .collect(),
        },
        Err(e) => {
            error!("JoinGroup error: {}", e);
            consumer::JoinGroupResponse {
                generation: Generation(0),
                assignments: vec![],
            }
        }
    };

    let mut buf = vec![0u8; 8192];
    buf[0] = MessageType::JoinGroupResponse.to_byte();
    let len = consumer::encode_join_response(&response, &mut buf[1..]);
    buf.truncate(len + 1);
    buf
}

/// Handle a HeartbeatRequest.
async fn handle_heartbeat<S: ObjectStore + Send + Sync>(
    req: consumer::HeartbeatRequest,
    state: &AgentState<S>,
) -> Vec<u8> {
    // HeartbeatRequest doesn't have topic_id, so we need to look it up
    // For now, use generation to find the group (or we could add topic_id to the request)
    // We'll use a simplified approach - heartbeat only checks membership

    let result = state
        .coordinator
        .heartbeat(&req.group_id, req.topic_id, &req.consumer_id, req.generation)
        .await;

    let response = match result {
        Ok(hb_result) => consumer::HeartbeatResponseExt {
            generation: hb_result.generation,
            status: match hb_result.status {
                crate::coordinator::HeartbeatStatus::Ok => consumer::HeartbeatStatus::Ok,
                crate::coordinator::HeartbeatStatus::RebalanceNeeded => {
                    consumer::HeartbeatStatus::RebalanceNeeded
                }
                crate::coordinator::HeartbeatStatus::UnknownMember => {
                    consumer::HeartbeatStatus::UnknownMember
                }
            },
        },
        Err(e) => {
            error!("Heartbeat error: {}", e);
            consumer::HeartbeatResponseExt {
                generation: Generation(0),
                status: consumer::HeartbeatStatus::UnknownMember,
            }
        }
    };

    let mut buf = vec![0u8; 64];
    buf[0] = MessageType::HeartbeatResponse.to_byte();
    let len = consumer::encode_heartbeat_response_ext(&response, &mut buf[1..]);
    buf.truncate(len + 1);
    buf
}

/// Handle a RejoinRequest.
async fn handle_rejoin<S: ObjectStore + Send + Sync>(
    req: consumer::RejoinRequest,
    state: &AgentState<S>,
) -> Vec<u8> {
    let result = state
        .coordinator
        .rejoin(&req.group_id, req.topic_id, &req.consumer_id, req.generation)
        .await;

    let response = match result {
        Ok(rejoin_result) => consumer::RejoinResponse {
            generation: rejoin_result.generation,
            status: match rejoin_result.status {
                crate::coordinator::RejoinStatus::Ok => consumer::RejoinStatus::Ok,
                crate::coordinator::RejoinStatus::RebalanceNeeded => {
                    consumer::RejoinStatus::RebalanceNeeded
                }
            },
            assignments: rejoin_result
                .assignments
                .into_iter()
                .map(|a| consumer::PartitionAssignment {
                    topic_id: req.topic_id,
                    partition_id: a.partition_id,
                    committed_offset: a.committed_offset,
                })
                .collect(),
        },
        Err(e) => {
            error!("Rejoin error: {}", e);
            consumer::RejoinResponse {
                generation: Generation(0),
                status: consumer::RejoinStatus::RebalanceNeeded,
                assignments: vec![],
            }
        }
    };

    let mut buf = vec![0u8; 8192];
    buf[0] = MessageType::RejoinResponse.to_byte();
    let len = consumer::encode_rejoin_response(&response, &mut buf[1..]);
    buf.truncate(len + 1);
    buf
}

/// Handle a LeaveGroupRequest.
async fn handle_leave_group<S: ObjectStore + Send + Sync>(
    req: consumer::LeaveGroupRequest,
    state: &AgentState<S>,
) -> Vec<u8> {
    let result = state
        .coordinator
        .leave_group(&req.group_id, req.topic_id, &req.consumer_id)
        .await;

    let response = consumer::LeaveGroupResponse {
        success: result.is_ok(),
    };

    if let Err(e) = result {
        error!("LeaveGroup error: {}", e);
    }

    let mut buf = vec![0u8; 16];
    buf[0] = MessageType::LeaveGroupResponse.to_byte();
    let len = consumer::encode_leave_response(&response, &mut buf[1..]);
    buf.truncate(len + 1);
    buf
}

/// Handle a CommitRequest.
async fn handle_commit<S: ObjectStore + Send + Sync>(
    req: consumer::CommitRequest,
    state: &AgentState<S>,
) -> Vec<u8> {
    let mut all_ok = true;

    for commit in &req.commits {
        let result = state
            .coordinator
            .commit_offset(
                &req.group_id,
                commit.topic_id,
                &req.consumer_id,
                commit.partition_id,
                commit.offset,
            )
            .await;

        match result {
            Ok(crate::coordinator::CommitStatus::Ok) => {}
            Ok(crate::coordinator::CommitStatus::NotOwner) => {
                warn!(
                    "Commit rejected: consumer {} doesn't own partition {}",
                    req.consumer_id, commit.partition_id.0
                );
                all_ok = false;
            }
            Err(e) => {
                error!("Commit error: {}", e);
                all_ok = false;
            }
        }
    }

    let response = consumer::CommitResponse { success: all_ok };

    let mut buf = vec![0u8; 16];
    buf[0] = MessageType::CommitResponse.to_byte();
    let len = consumer::encode_commit_response(&response, &mut buf[1..]);
    buf.truncate(len + 1);
    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use turbine_common::ids::{PartitionId, ProducerId, SchemaId, SeqNum, TopicId};
    use turbine_common::types::{Record, Segment};

    #[test]
    fn test_agent_config() {
        let config = AgentConfig {
            bind_addr: "127.0.0.1:9000".parse().unwrap(),
            bucket: "test-bucket".to_string(),
            key_prefix: "data".to_string(),
        };
        assert_eq!(config.bind_addr.port(), 9000);
    }

    #[test]
    fn test_produce_request_wire_roundtrip() {
        // Create a produce request
        let req = producer::ProduceRequest {
            producer_id: ProducerId(uuid::Uuid::new_v4()),
            seq: SeqNum(42),
            segments: vec![Segment {
                topic_id: TopicId(1),
                partition_id: PartitionId(0),
                schema_id: SchemaId(100),
                records: vec![
                    Record {
                        key: Some(Bytes::from("key1")),
                        value: Bytes::from("value1"),
                    },
                    Record {
                        key: None,
                        value: Bytes::from("value2"),
                    },
                ],
            }],
        };

        // Encode
        let mut buf = vec![0u8; 8192];
        let len = producer::encode_request(&req, &mut buf);

        // Decode
        let (decoded, decoded_len) = producer::decode_request(&buf[..len]).unwrap();

        assert_eq!(decoded_len, len);
        assert_eq!(decoded.seq.0, req.seq.0);
        assert_eq!(decoded.segments.len(), 1);
        assert_eq!(decoded.segments[0].records.len(), 2);
    }

    #[test]
    fn test_tbin_produce_flow() {
        // Simulate the produce flow: create segments -> write TBIN -> read back

        let segments = vec![
            Segment {
                topic_id: TopicId(1),
                partition_id: PartitionId(0),
                schema_id: SchemaId(100),
                records: vec![
                    Record {
                        key: Some(Bytes::from("user-1")),
                        value: Bytes::from(r#"{"name":"Alice","age":30}"#),
                    },
                    Record {
                        key: Some(Bytes::from("user-2")),
                        value: Bytes::from(r#"{"name":"Bob","age":25}"#),
                    },
                ],
            },
            Segment {
                topic_id: TopicId(1),
                partition_id: PartitionId(1),
                schema_id: SchemaId(100),
                records: vec![Record {
                    key: Some(Bytes::from("user-3")),
                    value: Bytes::from(r#"{"name":"Charlie","age":35}"#),
                }],
            },
        ];

        // Write TBIN
        let mut writer = TbinWriter::new();
        for segment in &segments {
            writer.add_segment(segment).unwrap();
        }
        let tbin_data = writer.finish();

        // Read back
        let metas = TbinReader::read_footer(&tbin_data).unwrap();
        assert_eq!(metas.len(), 2);

        // Verify first segment
        let records0 = TbinReader::read_segment(&tbin_data, &metas[0], true).unwrap();
        assert_eq!(records0.len(), 2);
        assert_eq!(records0[0].key, Some(Bytes::from("user-1")));
        assert_eq!(records0[1].key, Some(Bytes::from("user-2")));

        // Verify second segment
        let records1 = TbinReader::read_segment(&tbin_data, &metas[1], true).unwrap();
        assert_eq!(records1.len(), 1);
        assert_eq!(records1[0].key, Some(Bytes::from("user-3")));
    }

    #[test]
    fn test_fetch_response_wire_roundtrip() {
        // Create a fetch response
        let resp = consumer::FetchResponse {
            results: vec![consumer::PartitionResult {
                topic_id: TopicId(1),
                partition_id: PartitionId(0),
                schema_id: SchemaId(100),
                high_watermark: Offset(1000),
                records: vec![
                    Record {
                        key: Some(Bytes::from("key1")),
                        value: Bytes::from("value1"),
                    },
                    Record {
                        key: None,
                        value: Bytes::from("value2"),
                    },
                ],
            }],
        };

        // Encode
        let mut buf = vec![0u8; 8192];
        let len = consumer::encode_fetch_response(&resp, &mut buf);

        // Decode
        let (decoded, decoded_len) = consumer::decode_fetch_response(&buf[..len]).unwrap();

        assert_eq!(decoded_len, len);
        assert_eq!(decoded.results.len(), 1);
        assert_eq!(decoded.results[0].high_watermark.0, 1000);
        assert_eq!(decoded.results[0].records.len(), 2);
    }

    #[test]
    fn test_end_to_end_data_flow() {
        // This test simulates the complete data flow without a database:
        // 1. Producer sends records
        // 2. Agent writes to TBIN
        // 3. Agent reads from TBIN
        // 4. Consumer receives records

        // Step 1: Create produce request with 100 records
        let producer_id = ProducerId(uuid::Uuid::new_v4());
        let mut records = Vec::with_capacity(100);
        for i in 0..100 {
            records.push(Record {
                key: Some(Bytes::from(format!("key-{:03}", i))),
                value: Bytes::from(format!(r#"{{"id":{},"data":"test-{}"}}"#, i, i)),
            });
        }

        let req = producer::ProduceRequest {
            producer_id,
            seq: SeqNum(1),
            segments: vec![Segment {
                topic_id: TopicId(1),
                partition_id: PartitionId(0),
                schema_id: SchemaId(100),
                records: records.clone(),
            }],
        };

        // Step 2: Encode produce request (simulates network)
        let mut wire_buf = vec![0u8; 64 * 1024];
        let wire_len = producer::encode_request(&req, &mut wire_buf);
        let (decoded_req, _) = producer::decode_request(&wire_buf[..wire_len]).unwrap();

        // Step 3: Write to TBIN (simulates S3 write)
        let mut writer = TbinWriter::new();
        for segment in &decoded_req.segments {
            writer.add_segment(segment).unwrap();
        }
        let tbin_data = writer.finish();

        // Step 4: Read from TBIN (simulates S3 read)
        let metas = TbinReader::read_footer(&tbin_data).unwrap();
        assert_eq!(metas.len(), 1);

        let fetched_records = TbinReader::read_segment(&tbin_data, &metas[0], true).unwrap();
        assert_eq!(fetched_records.len(), 100);

        // Step 5: Build fetch response
        let fetch_resp = consumer::FetchResponse {
            results: vec![consumer::PartitionResult {
                topic_id: TopicId(1),
                partition_id: PartitionId(0),
                schema_id: SchemaId(100),
                high_watermark: Offset(100),
                records: fetched_records,
            }],
        };

        // Step 6: Encode fetch response (simulates network)
        let mut resp_buf = vec![0u8; 64 * 1024];
        let resp_len = consumer::encode_fetch_response(&fetch_resp, &mut resp_buf);
        let (decoded_resp, _) = consumer::decode_fetch_response(&resp_buf[..resp_len]).unwrap();

        // Step 7: Verify data integrity
        assert_eq!(decoded_resp.results.len(), 1);
        assert_eq!(decoded_resp.results[0].records.len(), 100);

        // Verify first and last records
        assert_eq!(
            decoded_resp.results[0].records[0].key,
            Some(Bytes::from("key-000"))
        );
        assert_eq!(
            decoded_resp.results[0].records[99].key,
            Some(Bytes::from("key-099"))
        );

        // Verify values match
        for (i, record) in decoded_resp.results[0].records.iter().enumerate() {
            let expected_value = format!(r#"{{"id":{},"data":"test-{}"}}"#, i, i);
            assert_eq!(record.value, Bytes::from(expected_value));
        }
    }
}
