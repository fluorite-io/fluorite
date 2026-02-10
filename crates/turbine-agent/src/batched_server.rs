//! Batched agent server with buffering and deduplication.
//!
//! Extends the basic server with:
//! - Request buffering and merging
//! - Periodic flush to S3
//! - Deduplication via LRU cache
//! - Backpressure handling

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use sqlx::PgPool;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use turbine_common::ids::{Offset, ProducerId, SchemaId, SeqNum};
use turbine_common::types::Segment;
use turbine_wire::{consumer, producer};

use crate::buffer::{AgentBuffer, BufferConfig, SegmentKey};
use crate::dedup::{DedupCache, DedupResult};
use crate::object_store::ObjectStore;
use crate::tbin::{TbinReader, TbinWriter};
use crate::AgentError;

/// Batched agent configuration.
#[derive(Debug, Clone)]
pub struct BatchedAgentConfig {
    /// Address to bind to.
    pub bind_addr: SocketAddr,
    /// S3 bucket for storing TBIN files.
    pub bucket: String,
    /// S3 key prefix for TBIN files.
    pub key_prefix: String,
    /// Buffer configuration.
    pub buffer: BufferConfig,
    /// Flush interval for the buffer.
    pub flush_interval: Duration,
}

impl Default for BatchedAgentConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9000".parse().unwrap(),
            bucket: "turbine".to_string(),
            key_prefix: "data".to_string(),
            buffer: BufferConfig::default(),
            flush_interval: Duration::from_millis(100),
        }
    }
}

/// Message sent to the flush task.
#[allow(dead_code)] // ForceFlush and Shutdown are for graceful shutdown
enum FlushCommand {
    /// Insert a produce request into the buffer.
    Insert {
        producer_id: ProducerId,
        seq: SeqNum,
        segments: Vec<Segment>,
        response_tx: tokio::sync::oneshot::Sender<Vec<turbine_common::types::SegmentAck>>,
    },
    /// Force an immediate flush.
    ForceFlush,
    /// Shutdown the flush task.
    Shutdown,
}

/// Shared state for the batched agent.
pub struct BatchedAgentState<S: ObjectStore> {
    pub pool: PgPool,
    pub store: Arc<S>,
    pub config: BatchedAgentConfig,
    pub dedup_cache: DedupCache,
    /// Channel to send commands to the flush task.
    flush_tx: mpsc::Sender<FlushCommand>,
    /// Flag indicating if backpressure is active.
    backpressure: Arc<RwLock<bool>>,
}

impl<S: ObjectStore + Send + Sync + 'static> BatchedAgentState<S> {
    /// Create new batched agent state and spawn flush task.
    pub async fn new(pool: PgPool, store: S, config: BatchedAgentConfig) -> Arc<Self> {
        let (flush_tx, flush_rx) = mpsc::channel(1000);
        let dedup_cache = DedupCache::new(pool.clone());
        let store = Arc::new(store);
        let backpressure = Arc::new(RwLock::new(false));

        let state = Arc::new(Self {
            pool: pool.clone(),
            store: store.clone(),
            config: config.clone(),
            dedup_cache,
            flush_tx,
            backpressure: backpressure.clone(),
        });

        // Spawn flush task
        let flush_state = state.clone();
        tokio::spawn(async move {
            flush_loop(flush_rx, flush_state).await;
        });

        state
    }

    /// Check if backpressure is active.
    pub async fn is_backpressure_active(&self) -> bool {
        *self.backpressure.read().await
    }
}

/// Run the batched agent server.
pub async fn run<S: ObjectStore + Send + Sync + 'static>(
    state: Arc<BatchedAgentState<S>>,
) -> Result<(), AgentError> {
    let listener = TcpListener::bind(&state.config.bind_addr).await?;
    info!("Batched agent listening on {}", state.config.bind_addr);

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
    state: Arc<BatchedAgentState<S>>,
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

/// Handle a binary message.
async fn handle_message<S: ObjectStore + Send + Sync + 'static>(
    data: &[u8],
    state: &BatchedAgentState<S>,
) -> Vec<u8> {
    // Try to decode as ProduceRequest first
    if let Ok((req, _)) = producer::decode_request(data) {
        return handle_produce_request(req, state).await;
    }

    // Try to decode as FetchRequest
    if let Ok((req, _)) = consumer::decode_fetch_request(data) {
        return handle_fetch_request(req, state).await;
    }

    // Unknown message type
    let mut buf = vec![0u8; 256];
    let error_resp = producer::ProduceResponse {
        seq: SeqNum(0),
        acks: vec![],
    };
    let len = producer::encode_response(&error_resp, &mut buf);
    buf.truncate(len);
    buf
}

/// Handle a ProduceRequest with batching.
async fn handle_produce_request<S: ObjectStore + Send + Sync + 'static>(
    req: producer::ProduceRequest,
    state: &BatchedAgentState<S>,
) -> Vec<u8> {
    // Check backpressure
    if state.is_backpressure_active().await {
        debug!("Backpressure active, rejecting request from {:?}", req.producer_id);
        let resp = producer::ProduceResponse {
            seq: req.seq,
            acks: vec![], // Empty acks signals rate limit
        };
        let mut buf = vec![0u8; 256];
        let len = producer::encode_response(&resp, &mut buf);
        buf.truncate(len);
        return buf;
    }

    // Check for duplicates
    match state.dedup_cache.check(req.producer_id, req.seq).await {
        Ok(DedupResult::Duplicate(acks)) => {
            debug!("Duplicate request from {:?} seq={}", req.producer_id, req.seq.0);
            let resp = producer::ProduceResponse {
                seq: req.seq,
                acks,
            };
            let mut buf = vec![0u8; 8192];
            let len = producer::encode_response(&resp, &mut buf);
            buf.truncate(len);
            return buf;
        }
        Ok(DedupResult::Stale) => {
            debug!("Stale request from {:?} seq={}", req.producer_id, req.seq.0);
            let resp = producer::ProduceResponse {
                seq: req.seq,
                acks: vec![],
            };
            let mut buf = vec![0u8; 256];
            let len = producer::encode_response(&resp, &mut buf);
            buf.truncate(len);
            return buf;
        }
        Ok(DedupResult::Accept) => {
            // Continue processing
        }
        Err(e) => {
            error!("Dedup check error: {}", e);
            // Continue processing on error (fail open)
        }
    }

    // Send to flush task
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();

    if state
        .flush_tx
        .send(FlushCommand::Insert {
            producer_id: req.producer_id,
            seq: req.seq,
            segments: req.segments,
            response_tx,
        })
        .await
        .is_err()
    {
        error!("Failed to send to flush task");
        let resp = producer::ProduceResponse {
            seq: req.seq,
            acks: vec![],
        };
        let mut buf = vec![0u8; 256];
        let len = producer::encode_response(&resp, &mut buf);
        buf.truncate(len);
        return buf;
    }

    // Wait for acks
    match response_rx.await {
        Ok(acks) => {
            // Update dedup cache
            state.dedup_cache.update(req.producer_id, req.seq, acks.clone()).await;

            let resp = producer::ProduceResponse {
                seq: req.seq,
                acks,
            };
            let mut buf = vec![0u8; 8192];
            let len = producer::encode_response(&resp, &mut buf);
            buf.truncate(len);
            buf
        }
        Err(_) => {
            error!("Response channel closed");
            let resp = producer::ProduceResponse {
                seq: req.seq,
                acks: vec![],
            };
            let mut buf = vec![0u8; 256];
            let len = producer::encode_response(&resp, &mut buf);
            buf.truncate(len);
            buf
        }
    }
}

/// Handle a FetchRequest (same as non-batched, reads from S3).
async fn handle_fetch_request<S: ObjectStore + Send + Sync>(
    req: consumer::FetchRequest,
    state: &BatchedAgentState<S>,
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

/// Process a fetch request.
async fn process_fetch<S: ObjectStore + Send + Sync>(
    req: consumer::FetchRequest,
    state: &BatchedAgentState<S>,
) -> Result<Vec<consumer::PartitionResult>, AgentError> {
    let mut results = Vec::with_capacity(req.fetches.len());

    for fetch in &req.fetches {
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
            let data = state.store.get(&s3_key).await?;
            let segment_metas = TbinReader::read_footer(&data)?;

            for seg_meta in &segment_metas {
                if seg_meta.topic_id == fetch.topic_id
                    && seg_meta.partition_id == fetch.partition_id
                {
                    let records = TbinReader::read_segment(&data, seg_meta, true)?;
                    let skip = (fetch.offset.0 as i64 - start_offset).max(0) as usize;
                    all_records.extend(records.into_iter().skip(skip));
                }
            }

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

/// Flush loop that buffers requests and periodically flushes to S3.
async fn flush_loop<S: ObjectStore + Send + Sync + 'static>(
    mut rx: mpsc::Receiver<FlushCommand>,
    state: Arc<BatchedAgentState<S>>,
) {
    let mut buffer = AgentBuffer::with_config(state.config.buffer.clone());
    let mut flush_interval = tokio::time::interval(state.config.flush_interval);

    loop {
        tokio::select! {
            Some(cmd) = rx.recv() => {
                match cmd {
                    FlushCommand::Insert { producer_id, seq, segments, response_tx } => {
                        // Insert into buffer
                        let ack_rx = buffer.insert(producer_id, seq, segments);

                        // Spawn task to forward acks when ready
                        tokio::spawn(async move {
                            if let Ok(acks) = ack_rx.await {
                                let _ = response_tx.send(acks);
                            }
                        });

                        // Update backpressure state
                        if buffer.should_apply_backpressure() {
                            *state.backpressure.write().await = true;
                        }

                        // Check if we should flush
                        if buffer.should_flush() {
                            flush_buffer(&mut buffer, &state).await;
                        }
                    }
                    FlushCommand::ForceFlush => {
                        flush_buffer(&mut buffer, &state).await;
                    }
                    FlushCommand::Shutdown => {
                        // Flush remaining data before shutdown
                        if !buffer.is_empty() {
                            flush_buffer(&mut buffer, &state).await;
                        }
                        break;
                    }
                }
            }
            _ = flush_interval.tick() => {
                // Periodic flush
                if !buffer.is_empty() {
                    flush_buffer(&mut buffer, &state).await;
                }
            }
        }
    }
}

/// Flush the buffer to S3 and commit to database.
async fn flush_buffer<S: ObjectStore + Send + Sync>(
    buffer: &mut AgentBuffer,
    state: &BatchedAgentState<S>,
) {
    let drain_result = buffer.drain();

    if drain_result.segments.is_empty() {
        return;
    }

    debug!(
        "Flushing {} segments, {} pending producers",
        drain_result.segments.len(),
        drain_result.pending_producers.len()
    );

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
    for segment in &drain_result.segments {
        if let Err(e) = writer.add_segment(segment) {
            error!("Failed to add segment to TBIN: {}", e);
            return;
        }
    }
    let tbin_data = writer.finish();

    // Write to S3
    if let Err(e) = state.store.put(&key, tbin_data).await {
        error!("Failed to write to S3: {}", e);
        return;
    }

    // Commit to database
    match commit_batch(&drain_result.segments, &drain_result.key_to_index, &key, &state.pool).await {
        Ok(segment_offsets) => {
            // Distribute acks to producers
            AgentBuffer::distribute_acks(drain_result, &segment_offsets);

            // Release backpressure if below low water mark
            if buffer.can_release_backpressure() {
                *state.backpressure.write().await = false;
            }
        }
        Err(e) => {
            error!("Failed to commit batch: {}", e);
            // Acks won't be sent, producers will retry
        }
    }
}

/// Commit a batch of segments to the database.
async fn commit_batch(
    segments: &[Segment],
    key_to_index: &std::collections::HashMap<SegmentKey, usize>,
    s3_key: &str,
    pool: &PgPool,
) -> Result<Vec<(u64, u64)>, sqlx::Error> {
    let mut tx = pool.begin().await?;
    let ingest_time = chrono::Utc::now();

    let mut segment_offsets = vec![(0u64, 0u64); segments.len()];

    for (key, &seg_idx) in key_to_index {
        let segment = &segments[seg_idx];
        let record_count = segment.records.len() as i64;

        // Get current offset (with lock)
        let current_offset: i64 = sqlx::query_scalar(
            r#"
            SELECT next_offset FROM partition_offsets
            WHERE topic_id = $1 AND partition_id = $2
            FOR UPDATE
            "#,
        )
        .bind(key.topic_id.0 as i32)
        .bind(key.partition_id.0 as i32)
        .fetch_optional(&mut *tx)
        .await?
        .unwrap_or(0);

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
        .bind(key.topic_id.0 as i32)
        .bind(key.partition_id.0 as i32)
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
        .bind(key.topic_id.0 as i32)
        .bind(key.partition_id.0 as i32)
        .bind(key.schema_id.0 as i32)
        .bind(start_offset)
        .bind(end_offset)
        .bind(record_count as i32)
        .bind(s3_key)
        .bind(ingest_time)
        .execute(&mut *tx)
        .await?;

        segment_offsets[seg_idx] = (start_offset as u64, end_offset as u64);
    }

    tx.commit().await?;

    Ok(segment_offsets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batched_agent_config_default() {
        let config = BatchedAgentConfig::default();
        assert_eq!(config.bind_addr.port(), 9000);
        assert_eq!(config.flush_interval, Duration::from_millis(100));
    }
}
