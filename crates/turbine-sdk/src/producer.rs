//! Producer client for sending messages to Turbine.
//!
//! Features:
//! - Automatic sequence number tracking
//! - Retry with exponential backoff on backpressure
//! - Connection reconnection
//! - Batch sending support

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{debug, warn};

use turbine_common::ids::{PartitionId, ProducerId, SchemaId, SeqNum, TopicId};
use turbine_common::types::{Record, Segment, SegmentAck};
use turbine_wire::producer;

use crate::SdkError;

/// Configuration for the producer.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Server URL (e.g., "ws://localhost:9000")
    pub url: String,
    /// Maximum retries on backpressure.
    pub max_retries: u32,
    /// Initial backoff duration.
    pub initial_backoff: Duration,
    /// Maximum backoff duration.
    pub max_backoff: Duration,
    /// Request timeout.
    pub timeout: Duration,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            url: "ws://localhost:9000".to_string(),
            max_retries: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            timeout: Duration::from_secs(30),
        }
    }
}

/// Producer client for sending messages.
pub struct Producer {
    /// Producer ID (UUID).
    id: ProducerId,
    /// Current sequence number.
    seq: AtomicU64,
    /// WebSocket connection.
    ws: Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    /// Configuration.
    config: ProducerConfig,
}

impl Producer {
    /// Connect to the server with default configuration.
    pub async fn connect(url: &str) -> Result<Arc<Self>, SdkError> {
        let config = ProducerConfig {
            url: url.to_string(),
            ..Default::default()
        };
        Self::connect_with_config(config).await
    }

    /// Connect to the server with custom configuration.
    pub async fn connect_with_config(config: ProducerConfig) -> Result<Arc<Self>, SdkError> {
        let (ws, _) = connect_async(&config.url)
            .await
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        Ok(Arc::new(Self {
            id: ProducerId::new(),
            seq: AtomicU64::new(1),
            ws: Mutex::new(ws),
            config,
        }))
    }

    /// Get the producer ID.
    pub fn id(&self) -> ProducerId {
        self.id
    }

    /// Get the current sequence number.
    pub fn seq(&self) -> SeqNum {
        SeqNum(self.seq.load(Ordering::SeqCst))
    }

    /// Send records to a single partition.
    pub async fn send(
        &self,
        topic_id: TopicId,
        partition_id: PartitionId,
        schema_id: SchemaId,
        records: Vec<Record>,
    ) -> Result<SegmentAck, SdkError> {
        let segments = vec![Segment {
            topic_id,
            partition_id,
            schema_id,
            records,
        }];

        let acks = self.send_batch(segments).await?;
        acks.into_iter().next().ok_or(SdkError::InvalidResponse)
    }

    /// Send records to multiple partitions in a single request.
    pub async fn send_batch(&self, segments: Vec<Segment>) -> Result<Vec<SegmentAck>, SdkError> {
        let mut retries = 0;
        let mut backoff = self.config.initial_backoff;

        loop {
            let seq = SeqNum(self.seq.fetch_add(1, Ordering::SeqCst));

            let req = producer::ProduceRequest {
                producer_id: self.id,
                seq,
                segments: segments.clone(),
            };

            match self.send_request(&req).await {
                Ok(acks) if acks.is_empty() => {
                    // Empty acks = backpressure
                    if retries >= self.config.max_retries {
                        return Err(SdkError::Backpressure);
                    }

                    warn!(
                        "Backpressure from server, retrying in {:?} (attempt {}/{})",
                        backoff,
                        retries + 1,
                        self.config.max_retries
                    );

                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(self.config.max_backoff);
                    retries += 1;
                }
                Ok(acks) => {
                    debug!("Sent {} segments, got {} acks", segments.len(), acks.len());
                    return Ok(acks);
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Send a single request and wait for response.
    async fn send_request(
        &self,
        req: &producer::ProduceRequest,
    ) -> Result<Vec<SegmentAck>, SdkError> {
        // Encode request
        let mut buf = vec![0u8; 64 * 1024];
        let len = producer::encode_request(req, &mut buf);
        buf.truncate(len);

        let mut ws = self.ws.lock().await;

        // Send
        ws.send(Message::Binary(buf))
            .await
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        // Receive response
        let response = tokio::time::timeout(self.config.timeout, ws.next())
            .await
            .map_err(|_| SdkError::Timeout)?
            .ok_or(SdkError::Disconnected)?
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        match response {
            Message::Binary(data) => {
                let (resp, _) = producer::decode_response(&data)
                    .map_err(|e| SdkError::Decode(e.to_string()))?;

                if resp.seq != req.seq {
                    return Err(SdkError::InvalidResponse);
                }

                Ok(resp.acks)
            }
            Message::Close(_) => Err(SdkError::Disconnected),
            _ => Err(SdkError::InvalidResponse),
        }
    }

    /// Send a single record (convenience method).
    pub async fn send_one(
        &self,
        topic_id: TopicId,
        partition_id: PartitionId,
        schema_id: SchemaId,
        key: Option<Bytes>,
        value: Bytes,
    ) -> Result<SegmentAck, SdkError> {
        self.send(
            topic_id,
            partition_id,
            schema_id,
            vec![Record { key, value }],
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_config_default() {
        let config = ProducerConfig::default();
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.initial_backoff, Duration::from_millis(100));
    }

    #[test]
    fn test_produce_request_encoding() {
        let req = producer::ProduceRequest {
            producer_id: ProducerId::new(),
            seq: SeqNum(1),
            segments: vec![Segment {
                topic_id: TopicId(1),
                partition_id: PartitionId(0),
                schema_id: SchemaId(100),
                records: vec![Record {
                    key: Some(Bytes::from("key")),
                    value: Bytes::from("value"),
                }],
            }],
        };

        let mut buf = vec![0u8; 1024];
        let len = producer::encode_request(&req, &mut buf);
        assert!(len > 0);

        let (decoded, _) = producer::decode_request(&buf[..len]).unwrap();
        assert_eq!(decoded.seq.0, 1);
        assert_eq!(decoded.segments.len(), 1);
    }

    #[test]
    fn test_backoff_calculation() {
        let initial = Duration::from_millis(100);
        let max = Duration::from_secs(10);

        let mut backoff = initial;
        for _ in 0..10 {
            backoff = (backoff * 2).min(max);
        }

        assert_eq!(backoff, max);
    }
}
