//! Consumer client for fetching messages from Turbine.
//!
//! Features:
//! - Offset tracking per partition
//! - Batch fetching with max_bytes limit
//! - Consumer group support (group_id, consumer_id)
//! - Automatic offset advancement

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::debug;

use turbine_common::ids::{Generation, Offset, PartitionId, SchemaId, TopicId};
use turbine_common::types::Record;
use turbine_wire::consumer;

use crate::SdkError;

/// Configuration for the consumer.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Server URL (e.g., "ws://localhost:9000")
    pub url: String,
    /// Consumer group ID.
    pub group_id: String,
    /// Consumer ID within the group.
    pub consumer_id: String,
    /// Default max bytes per fetch.
    pub max_bytes: u32,
    /// Request timeout.
    pub timeout: Duration,
    /// Whether to auto-commit offsets.
    pub auto_commit: bool,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            url: "ws://localhost:9000".to_string(),
            group_id: "default".to_string(),
            consumer_id: uuid::Uuid::new_v4().to_string(),
            max_bytes: 1024 * 1024, // 1 MB
            timeout: Duration::from_secs(30),
            auto_commit: true,
        }
    }
}

/// Partition key for offset tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PartitionKey {
    topic_id: TopicId,
    partition_id: PartitionId,
}

/// Result of a fetch operation.
#[derive(Debug)]
pub struct FetchResult {
    /// Topic ID.
    pub topic_id: TopicId,
    /// Partition ID.
    pub partition_id: PartitionId,
    /// Schema ID of the records.
    pub schema_id: SchemaId,
    /// High watermark (latest offset in partition).
    pub high_watermark: Offset,
    /// Fetched records.
    pub records: Vec<Record>,
}

/// Consumer client for fetching messages.
pub struct Consumer {
    /// Configuration.
    config: ConsumerConfig,
    /// WebSocket connection.
    ws: Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    /// Current generation (for consumer group).
    generation: Mutex<Generation>,
    /// Committed offsets per partition.
    offsets: Mutex<HashMap<PartitionKey, Offset>>,
}

impl Consumer {
    /// Connect to the server.
    pub async fn connect(url: &str, group_id: &str, consumer_id: &str) -> Result<Arc<Self>, SdkError> {
        let config = ConsumerConfig {
            url: url.to_string(),
            group_id: group_id.to_string(),
            consumer_id: consumer_id.to_string(),
            ..Default::default()
        };
        Self::connect_with_config(config).await
    }

    /// Connect with custom configuration.
    pub async fn connect_with_config(config: ConsumerConfig) -> Result<Arc<Self>, SdkError> {
        let (ws, _) = connect_async(&config.url)
            .await
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        Ok(Arc::new(Self {
            config,
            ws: Mutex::new(ws),
            generation: Mutex::new(Generation(0)),
            offsets: Mutex::new(HashMap::new()),
        }))
    }

    /// Get the consumer ID.
    pub fn consumer_id(&self) -> &str {
        &self.config.consumer_id
    }

    /// Get the group ID.
    pub fn group_id(&self) -> &str {
        &self.config.group_id
    }

    /// Fetch records from a single partition.
    pub async fn fetch(
        &self,
        topic_id: TopicId,
        partition_id: PartitionId,
        max_bytes: Option<u32>,
    ) -> Result<FetchResult, SdkError> {
        let results = self
            .fetch_many(vec![(topic_id, partition_id, max_bytes)])
            .await?;

        results.into_iter().next().ok_or(SdkError::InvalidResponse)
    }

    /// Fetch records from multiple partitions.
    pub async fn fetch_many(
        &self,
        partitions: Vec<(TopicId, PartitionId, Option<u32>)>,
    ) -> Result<Vec<FetchResult>, SdkError> {
        let generation = *self.generation.lock().await;
        let offsets = self.offsets.lock().await;

        let fetches: Vec<consumer::PartitionFetch> = partitions
            .iter()
            .map(|(topic_id, partition_id, max_bytes)| {
                let key = PartitionKey {
                    topic_id: *topic_id,
                    partition_id: *partition_id,
                };
                let offset = offsets.get(&key).copied().unwrap_or(Offset(0));

                consumer::PartitionFetch {
                    topic_id: *topic_id,
                    partition_id: *partition_id,
                    offset,
                    max_bytes: max_bytes.unwrap_or(self.config.max_bytes),
                }
            })
            .collect();

        drop(offsets); // Release lock before network call

        let req = consumer::FetchRequest {
            group_id: self.config.group_id.clone(),
            consumer_id: self.config.consumer_id.clone(),
            generation,
            fetches,
        };

        let results = self.send_fetch_request(&req).await?;

        // Update offsets if auto-commit
        if self.config.auto_commit {
            let mut offsets = self.offsets.lock().await;
            for result in &results {
                if !result.records.is_empty() {
                    let key = PartitionKey {
                        topic_id: result.topic_id,
                        partition_id: result.partition_id,
                    };
                    // Advance offset by number of records fetched
                    let current = offsets.get(&key).copied().unwrap_or(Offset(0));
                    offsets.insert(key, Offset(current.0 + result.records.len() as u64));
                }
            }
        }

        Ok(results)
    }

    /// Send a fetch request and receive response.
    async fn send_fetch_request(
        &self,
        req: &consumer::FetchRequest,
    ) -> Result<Vec<FetchResult>, SdkError> {
        // Encode request
        let mut buf = vec![0u8; 8 * 1024];
        let len = consumer::encode_fetch_request(req, &mut buf);
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
                let (resp, _) = consumer::decode_fetch_response(&data)
                    .map_err(|e| SdkError::Decode(e.to_string()))?;

                Ok(resp
                    .results
                    .into_iter()
                    .map(|r| FetchResult {
                        topic_id: r.topic_id,
                        partition_id: r.partition_id,
                        schema_id: r.schema_id,
                        high_watermark: r.high_watermark,
                        records: r.records,
                    })
                    .collect())
            }
            Message::Close(_) => Err(SdkError::Disconnected),
            _ => Err(SdkError::InvalidResponse),
        }
    }

    /// Get the current offset for a partition.
    pub async fn get_offset(&self, topic_id: TopicId, partition_id: PartitionId) -> Offset {
        let key = PartitionKey {
            topic_id,
            partition_id,
        };
        self.offsets
            .lock()
            .await
            .get(&key)
            .copied()
            .unwrap_or(Offset(0))
    }

    /// Set the offset for a partition (seek).
    pub async fn seek(&self, topic_id: TopicId, partition_id: PartitionId, offset: Offset) {
        let key = PartitionKey {
            topic_id,
            partition_id,
        };
        self.offsets.lock().await.insert(key, offset);
        debug!(
            "Seek to offset {} for {:?}/{:?}",
            offset.0, topic_id, partition_id
        );
    }

    /// Reset offset to beginning (0).
    pub async fn seek_to_beginning(&self, topic_id: TopicId, partition_id: PartitionId) {
        self.seek(topic_id, partition_id, Offset(0)).await;
    }

    /// Commit current offsets (no-op if auto_commit is enabled).
    pub async fn commit(&self) -> Result<(), SdkError> {
        // In a full implementation, this would send a commit request to the server
        // For now, offsets are tracked locally
        debug!("Commit offsets (local only)");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_config_default() {
        let config = ConsumerConfig::default();
        assert_eq!(config.max_bytes, 1024 * 1024);
        assert!(config.auto_commit);
    }

    #[test]
    fn test_partition_key_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        let key1 = PartitionKey {
            topic_id: TopicId(1),
            partition_id: PartitionId(0),
        };
        let key2 = PartitionKey {
            topic_id: TopicId(1),
            partition_id: PartitionId(1),
        };
        let key3 = PartitionKey {
            topic_id: TopicId(1),
            partition_id: PartitionId(0),
        };

        set.insert(key1);
        set.insert(key2);
        set.insert(key3); // Duplicate of key1

        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_fetch_request_encoding() {
        let req = consumer::FetchRequest {
            group_id: "test-group".to_string(),
            consumer_id: "test-consumer".to_string(),
            generation: Generation(1),
            fetches: vec![consumer::PartitionFetch {
                topic_id: TopicId(1),
                partition_id: PartitionId(0),
                offset: Offset(100),
                max_bytes: 1024,
            }],
        };

        let mut buf = vec![0u8; 1024];
        let len = consumer::encode_fetch_request(&req, &mut buf);
        assert!(len > 0);

        let (decoded, _) = consumer::decode_fetch_request(&buf[..len]).unwrap();
        assert_eq!(decoded.group_id, "test-group");
        assert_eq!(decoded.fetches.len(), 1);
        assert_eq!(decoded.fetches[0].offset.0, 100);
    }

    #[tokio::test]
    async fn test_offset_tracking() {
        // Create a mock consumer (can't connect without server)
        let offsets: HashMap<PartitionKey, Offset> = HashMap::new();
        let offsets = Mutex::new(offsets);

        let key = PartitionKey {
            topic_id: TopicId(1),
            partition_id: PartitionId(0),
        };

        // Initial offset should be 0
        assert_eq!(offsets.lock().await.get(&key).copied(), None);

        // Set offset
        offsets.lock().await.insert(key, Offset(100));
        assert_eq!(offsets.lock().await.get(&key).copied(), Some(Offset(100)));

        // Advance offset
        let current = offsets.lock().await.get(&key).copied().unwrap_or(Offset(0));
        offsets.lock().await.insert(key, Offset(current.0 + 10));
        assert_eq!(offsets.lock().await.get(&key).copied(), Some(Offset(110)));
    }
}
