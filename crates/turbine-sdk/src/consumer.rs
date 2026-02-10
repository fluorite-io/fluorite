//! Consumer client for fetching messages from Turbine.
//!
//! Features:
//! - Consumer group support with automatic partition assignment
//! - Heartbeat loop for membership maintenance
//! - Automatic rebalance handling
//! - Offset tracking and commit

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error, info, warn};

use turbine_common::ids::{Generation, Offset, PartitionId, TopicId};
use turbine_wire::{consumer, MessageType};

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
    /// Topic to subscribe to.
    pub topic_id: TopicId,
    /// Default max bytes per fetch.
    pub max_bytes: u32,
    /// Request timeout.
    pub timeout: Duration,
    /// Heartbeat interval.
    pub heartbeat_interval: Duration,
    /// Rebalance delay (wait before claiming partitions).
    pub rebalance_delay: Duration,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            url: "ws://localhost:9000".to_string(),
            group_id: "default".to_string(),
            consumer_id: uuid::Uuid::new_v4().to_string(),
            topic_id: TopicId(1),
            max_bytes: 1024 * 1024, // 1 MB
            timeout: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(10),
            rebalance_delay: Duration::from_secs(5),
        }
    }
}

/// Partition assignment with committed offset.
#[derive(Debug, Clone)]
pub struct PartitionAssignment {
    pub partition_id: PartitionId,
    pub committed_offset: Offset,
}

/// Consumer state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsumerState {
    /// Not yet joined.
    Init,
    /// Joined and actively consuming.
    Active,
    /// Rebalance in progress.
    Rebalancing,
    /// Stopped.
    Stopped,
}

/// Consumer client with consumer group support.
pub struct GroupConsumer {
    config: ConsumerConfig,
    ws: Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    state: RwLock<ConsumerState>,
    generation: RwLock<Generation>,
    assignments: RwLock<Vec<PartitionAssignment>>,
    offsets: RwLock<HashMap<PartitionId, Offset>>,
    running: AtomicBool,
}

impl GroupConsumer {
    /// Create a new group consumer and join the group.
    pub async fn join(config: ConsumerConfig) -> Result<Arc<Self>, SdkError> {
        let (ws, _) = connect_async(&config.url)
            .await
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        let consumer = Arc::new(Self {
            config,
            ws: Mutex::new(ws),
            state: RwLock::new(ConsumerState::Init),
            generation: RwLock::new(Generation(0)),
            assignments: RwLock::new(vec![]),
            offsets: RwLock::new(HashMap::new()),
            running: AtomicBool::new(true),
        });

        // Join the group
        consumer.do_join().await?;

        Ok(consumer)
    }

    /// Start the heartbeat loop in the background.
    pub fn start_heartbeat(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let consumer = self.clone();
        tokio::spawn(async move {
            consumer.heartbeat_loop().await;
        })
    }

    /// Stop the consumer.
    pub async fn stop(&self) -> Result<(), SdkError> {
        self.running.store(false, Ordering::SeqCst);
        *self.state.write().await = ConsumerState::Stopped;

        // Send leave group request
        self.do_leave().await?;

        Ok(())
    }

    /// Get current state.
    pub async fn state(&self) -> ConsumerState {
        *self.state.read().await
    }

    /// Get current generation.
    pub async fn generation(&self) -> Generation {
        *self.generation.read().await
    }

    /// Get assigned partitions.
    pub async fn assignments(&self) -> Vec<PartitionAssignment> {
        self.assignments.read().await.clone()
    }

    /// Get assigned partition IDs.
    pub async fn assigned_partitions(&self) -> Vec<PartitionId> {
        self.assignments
            .read()
            .await
            .iter()
            .map(|a| a.partition_id)
            .collect()
    }

    /// Fetch records from assigned partitions.
    pub async fn poll(&self) -> Result<Vec<FetchResult>, SdkError> {
        let state = *self.state.read().await;
        if state != ConsumerState::Active {
            return Err(SdkError::InvalidState(format!("{:?}", state)));
        }

        let assignments = self.assignments.read().await.clone();
        if assignments.is_empty() {
            return Ok(vec![]);
        }

        let offsets = self.offsets.read().await;
        let fetches: Vec<_> = assignments
            .iter()
            .map(|a| {
                let offset = offsets.get(&a.partition_id).copied().unwrap_or(a.committed_offset);
                (self.config.topic_id, a.partition_id, offset)
            })
            .collect();
        drop(offsets);

        self.fetch_partitions(&fetches).await
    }

    /// Commit current offsets.
    pub async fn commit(&self) -> Result<(), SdkError> {
        let generation = *self.generation.read().await;
        let offsets = self.offsets.read().await;

        let commits: Vec<consumer::PartitionCommit> = offsets
            .iter()
            .map(|(&partition_id, &offset)| consumer::PartitionCommit {
                topic_id: self.config.topic_id,
                partition_id,
                offset,
            })
            .collect();

        drop(offsets);

        if commits.is_empty() {
            return Ok(());
        }

        let req = consumer::CommitRequest {
            group_id: self.config.group_id.clone(),
            consumer_id: self.config.consumer_id.clone(),
            generation,
            commits,
        };

        self.send_commit(&req).await
    }

    /// Join the consumer group.
    async fn do_join(&self) -> Result<(), SdkError> {
        let req = consumer::JoinGroupRequest {
            group_id: self.config.group_id.clone(),
            consumer_id: self.config.consumer_id.clone(),
            topic_ids: vec![self.config.topic_id],
        };

        let response = self.send_join(&req).await?;

        // Update state
        *self.generation.write().await = response.generation;

        let assignments: Vec<PartitionAssignment> = response
            .assignments
            .into_iter()
            .map(|a| PartitionAssignment {
                partition_id: a.partition_id,
                committed_offset: a.committed_offset,
            })
            .collect();

        // Initialize offsets from committed offsets
        let mut offsets = self.offsets.write().await;
        for a in &assignments {
            offsets.insert(a.partition_id, a.committed_offset);
        }
        drop(offsets);

        *self.assignments.write().await = assignments;
        *self.state.write().await = ConsumerState::Active;

        info!(
            "Joined group {} at generation {}",
            self.config.group_id,
            response.generation.0
        );

        Ok(())
    }

    /// Handle rebalance: rejoin with new generation.
    async fn do_rejoin(&self, new_generation: Generation) -> Result<(), SdkError> {
        *self.state.write().await = ConsumerState::Rebalancing;

        // Commit current offsets before releasing partitions
        if let Err(e) = self.commit().await {
            warn!("Failed to commit offsets during rebalance: {}", e);
        }

        // Wait for rebalance delay
        tokio::time::sleep(self.config.rebalance_delay).await;

        let mut current_generation = new_generation;
        loop {
            let req = consumer::RejoinRequest {
                group_id: self.config.group_id.clone(),
                topic_id: self.config.topic_id,
                consumer_id: self.config.consumer_id.clone(),
                generation: current_generation,
            };

            let response = self.send_rejoin(&req).await?;

            if response.status == consumer::RejoinStatus::RebalanceNeeded {
                // Generation changed again, retry
                current_generation = response.generation;
                tokio::time::sleep(self.config.rebalance_delay).await;
                continue;
            }

            // Update assignments
            let assignments: Vec<PartitionAssignment> = response
                .assignments
                .into_iter()
                .map(|a| PartitionAssignment {
                    partition_id: a.partition_id,
                    committed_offset: a.committed_offset,
                })
                .collect();

            // Reset offsets to committed
            let mut offsets = self.offsets.write().await;
            offsets.clear();
            for a in &assignments {
                offsets.insert(a.partition_id, a.committed_offset);
            }
            drop(offsets);

            *self.assignments.write().await = assignments;
            *self.generation.write().await = response.generation;
            *self.state.write().await = ConsumerState::Active;

            info!(
                "Rejoined group {} at generation {}",
                self.config.group_id, response.generation.0
            );

            return Ok(());
        }
    }

    /// Leave the consumer group.
    async fn do_leave(&self) -> Result<(), SdkError> {
        // Commit before leaving
        if let Err(e) = self.commit().await {
            warn!("Failed to commit offsets during leave: {}", e);
        }

        let req = consumer::LeaveGroupRequest {
            group_id: self.config.group_id.clone(),
            topic_id: self.config.topic_id,
            consumer_id: self.config.consumer_id.clone(),
        };

        self.send_leave(&req).await?;

        info!("Left group {}", self.config.group_id);
        Ok(())
    }

    /// Heartbeat loop.
    async fn heartbeat_loop(&self) {
        while self.running.load(Ordering::SeqCst) {
            tokio::time::sleep(self.config.heartbeat_interval).await;

            if !self.running.load(Ordering::SeqCst) {
                break;
            }

            let generation = *self.generation.read().await;
            let req = consumer::HeartbeatRequest {
                group_id: self.config.group_id.clone(),
                topic_id: self.config.topic_id,
                consumer_id: self.config.consumer_id.clone(),
                generation,
            };

            match self.send_heartbeat(&req).await {
                Ok(response) => match response.status {
                    consumer::HeartbeatStatus::Ok => {
                        debug!("Heartbeat OK at generation {}", response.generation.0);
                    }
                    consumer::HeartbeatStatus::RebalanceNeeded => {
                        info!(
                            "Rebalance needed, new generation {}",
                            response.generation.0
                        );
                        if let Err(e) = self.do_rejoin(response.generation).await {
                            error!("Failed to rejoin: {}", e);
                        }
                    }
                    consumer::HeartbeatStatus::UnknownMember => {
                        warn!("Unknown member, rejoining group");
                        if let Err(e) = self.do_join().await {
                            error!("Failed to rejoin: {}", e);
                        }
                    }
                },
                Err(e) => {
                    error!("Heartbeat failed: {}", e);
                }
            }
        }
    }

    /// Fetch from specific partitions.
    async fn fetch_partitions(
        &self,
        partitions: &[(TopicId, PartitionId, Offset)],
    ) -> Result<Vec<FetchResult>, SdkError> {
        let generation = *self.generation.read().await;

        let fetches: Vec<consumer::PartitionFetch> = partitions
            .iter()
            .map(|(topic_id, partition_id, offset)| consumer::PartitionFetch {
                topic_id: *topic_id,
                partition_id: *partition_id,
                offset: *offset,
                max_bytes: self.config.max_bytes,
            })
            .collect();

        let req = consumer::FetchRequest {
            group_id: self.config.group_id.clone(),
            consumer_id: self.config.consumer_id.clone(),
            generation,
            fetches,
        };

        let results = self.send_fetch(&req).await?;

        // Update offsets
        let mut offsets = self.offsets.write().await;
        for result in &results {
            if !result.records.is_empty() {
                let current = offsets.get(&result.partition_id).copied().unwrap_or(Offset(0));
                offsets.insert(
                    result.partition_id,
                    Offset(current.0 + result.records.len() as u64),
                );
            }
        }

        Ok(results)
    }

    // ============ Wire protocol methods ============

    async fn send_join(
        &self,
        req: &consumer::JoinGroupRequest,
    ) -> Result<consumer::JoinGroupResponse, SdkError> {
        let mut buf = vec![0u8; 8192];
        buf[0] = MessageType::JoinGroup.to_byte();
        let len = consumer::encode_join_request(req, &mut buf[1..]);
        buf.truncate(len + 1);

        let response_data = self.send_and_receive(buf).await?;

        // Skip message type byte
        let payload = if !response_data.is_empty() && response_data[0] == MessageType::JoinGroupResponse.to_byte() {
            &response_data[1..]
        } else {
            &response_data[..]
        };

        let (response, _) =
            consumer::decode_join_response(payload).map_err(|e| SdkError::Decode(e.to_string()))?;

        Ok(response)
    }

    async fn send_heartbeat(
        &self,
        req: &consumer::HeartbeatRequest,
    ) -> Result<consumer::HeartbeatResponseExt, SdkError> {
        let mut buf = vec![0u8; 256];
        buf[0] = MessageType::Heartbeat.to_byte();
        let len = consumer::encode_heartbeat_request(req, &mut buf[1..]);
        buf.truncate(len + 1);

        let response_data = self.send_and_receive(buf).await?;

        // Skip message type byte
        let payload = if !response_data.is_empty() && response_data[0] == MessageType::HeartbeatResponse.to_byte() {
            &response_data[1..]
        } else {
            &response_data[..]
        };

        let (response, _) = consumer::decode_heartbeat_response_ext(payload)
            .map_err(|e| SdkError::Decode(e.to_string()))?;

        Ok(response)
    }

    async fn send_rejoin(
        &self,
        req: &consumer::RejoinRequest,
    ) -> Result<consumer::RejoinResponse, SdkError> {
        let mut buf = vec![0u8; 8192];
        buf[0] = MessageType::Rejoin.to_byte();
        let len = consumer::encode_rejoin_request(req, &mut buf[1..]);
        buf.truncate(len + 1);

        let response_data = self.send_and_receive(buf).await?;

        // Skip message type byte
        let payload = if !response_data.is_empty() && response_data[0] == MessageType::RejoinResponse.to_byte() {
            &response_data[1..]
        } else {
            &response_data[..]
        };

        let (response, _) = consumer::decode_rejoin_response(payload)
            .map_err(|e| SdkError::Decode(e.to_string()))?;

        Ok(response)
    }

    async fn send_leave(&self, req: &consumer::LeaveGroupRequest) -> Result<(), SdkError> {
        let mut buf = vec![0u8; 256];
        buf[0] = MessageType::LeaveGroup.to_byte();
        let len = consumer::encode_leave_request(req, &mut buf[1..]);
        buf.truncate(len + 1);

        let _ = self.send_and_receive(buf).await?;
        Ok(())
    }

    async fn send_commit(&self, req: &consumer::CommitRequest) -> Result<(), SdkError> {
        let mut buf = vec![0u8; 8192];
        buf[0] = MessageType::Commit.to_byte();
        let len = consumer::encode_commit_request(req, &mut buf[1..]);
        buf.truncate(len + 1);

        let response_data = self.send_and_receive(buf).await?;

        // Skip message type byte
        let payload = if !response_data.is_empty() && response_data[0] == MessageType::CommitResponse.to_byte() {
            &response_data[1..]
        } else {
            &response_data[..]
        };

        let (response, _) = consumer::decode_commit_response(payload)
            .map_err(|e| SdkError::Decode(e.to_string()))?;

        if !response.success {
            return Err(SdkError::CommitFailed);
        }

        Ok(())
    }

    async fn send_fetch(&self, req: &consumer::FetchRequest) -> Result<Vec<FetchResult>, SdkError> {
        let mut buf = vec![0u8; 8192];
        buf[0] = MessageType::Fetch.to_byte();
        let len = consumer::encode_fetch_request(req, &mut buf[1..]);
        buf.truncate(len + 1);

        let response_data = self.send_and_receive(buf).await?;

        // Skip message type byte
        let payload = if !response_data.is_empty() && response_data[0] == MessageType::FetchResponse.to_byte() {
            &response_data[1..]
        } else {
            &response_data[..]
        };

        let (response, _) = consumer::decode_fetch_response(payload)
            .map_err(|e| SdkError::Decode(e.to_string()))?;

        Ok(response
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

    async fn send_and_receive(&self, buf: Vec<u8>) -> Result<Vec<u8>, SdkError> {
        let mut ws = self.ws.lock().await;

        ws.send(Message::Binary(buf))
            .await
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        let response = tokio::time::timeout(self.config.timeout, ws.next())
            .await
            .map_err(|_| SdkError::Timeout)?
            .ok_or(SdkError::Disconnected)?
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        match response {
            Message::Binary(data) => Ok(data),
            Message::Close(_) => Err(SdkError::Disconnected),
            _ => Err(SdkError::InvalidResponse),
        }
    }
}

/// Result of a fetch operation.
#[derive(Debug)]
pub struct FetchResult {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub schema_id: turbine_common::ids::SchemaId,
    pub high_watermark: Offset,
    pub records: Vec<turbine_common::types::Record>,
}

// Keep the old Consumer for backwards compatibility
pub use legacy::Consumer;
pub use legacy::ConsumerConfig as LegacyConsumerConfig;

mod legacy {
    //! Legacy consumer without consumer group support.

    use super::*;
    use turbine_common::ids::SchemaId;
    use turbine_common::types::Record;

    /// Configuration for the consumer.
    #[derive(Debug, Clone)]
    pub struct ConsumerConfig {
        pub url: String,
        pub group_id: String,
        pub consumer_id: String,
        pub max_bytes: u32,
        pub timeout: Duration,
        pub auto_commit: bool,
    }

    impl Default for ConsumerConfig {
        fn default() -> Self {
            Self {
                url: "ws://localhost:9000".to_string(),
                group_id: "default".to_string(),
                consumer_id: uuid::Uuid::new_v4().to_string(),
                max_bytes: 1024 * 1024,
                timeout: Duration::from_secs(30),
                auto_commit: true,
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    struct PartitionKey {
        topic_id: TopicId,
        partition_id: PartitionId,
    }

    #[derive(Debug)]
    pub struct FetchResult {
        pub topic_id: TopicId,
        pub partition_id: PartitionId,
        pub schema_id: SchemaId,
        pub high_watermark: Offset,
        pub records: Vec<Record>,
    }

    pub struct Consumer {
        config: ConsumerConfig,
        ws: Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        generation: Mutex<Generation>,
        offsets: Mutex<HashMap<PartitionKey, Offset>>,
    }

    impl Consumer {
        pub async fn connect(
            url: &str,
            group_id: &str,
            consumer_id: &str,
        ) -> Result<Arc<Self>, SdkError> {
            let config = ConsumerConfig {
                url: url.to_string(),
                group_id: group_id.to_string(),
                consumer_id: consumer_id.to_string(),
                ..Default::default()
            };
            Self::connect_with_config(config).await
        }

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

        pub fn consumer_id(&self) -> &str {
            &self.config.consumer_id
        }

        pub fn group_id(&self) -> &str {
            &self.config.group_id
        }

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

            drop(offsets);

            let req = consumer::FetchRequest {
                group_id: self.config.group_id.clone(),
                consumer_id: self.config.consumer_id.clone(),
                generation,
                fetches,
            };

            let results = self.send_fetch_request(&req).await?;

            if self.config.auto_commit {
                let mut offsets = self.offsets.lock().await;
                for result in &results {
                    if !result.records.is_empty() {
                        let key = PartitionKey {
                            topic_id: result.topic_id,
                            partition_id: result.partition_id,
                        };
                        let current = offsets.get(&key).copied().unwrap_or(Offset(0));
                        offsets.insert(key, Offset(current.0 + result.records.len() as u64));
                    }
                }
            }

            Ok(results)
        }

        async fn send_fetch_request(
            &self,
            req: &consumer::FetchRequest,
        ) -> Result<Vec<FetchResult>, SdkError> {
            let mut buf = vec![0u8; 8 * 1024];
            let len = consumer::encode_fetch_request(req, &mut buf);
            buf.truncate(len);

            let mut ws = self.ws.lock().await;

            ws.send(Message::Binary(buf))
                .await
                .map_err(|e| SdkError::Connection(e.to_string()))?;

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

        pub async fn seek(&self, topic_id: TopicId, partition_id: PartitionId, offset: Offset) {
            let key = PartitionKey {
                topic_id,
                partition_id,
            };
            self.offsets.lock().await.insert(key, offset);
        }

        pub async fn seek_to_beginning(&self, topic_id: TopicId, partition_id: PartitionId) {
            self.seek(topic_id, partition_id, Offset(0)).await;
        }

        pub async fn commit(&self) -> Result<(), SdkError> {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_config_default() {
        let config = ConsumerConfig::default();
        assert_eq!(config.max_bytes, 1024 * 1024);
        assert_eq!(config.heartbeat_interval, Duration::from_secs(10));
    }

    #[test]
    fn test_partition_key_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(PartitionId(0));
        set.insert(PartitionId(1));
        set.insert(PartitionId(0)); // Duplicate

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
        let offsets: HashMap<PartitionId, Offset> = HashMap::new();
        let offsets = RwLock::new(offsets);

        let key = PartitionId(0);

        assert_eq!(offsets.read().await.get(&key).copied(), None);

        offsets.write().await.insert(key, Offset(100));
        assert_eq!(offsets.read().await.get(&key).copied(), Some(Offset(100)));

        let current = offsets.read().await.get(&key).copied().unwrap_or(Offset(0));
        offsets.write().await.insert(key, Offset(current.0 + 10));
        assert_eq!(offsets.read().await.get(&key).copied(), Some(Offset(110)));
    }
}
