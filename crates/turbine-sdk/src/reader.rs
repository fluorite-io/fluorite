//! Reader client for reading messages from Turbine.
//!
//! Features:
//! - Reader group support with automatic partition assignment
//! - Heartbeat loop for membership maintenance
//! - Automatic rebalance handling
//! - Offset tracking and commit

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};
use tracing::{debug, error, info, warn};

use turbine_common::ids::{Generation, Offset, PartitionId, TopicId};
use turbine_wire::{
    ClientMessage, ServerMessage, auth as wire_auth, reader, decode_server_message,
    encode_client_message,
};

use crate::SdkError;

/// Configuration for the reader.
#[derive(Debug, Clone)]
pub struct ReaderConfig {
    /// Server URL (e.g., "ws://localhost:9000")
    pub url: String,
    /// API key for authentication (optional).
    pub api_key: Option<String>,
    /// Reader group ID.
    pub group_id: String,
    /// Reader ID within the group.
    pub reader_id: String,
    /// Topic to subscribe to.
    pub topic_id: TopicId,
    /// Default max bytes per read.
    pub max_bytes: u32,
    /// Request timeout.
    pub timeout: Duration,
    /// Heartbeat interval.
    pub heartbeat_interval: Duration,
    /// Rebalance delay (wait before claiming partitions).
    pub rebalance_delay: Duration,
}

impl Default for ReaderConfig {
    fn default() -> Self {
        Self {
            url: "ws://localhost:9000".to_string(),
            api_key: None,
            group_id: "default".to_string(),
            reader_id: uuid::Uuid::new_v4().to_string(),
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

/// Reader state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReaderState {
    /// Not yet joined.
    Init,
    /// Joined and actively consuming.
    Active,
    /// Rebalance in progress.
    Rebalancing,
    /// Stopped.
    Stopped,
}

/// Reader client with reader group support.
pub struct GroupReader {
    config: ReaderConfig,
    ws: Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>,
    state: RwLock<ReaderState>,
    generation: RwLock<Generation>,
    assignments: RwLock<Vec<PartitionAssignment>>,
    offsets: RwLock<HashMap<PartitionId, Offset>>,
    running: AtomicBool,
}

impl GroupReader {
    /// Create a new group reader and join the group.
    pub async fn join(config: ReaderConfig) -> Result<Arc<Self>, SdkError> {
        let (mut ws, _) = connect_async(&config.url)
            .await
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        // Perform auth handshake if API key is provided
        if let Some(ref api_key) = config.api_key {
            Self::authenticate(&mut ws, api_key).await?;
        }

        let reader = Arc::new(Self {
            config,
            ws: Mutex::new(ws),
            state: RwLock::new(ReaderState::Init),
            generation: RwLock::new(Generation(0)),
            assignments: RwLock::new(vec![]),
            offsets: RwLock::new(HashMap::new()),
            running: AtomicBool::new(true),
        });

        // Join the group
        reader.do_join().await?;

        Ok(reader)
    }

    /// Perform authentication handshake.
    async fn authenticate(
        ws: &mut WebSocketStream<MaybeTlsStream<TcpStream>>,
        api_key: &str,
    ) -> Result<(), SdkError> {
        let auth_req = wire_auth::AuthRequest {
            api_key: api_key.to_string(),
        };

        let mut buf = vec![0u8; 512];
        let len = encode_client_message(&ClientMessage::Auth(auth_req), &mut buf)
            .map_err(|e| SdkError::Protocol(e.to_string()))?;
        buf.truncate(len);

        ws.send(Message::Binary(buf))
            .await
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        // Wait for response
        let resp = ws
            .next()
            .await
            .ok_or_else(|| SdkError::Connection("connection closed during auth".to_string()))?
            .map_err(|e| SdkError::Connection(e.to_string()))?;

        let data = match resp {
            Message::Binary(d) => d,
            _ => {
                return Err(SdkError::Protocol(
                    "unexpected auth response type".to_string(),
                ));
            }
        };

        let (resp_msg, used) =
            decode_server_message(&data).map_err(|e| SdkError::Protocol(e.to_string()))?;
        if used != data.len() {
            return Err(SdkError::Protocol(
                "trailing bytes in auth response".to_string(),
            ));
        }
        let auth_resp = match resp_msg {
            ServerMessage::Auth(resp) => resp,
            _ => {
                return Err(SdkError::Protocol(
                    "unexpected auth response type".to_string(),
                ));
            }
        };

        if !auth_resp.success {
            return Err(SdkError::Auth(auth_resp.error_message));
        }

        debug!("Authentication successful");
        Ok(())
    }

    /// Start the heartbeat loop in the background.
    pub fn start_heartbeat(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let reader = self.clone();
        tokio::spawn(async move {
            reader.heartbeat_loop().await;
        })
    }

    /// Stop the reader.
    pub async fn stop(&self) -> Result<(), SdkError> {
        self.running.store(false, Ordering::SeqCst);
        *self.state.write().await = ReaderState::Stopped;

        // Send leave group request
        self.do_leave().await?;

        Ok(())
    }

    /// Get current state.
    pub async fn state(&self) -> ReaderState {
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

    /// Read records from assigned partitions.
    pub async fn poll(&self) -> Result<Vec<ReadResult>, SdkError> {
        let state = *self.state.read().await;
        if state != ReaderState::Active {
            return Err(SdkError::InvalidState(format!("{:?}", state)));
        }

        let assignments = self.assignments.read().await.clone();
        if assignments.is_empty() {
            return Ok(vec![]);
        }

        let offsets = self.offsets.read().await;
        let reads: Vec<_> = assignments
            .iter()
            .map(|a| {
                let offset = offsets
                    .get(&a.partition_id)
                    .copied()
                    .unwrap_or(a.committed_offset);
                (self.config.topic_id, a.partition_id, offset)
            })
            .collect();
        drop(offsets);

        self.read_partitions(&reads).await
    }

    /// Commit current offsets.
    pub async fn commit(&self) -> Result<(), SdkError> {
        let generation = *self.generation.read().await;
        let offsets = self.offsets.read().await;

        let commits: Vec<reader::PartitionCommit> = offsets
            .iter()
            .map(|(&partition_id, &offset)| reader::PartitionCommit {
                topic_id: self.config.topic_id,
                partition_id,
                offset,
            })
            .collect();

        drop(offsets);

        if commits.is_empty() {
            return Ok(());
        }

        let req = reader::CommitRequest {
            group_id: self.config.group_id.clone(),
            reader_id: self.config.reader_id.clone(),
            generation,
            commits,
        };

        self.send_commit(&req).await
    }

    /// Join the reader group.
    async fn do_join(&self) -> Result<(), SdkError> {
        let req = reader::JoinGroupRequest {
            group_id: self.config.group_id.clone(),
            reader_id: self.config.reader_id.clone(),
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
        *self.state.write().await = ReaderState::Active;

        info!(
            "Joined group {} at generation {}",
            self.config.group_id, response.generation.0
        );

        Ok(())
    }

    /// Handle rebalance: rejoin with new generation.
    async fn do_rejoin(&self, new_generation: Generation) -> Result<(), SdkError> {
        *self.state.write().await = ReaderState::Rebalancing;

        // Commit current offsets before releasing partitions
        if let Err(e) = self.commit().await {
            warn!("Failed to commit offsets during rebalance: {}", e);
        }

        // Wait for rebalance delay
        tokio::time::sleep(self.config.rebalance_delay).await;

        let mut current_generation = new_generation;
        loop {
            let req = reader::RejoinRequest {
                group_id: self.config.group_id.clone(),
                topic_id: self.config.topic_id,
                reader_id: self.config.reader_id.clone(),
                generation: current_generation,
            };

            let response = self.send_rejoin(&req).await?;

            if response.status == reader::RejoinStatus::RebalanceNeeded {
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
            *self.state.write().await = ReaderState::Active;

            info!(
                "Rejoined group {} at generation {}",
                self.config.group_id, response.generation.0
            );

            return Ok(());
        }
    }

    /// Leave the reader group.
    async fn do_leave(&self) -> Result<(), SdkError> {
        // Commit before leaving
        if let Err(e) = self.commit().await {
            warn!("Failed to commit offsets during leave: {}", e);
        }

        let req = reader::LeaveGroupRequest {
            group_id: self.config.group_id.clone(),
            topic_id: self.config.topic_id,
            reader_id: self.config.reader_id.clone(),
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
            let req = reader::HeartbeatRequest {
                group_id: self.config.group_id.clone(),
                topic_id: self.config.topic_id,
                reader_id: self.config.reader_id.clone(),
                generation,
            };

            match self.send_heartbeat(&req).await {
                Ok(response) => match response.status {
                    reader::HeartbeatStatus::Ok => {
                        debug!("Heartbeat OK at generation {}", response.generation.0);
                    }
                    reader::HeartbeatStatus::RebalanceNeeded => {
                        info!("Rebalance needed, new generation {}", response.generation.0);
                        if let Err(e) = self.do_rejoin(response.generation).await {
                            error!("Failed to rejoin: {}", e);
                        }
                    }
                    reader::HeartbeatStatus::UnknownMember => {
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

    /// Read from specific partitions.
    async fn read_partitions(
        &self,
        partitions: &[(TopicId, PartitionId, Offset)],
    ) -> Result<Vec<ReadResult>, SdkError> {
        let generation = *self.generation.read().await;

        let reads: Vec<reader::PartitionRead> = partitions
            .iter()
            .map(
                |(topic_id, partition_id, offset)| reader::PartitionRead {
                    topic_id: *topic_id,
                    partition_id: *partition_id,
                    offset: *offset,
                    max_bytes: self.config.max_bytes,
                },
            )
            .collect();

        let req = reader::ReadRequest {
            group_id: self.config.group_id.clone(),
            reader_id: self.config.reader_id.clone(),
            generation,
            reads,
        };

        let results = self.send_read(&req).await?;

        // Update offsets
        let mut offsets = self.offsets.write().await;
        for result in &results {
            if !result.records.is_empty() {
                let current = offsets
                    .get(&result.partition_id)
                    .copied()
                    .unwrap_or(Offset(0));
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
        req: &reader::JoinGroupRequest,
    ) -> Result<reader::JoinGroupResponse, SdkError> {
        let mut buf = vec![0u8; 8192];
        let len = encode_client_message(&ClientMessage::JoinGroup(req.clone()), &mut buf)
            .map_err(|e| SdkError::Protocol(e.to_string()))?;
        buf.truncate(len);

        let response_data = self.send_and_receive(buf).await?;
        let (response_msg, used) =
            decode_server_message(&response_data).map_err(|e| SdkError::Decode(e.to_string()))?;
        if used != response_data.len() {
            return Err(SdkError::Decode(
                "trailing bytes in join response".to_string(),
            ));
        }
        let response = match response_msg {
            ServerMessage::JoinGroup(resp) => resp,
            _ => return Err(SdkError::InvalidResponse),
        };

        if !response.success {
            return Err(SdkError::Server {
                code: response.error_code,
                message: response.error_message,
            });
        }

        Ok(response)
    }

    async fn send_heartbeat(
        &self,
        req: &reader::HeartbeatRequest,
    ) -> Result<reader::HeartbeatResponseExt, SdkError> {
        let mut buf = vec![0u8; 256];
        let len = encode_client_message(&ClientMessage::Heartbeat(req.clone()), &mut buf)
            .map_err(|e| SdkError::Protocol(e.to_string()))?;
        buf.truncate(len);

        let response_data = self.send_and_receive(buf).await?;
        let (response_msg, used) =
            decode_server_message(&response_data).map_err(|e| SdkError::Decode(e.to_string()))?;
        if used != response_data.len() {
            return Err(SdkError::Decode(
                "trailing bytes in heartbeat response".to_string(),
            ));
        }
        let response = match response_msg {
            ServerMessage::Heartbeat(resp) => resp,
            _ => return Err(SdkError::InvalidResponse),
        };

        if !response.success {
            return Err(SdkError::Server {
                code: response.error_code,
                message: response.error_message,
            });
        }

        Ok(response)
    }

    async fn send_rejoin(
        &self,
        req: &reader::RejoinRequest,
    ) -> Result<reader::RejoinResponse, SdkError> {
        let mut buf = vec![0u8; 8192];
        let len = encode_client_message(&ClientMessage::Rejoin(req.clone()), &mut buf)
            .map_err(|e| SdkError::Protocol(e.to_string()))?;
        buf.truncate(len);

        let response_data = self.send_and_receive(buf).await?;
        let (response_msg, used) =
            decode_server_message(&response_data).map_err(|e| SdkError::Decode(e.to_string()))?;
        if used != response_data.len() {
            return Err(SdkError::Decode(
                "trailing bytes in rejoin response".to_string(),
            ));
        }
        let response = match response_msg {
            ServerMessage::Rejoin(resp) => resp,
            _ => return Err(SdkError::InvalidResponse),
        };

        if !response.success {
            return Err(SdkError::Server {
                code: response.error_code,
                message: response.error_message,
            });
        }

        Ok(response)
    }

    async fn send_leave(&self, req: &reader::LeaveGroupRequest) -> Result<(), SdkError> {
        let mut buf = vec![0u8; 256];
        let len = encode_client_message(&ClientMessage::LeaveGroup(req.clone()), &mut buf)
            .map_err(|e| SdkError::Protocol(e.to_string()))?;
        buf.truncate(len);

        let response_data = self.send_and_receive(buf).await?;
        let (response_msg, used) =
            decode_server_message(&response_data).map_err(|e| SdkError::Decode(e.to_string()))?;
        if used != response_data.len() {
            return Err(SdkError::Decode(
                "trailing bytes in leave response".to_string(),
            ));
        }
        let response = match response_msg {
            ServerMessage::LeaveGroup(resp) => resp,
            _ => return Err(SdkError::InvalidResponse),
        };
        if !response.success {
            return Err(SdkError::Server {
                code: response.error_code,
                message: response.error_message,
            });
        }
        Ok(())
    }

    async fn send_commit(&self, req: &reader::CommitRequest) -> Result<(), SdkError> {
        let mut buf = vec![0u8; 8192];
        let len = encode_client_message(&ClientMessage::Commit(req.clone()), &mut buf)
            .map_err(|e| SdkError::Protocol(e.to_string()))?;
        buf.truncate(len);

        let response_data = self.send_and_receive(buf).await?;
        let (response_msg, used) =
            decode_server_message(&response_data).map_err(|e| SdkError::Decode(e.to_string()))?;
        if used != response_data.len() {
            return Err(SdkError::Decode(
                "trailing bytes in commit response".to_string(),
            ));
        }
        let response = match response_msg {
            ServerMessage::Commit(resp) => resp,
            _ => return Err(SdkError::InvalidResponse),
        };

        if !response.success {
            return Err(SdkError::Server {
                code: response.error_code,
                message: response.error_message,
            });
        }

        Ok(())
    }

    async fn send_read(&self, req: &reader::ReadRequest) -> Result<Vec<ReadResult>, SdkError> {
        let mut buf = vec![0u8; 8192];
        let len = encode_client_message(&ClientMessage::Read(req.clone()), &mut buf)
            .map_err(|e| SdkError::Protocol(e.to_string()))?;
        buf.truncate(len);

        let response_data = self.send_and_receive(buf).await?;
        let (response_msg, used) =
            decode_server_message(&response_data).map_err(|e| SdkError::Decode(e.to_string()))?;
        if used != response_data.len() {
            return Err(SdkError::Decode(
                "trailing bytes in read response".to_string(),
            ));
        }
        let response = match response_msg {
            ServerMessage::Read(resp) => resp,
            _ => return Err(SdkError::InvalidResponse),
        };

        if !response.success {
            return Err(SdkError::Server {
                code: response.error_code,
                message: response.error_message,
            });
        }

        Ok(response
            .results
            .into_iter()
            .map(|r| ReadResult {
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

/// Result of a read operation.
#[derive(Debug)]
pub struct ReadResult {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub schema_id: turbine_common::ids::SchemaId,
    pub high_watermark: Offset,
    pub records: Vec<turbine_common::types::Record>,
}

/// Primary SDK reader (group-aware).
pub type Reader = GroupReader;
/// Legacy single-connection reader.
pub use legacy::Reader as LegacyReader;
pub use legacy::ReaderConfig as LegacyReaderConfig;

mod legacy {
    //! Legacy reader without reader group support.

    use super::*;
    use turbine_common::ids::SchemaId;
    use turbine_common::types::Record;

    /// Configuration for the reader.
    #[derive(Debug, Clone)]
    pub struct ReaderConfig {
        pub url: String,
        pub group_id: String,
        pub reader_id: String,
        pub max_bytes: u32,
        pub timeout: Duration,
        pub auto_commit: bool,
    }

    impl Default for ReaderConfig {
        fn default() -> Self {
            Self {
                url: "ws://localhost:9000".to_string(),
                group_id: "default".to_string(),
                reader_id: uuid::Uuid::new_v4().to_string(),
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
    pub struct ReadResult {
        pub topic_id: TopicId,
        pub partition_id: PartitionId,
        pub schema_id: SchemaId,
        pub high_watermark: Offset,
        pub records: Vec<Record>,
    }

    pub struct Reader {
        config: ReaderConfig,
        ws: Mutex<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        generation: Mutex<Generation>,
        offsets: Mutex<HashMap<PartitionKey, Offset>>,
    }

    impl Reader {
        pub async fn connect(
            url: &str,
            group_id: &str,
            reader_id: &str,
        ) -> Result<Arc<Self>, SdkError> {
            let config = ReaderConfig {
                url: url.to_string(),
                group_id: group_id.to_string(),
                reader_id: reader_id.to_string(),
                ..Default::default()
            };
            Self::connect_with_config(config).await
        }

        pub async fn connect_with_config(config: ReaderConfig) -> Result<Arc<Self>, SdkError> {
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

        pub fn reader_id(&self) -> &str {
            &self.config.reader_id
        }

        pub fn group_id(&self) -> &str {
            &self.config.group_id
        }

        pub async fn read(
            &self,
            topic_id: TopicId,
            partition_id: PartitionId,
            max_bytes: Option<u32>,
        ) -> Result<ReadResult, SdkError> {
            let results = self
                .read_many(vec![(topic_id, partition_id, max_bytes)])
                .await?;

            results.into_iter().next().ok_or(SdkError::InvalidResponse)
        }

        pub async fn read_many(
            &self,
            partitions: Vec<(TopicId, PartitionId, Option<u32>)>,
        ) -> Result<Vec<ReadResult>, SdkError> {
            let generation = *self.generation.lock().await;
            let offsets = self.offsets.lock().await;

            let reads: Vec<reader::PartitionRead> = partitions
                .iter()
                .map(|(topic_id, partition_id, max_bytes)| {
                    let key = PartitionKey {
                        topic_id: *topic_id,
                        partition_id: *partition_id,
                    };
                    let offset = offsets.get(&key).copied().unwrap_or(Offset(0));

                    reader::PartitionRead {
                        topic_id: *topic_id,
                        partition_id: *partition_id,
                        offset,
                        max_bytes: max_bytes.unwrap_or(self.config.max_bytes),
                    }
                })
                .collect();

            drop(offsets);

            let req = reader::ReadRequest {
                group_id: self.config.group_id.clone(),
                reader_id: self.config.reader_id.clone(),
                generation,
                reads,
            };

            let results = self.send_read_request(&req).await?;

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

        async fn send_read_request(
            &self,
            req: &reader::ReadRequest,
        ) -> Result<Vec<ReadResult>, SdkError> {
            let mut buf = vec![0u8; 8 * 1024];
            let len = encode_client_message(&ClientMessage::Read(req.clone()), &mut buf)
                .map_err(|e| SdkError::Protocol(e.to_string()))?;
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
                    let (resp_msg, used) = decode_server_message(&data)
                        .map_err(|e| SdkError::Decode(e.to_string()))?;
                    if used != data.len() {
                        return Err(SdkError::Decode(
                            "trailing bytes in read response".to_string(),
                        ));
                    }
                    let resp = match resp_msg {
                        ServerMessage::Read(resp) => resp,
                        _ => return Err(SdkError::InvalidResponse),
                    };

                    if !resp.success {
                        return Err(SdkError::Server {
                            code: resp.error_code,
                            message: resp.error_message,
                        });
                    }

                    Ok(resp
                        .results
                        .into_iter()
                        .map(|r| ReadResult {
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
    fn test_reader_config_default() {
        let config = ReaderConfig::default();
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
    fn test_read_request_encoding() {
        let req = reader::ReadRequest {
            group_id: "test-group".to_string(),
            reader_id: "test-reader".to_string(),
            generation: Generation(1),
            reads: vec![reader::PartitionRead {
                topic_id: TopicId(1),
                partition_id: PartitionId(0),
                offset: Offset(100),
                max_bytes: 1024,
            }],
        };

        let mut buf = vec![0u8; 1024];
        let len = reader::encode_read_request(&req, &mut buf);
        assert!(len > 0);

        let (decoded, _) = reader::decode_read_request(&buf[..len]).unwrap();
        assert_eq!(decoded.group_id, "test-group");
        assert_eq!(decoded.reads.len(), 1);
        assert_eq!(decoded.reads[0].offset.0, 100);
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
