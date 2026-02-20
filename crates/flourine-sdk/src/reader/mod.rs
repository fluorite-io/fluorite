//! Reader client for reading messages from Flourine.
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

use flourine_common::ids::{Generation, Offset, PartitionId, TopicId};
use flourine_wire::{
    ClientMessage, ServerMessage, auth as wire_auth, decode_server_message, encode_client_message,
    reader,
};

use crate::SdkError;

/// Primary SDK reader (group-aware).
pub type Reader = GroupReader;

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
    Init,
    Active,
    Rebalancing,
    Stopped,
}

/// Result of a read operation.
#[derive(Debug)]
pub struct ReadResult {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub schema_id: flourine_common::ids::SchemaId,
    pub high_watermark: Offset,
    pub records: Vec<flourine_common::types::Record>,
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
        self.do_leave().await?;
        Ok(())
    }

    pub async fn state(&self) -> ReaderState {
        *self.state.read().await
    }

    pub async fn generation(&self) -> Generation {
        *self.generation.read().await
    }

    pub async fn assignments(&self) -> Vec<PartitionAssignment> {
        self.assignments.read().await.clone()
    }

    pub async fn assigned_partitions(&self) -> Vec<PartitionId> {
        self.assignments
            .read()
            .await
            .iter()
            .map(|a| a.partition_id)
            .collect()
    }

    /// Seek a partition to the given offset. The next `poll()` will read from this offset.
    pub async fn seek(&self, partition_id: PartitionId, offset: Offset) {
        self.offsets.write().await.insert(partition_id, offset);
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

        let resp = self
            .send_request(ClientMessage::Commit(req), 8192)
            .await?;
        match resp {
            ServerMessage::Commit(r) if r.success => Ok(()),
            ServerMessage::Commit(r) => Err(SdkError::Server {
                code: r.error_code,
                message: r.error_message,
            }),
            _ => Err(SdkError::InvalidResponse),
        }
    }

    /// Join the reader group.
    async fn do_join(&self) -> Result<(), SdkError> {
        let req = reader::JoinGroupRequest {
            group_id: self.config.group_id.clone(),
            reader_id: self.config.reader_id.clone(),
            topic_ids: vec![self.config.topic_id],
        };

        let resp = self
            .send_request(ClientMessage::JoinGroup(req), 8192)
            .await?;
        let response = match resp {
            ServerMessage::JoinGroup(r) if r.success => r,
            ServerMessage::JoinGroup(r) => {
                return Err(SdkError::Server {
                    code: r.error_code,
                    message: r.error_message,
                })
            }
            _ => return Err(SdkError::InvalidResponse),
        };

        *self.generation.write().await = response.generation;

        let assignments: Vec<PartitionAssignment> = response
            .assignments
            .into_iter()
            .map(|a| PartitionAssignment {
                partition_id: a.partition_id,
                committed_offset: a.committed_offset,
            })
            .collect();

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

        if let Err(e) = self.commit().await {
            warn!("Failed to commit offsets during rebalance: {}", e);
        }

        tokio::time::sleep(self.config.rebalance_delay).await;

        let mut current_generation = new_generation;
        loop {
            let req = reader::RejoinRequest {
                group_id: self.config.group_id.clone(),
                topic_id: self.config.topic_id,
                reader_id: self.config.reader_id.clone(),
                generation: current_generation,
            };

            let resp = self
                .send_request(ClientMessage::Rejoin(req), 8192)
                .await?;
            let response = match resp {
                ServerMessage::Rejoin(r) if r.success => r,
                ServerMessage::Rejoin(r) => {
                    return Err(SdkError::Server {
                        code: r.error_code,
                        message: r.error_message,
                    })
                }
                _ => return Err(SdkError::InvalidResponse),
            };

            if response.status == reader::RejoinStatus::RebalanceNeeded {
                current_generation = response.generation;
                tokio::time::sleep(self.config.rebalance_delay).await;
                continue;
            }

            let assignments: Vec<PartitionAssignment> = response
                .assignments
                .into_iter()
                .map(|a| PartitionAssignment {
                    partition_id: a.partition_id,
                    committed_offset: a.committed_offset,
                })
                .collect();

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
        if let Err(e) = self.commit().await {
            warn!("Failed to commit offsets during leave: {}", e);
        }

        let req = reader::LeaveGroupRequest {
            group_id: self.config.group_id.clone(),
            topic_id: self.config.topic_id,
            reader_id: self.config.reader_id.clone(),
        };

        let resp = self
            .send_request(ClientMessage::LeaveGroup(req), 256)
            .await?;
        match resp {
            ServerMessage::LeaveGroup(r) if r.success => {}
            ServerMessage::LeaveGroup(r) => {
                return Err(SdkError::Server {
                    code: r.error_code,
                    message: r.error_message,
                })
            }
            _ => return Err(SdkError::InvalidResponse),
        }

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

            let result = self
                .send_request(ClientMessage::Heartbeat(req), 256)
                .await;

            match result {
                Ok(ServerMessage::Heartbeat(response)) if response.success => {
                    match response.status {
                        reader::HeartbeatStatus::Ok => {
                            debug!("Heartbeat OK at generation {}", response.generation.0);
                        }
                        reader::HeartbeatStatus::RebalanceNeeded => {
                            info!(
                                "Rebalance needed, new generation {}",
                                response.generation.0
                            );
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
                    }
                }
                Ok(ServerMessage::Heartbeat(response)) => {
                    error!(
                        "Heartbeat failed: {} {}",
                        response.error_code, response.error_message
                    );
                }
                Err(e) => {
                    error!("Heartbeat failed: {}", e);
                }
                _ => {
                    error!("Heartbeat: unexpected response type");
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

        let resp = self
            .send_request(ClientMessage::Read(req), 8192)
            .await?;
        let response = match resp {
            ServerMessage::Read(r) if r.success => r,
            ServerMessage::Read(r) => {
                return Err(SdkError::Server {
                    code: r.error_code,
                    message: r.error_message,
                })
            }
            _ => return Err(SdkError::InvalidResponse),
        };

        let results: Vec<ReadResult> = response
            .results
            .into_iter()
            .map(|r| ReadResult {
                topic_id: r.topic_id,
                partition_id: r.partition_id,
                schema_id: r.schema_id,
                high_watermark: r.high_watermark,
                records: r.records,
            })
            .collect();

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

    // ============ Wire protocol ============

    /// Encode a client message, send it, receive and decode the server response.
    async fn send_request(
        &self,
        msg: ClientMessage,
        buf_size: usize,
    ) -> Result<ServerMessage, SdkError> {
        let mut buf = vec![0u8; buf_size];
        let len = encode_client_message(&msg, &mut buf)
            .map_err(|e| SdkError::Protocol(e.to_string()))?;
        buf.truncate(len);

        let response_data = self.send_and_receive(buf).await?;

        let (response_msg, used) =
            decode_server_message(&response_data).map_err(|e| SdkError::Decode(e.to_string()))?;
        if used != response_data.len() {
            return Err(SdkError::Decode(
                "trailing bytes in response".to_string(),
            ));
        }

        Ok(response_msg)
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
