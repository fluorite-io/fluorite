//! Batched broker server with buffering and deduplication.
//!
//! Extends the basic server with:
//! - Request buffering and merging
//! - Periodic flush to S3
//! - Deduplication via LRU cache
//! - Backpressure handling

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex as StdMutex};
use std::time::{Duration, Instant};
use std::ops::Bound;

use bumpalo::Bump;
use bumpalo::collections::Vec as BumpVec;
use futures::{SinkExt, StreamExt};
use sqlx::{PgPool, Postgres, QueryBuilder};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, RwLock, mpsc};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use tracing::Instrument;
use tracing::{debug, error, info, warn};

use turbine_common::ids::{Generation, Offset, WriterId, SchemaId, AppendSeq, TopicId};
use turbine_common::types::{RecordBatch, BatchAck};
use turbine_wire::{
    ClientMessage, ERR_AUTHZ_DENIED, ERR_BACKPRESSURE, ERR_DECODE_ERROR, ERR_INTERNAL_ERROR,
    ERR_INVALID_MESSAGE_TYPE, ERR_NOT_OWNER, ERR_STALE_SEQUENCE, STATUS_OK,
    ServerMessage, auth as wire_auth, reader, decode_client_message, encode_server_message,
    writer,
};

use crate::auth::{AclChecker, ApiKeyValidator, AuthError, Operation, Principal, ResourceType};

use crate::BrokerError;
use crate::buffer::{BrokerBuffer, BufferConfig, BatchKey};
use crate::coordinator::{Coordinator, CoordinatorConfig};
use crate::dedup::{DedupCache, DedupResult};
use crate::metrics::{
    BACKPRESSURE_ACTIVE, BUFFER_SIZE_BYTES, ConnectionGuard, DB_COMMIT_LATENCY_SECONDS,
    DB_COMMIT_OFFSETS_SECONDS, DB_COMMIT_WRITER_STATE_PREP_SECONDS,
    DB_COMMIT_WRITER_STATE_UPSERT_SECONDS, DB_COMMIT_TOPIC_BATCHES_SECONDS,
    DB_COMMIT_TX_COMMIT_SECONDS, DEDUP_CHECK_LATENCY_SECONDS, DEDUP_RESULT_TOTAL, ERRORS_TOTAL,
    READ_LATENCY_SECONDS, READ_REQUESTS_TOTAL, FLUSH_ACK_DISTRIBUTE_SECONDS,
    FLUSH_BATCH_PENDING_WRITERS, FLUSH_BATCH_RECORDS, FLUSH_BATCH_SEGMENTS,
    FLUSH_BUFFER_RESIDENCY_SECONDS, FLUSH_LATENCY_SECONDS, FLUSH_QUEUE_DEPTH,
    FLUSH_QUEUE_WAIT_SECONDS, FLUSH_TBIN_BUILD_SECONDS, FLUSH_TOTAL, INFLIGHT_APPEND_REQUESTS,
    LatencyTimer, APPEND_BYTES_TOTAL,
    APPEND_ENQUEUE_TO_ACK_SECONDS, APPEND_LATENCY_SECONDS, APPEND_REQUESTS_TOTAL,
    S3_PUT_LATENCY_SECONDS, WS_DECODE_CLIENT_SECONDS, WS_ENCODE_SERVER_SECONDS,
    WS_HANDLE_MESSAGE_SECONDS, WS_OUTBOUND_CHANNEL_WAIT_SECONDS, WS_OUTBOUND_QUEUE_WAIT_SECONDS,
    WS_WRITE_SECONDS,
};
use crate::object_store::ObjectStore;
use crate::tbin::{Codec, SegmentMeta, TbinReader, TbinWriter};

/// Batched broker configuration.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
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
    /// Whether to require authentication.
    pub require_auth: bool,
    /// Auth timeout for connection handshake.
    pub auth_timeout: Duration,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:9000".parse().unwrap(),
            bucket: "turbine".to_string(),
            key_prefix: "data".to_string(),
            buffer: BufferConfig::default(),
            flush_interval: Duration::from_millis(100),
            require_auth: false,
            auth_timeout: Duration::from_secs(10),
        }
    }
}

/// Message sent to the flush task.
#[allow(dead_code)] // ForceFlush and Shutdown are for graceful shutdown
enum FlushCommand {
    /// Insert a append request into the buffer.
    Insert {
        writer_id: WriterId,
        append_seq: AppendSeq,
        batches: Vec<RecordBatch>,
        enqueued_at: Instant,
        response_tx: tokio::sync::oneshot::Sender<Vec<turbine_common::types::BatchAck>>,
    },
    /// Force an immediate flush.
    ForceFlush,
    /// Shutdown the flush task.
    Shutdown,
}

#[derive(Clone)]
enum InFlightOutcome {
    Success(Vec<BatchAck>),
    Error { code: u16, message: String },
}

struct InFlightAppendEntry {
    waiters: Vec<tokio::sync::oneshot::Sender<InFlightOutcome>>,
}

#[derive(Default)]
struct WriterInFlightState {
    entries: HashMap<u64, InFlightAppendEntry>,
    seqs: BTreeSet<u64>,
}

/// Shared state for the batched broker.
pub struct BrokerState<S: ObjectStore> {
    pub pool: PgPool,
    pub store: Arc<S>,
    pub config: BrokerConfig,
    pub dedup_cache: DedupCache,
    pub coordinator: Coordinator,
    pub api_key_validator: ApiKeyValidator,
    pub acl_checker: AclChecker,
    /// Channel to send commands to the flush task.
    flush_tx: mpsc::Sender<FlushCommand>,
    /// Flag indicating if backpressure is active.
    backpressure: Arc<RwLock<bool>>,
    /// In-flight append requests grouped per writer.
    in_flight_append: Arc<Mutex<HashMap<WriterId, WriterInFlightState>>>,
    /// Number of queued append commands waiting in flush loop.
    pending_flush_commands: Arc<AtomicUsize>,
}

impl<S: ObjectStore + Send + Sync + 'static> BrokerState<S> {
    /// Create new batched broker state and spawn flush task.
    pub async fn new(pool: PgPool, store: S, config: BrokerConfig) -> Arc<Self> {
        Self::with_coordinator_config(pool, store, config, CoordinatorConfig::default()).await
    }

    /// Create new batched broker state with custom coordinator config.
    pub async fn with_coordinator_config(
        pool: PgPool,
        store: S,
        config: BrokerConfig,
        coordinator_config: CoordinatorConfig,
    ) -> Arc<Self> {
        let (flush_tx, flush_rx) = mpsc::channel(1000);
        let dedup_cache = DedupCache::new(pool.clone());
        let coordinator = Coordinator::new(pool.clone(), coordinator_config);
        let api_key_validator = ApiKeyValidator::new(pool.clone());
        let acl_checker = AclChecker::new(pool.clone());
        let store = Arc::new(store);
        let backpressure = Arc::new(RwLock::new(false));
        let in_flight_append = Arc::new(Mutex::new(HashMap::new()));
        let pending_flush_commands = Arc::new(AtomicUsize::new(0));

        let state = Arc::new(Self {
            pool: pool.clone(),
            store: store.clone(),
            config: config.clone(),
            dedup_cache,
            coordinator,
            api_key_validator,
            acl_checker,
            flush_tx,
            backpressure: backpressure.clone(),
            in_flight_append,
            pending_flush_commands,
        });
        FLUSH_QUEUE_DEPTH.set(0.0);
        BUFFER_SIZE_BYTES.set(0.0);

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

/// Default drain timeout for graceful shutdown.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(25);

/// Run the batched broker server.
pub async fn run<S: ObjectStore + Send + Sync + 'static>(
    state: Arc<BrokerState<S>>,
) -> Result<(), BrokerError> {
    run_with_shutdown(state, std::future::pending()).await
}

/// Run the batched broker server with graceful shutdown support.
pub async fn run_with_shutdown<S, F>(
    state: Arc<BrokerState<S>>,
    shutdown: F,
) -> Result<(), BrokerError>
where
    S: ObjectStore + Send + Sync + 'static,
    F: Future<Output = ()> + Send,
{
    use crate::shutdown::ConnectionTracker;

    let listener = TcpListener::bind(&state.config.bind_addr).await?;
    info!("Batched broker listening on {}", state.config.bind_addr);

    let tracker = Arc::new(ConnectionTracker::new());
    let mut shutdown = Pin::from(Box::new(shutdown));

    loop {
        tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, addr)) => {
                        let state = state.clone();
                        let conn_tracker = tracker.clone();

                        tokio::spawn(async move {
                            // Track connection for graceful shutdown
                            let _tracked = crate::shutdown::TrackedConnection::new(conn_tracker);
                            if let Err(e) = handle_connection(stream, addr, state).await {
                                error!("Connection error from {}: {}", addr, e);
                            }
                        });
                    }
                    Err(e) => {
                        error!("Failed to accept connection: {}", e);
                    }
                }
            }
            _ = &mut shutdown => {
                info!("Shutdown signal received, draining connections...");
                break;
            }
        }
    }

    // Stop accepting new connections and drain existing ones
    let drained = tracker.wait_for_drain(DRAIN_TIMEOUT).await;
    if !drained {
        warn!(
            "Drain timeout reached, {} connections still active",
            tracker.count()
        );
    }

    // Flush remaining buffer before exit
    info!("Flushing remaining buffer...");
    if state.flush_tx.send(FlushCommand::Shutdown).await.is_err() {
        error!("Failed to send shutdown command to flush task");
    }

    // Give the flush task time to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    info!("Graceful shutdown complete");
    Ok(())
}

/// Connection context with authenticated principal.
#[derive(Clone)]
struct ConnectionContext {
    principal: Option<Principal>,
}

enum OutboundMessage {
    Binary {
        data: Vec<u8>,
        enqueued_at: Instant,
    },
    Pong {
        data: Vec<u8>,
        enqueued_at: Instant,
    },
}

/// Handle a single WebSocket connection.
async fn handle_connection<S: ObjectStore + Send + Sync + 'static>(
    stream: TcpStream,
    addr: SocketAddr,
    state: Arc<BrokerState<S>>,
) -> Result<(), BrokerError> {
    let _conn_guard = ConnectionGuard::new();
    let ws_stream = accept_async(stream).await?;
    info!("New WebSocket connection from {}", addr);

    let (mut write, mut read) = ws_stream.split();

    // Auth handshake if required
    let ctx = if state.config.require_auth {
        match perform_auth_handshake(&mut read, &mut write, &state).await {
            Ok(principal) => {
                info!("Authenticated connection from {}: {}", addr, principal.id);
                ConnectionContext {
                    principal: Some(principal),
                }
            }
            Err(e) => {
                warn!("Auth failed for {}: {}", addr, e);
                return Ok(());
            }
        }
    } else {
        ConnectionContext { principal: None }
    };

    let (out_tx, mut out_rx) = mpsc::channel::<OutboundMessage>(1024);
    let writer_task = tokio::spawn(async move {
        while let Some(outbound) = out_rx.recv().await {
            let send_result = match outbound {
                OutboundMessage::Binary { data, enqueued_at } => {
                    WS_OUTBOUND_QUEUE_WAIT_SECONDS.observe(enqueued_at.elapsed().as_secs_f64());
                    let write_started = Instant::now();
                    let result = write.send(Message::Binary(data)).await;
                    WS_WRITE_SECONDS
                        .with_label_values(&["binary"])
                        .observe(write_started.elapsed().as_secs_f64());
                    result
                }
                OutboundMessage::Pong { data, enqueued_at } => {
                    WS_OUTBOUND_QUEUE_WAIT_SECONDS.observe(enqueued_at.elapsed().as_secs_f64());
                    let write_started = Instant::now();
                    let result = write.send(Message::Pong(data)).await;
                    WS_WRITE_SECONDS
                        .with_label_values(&["pong"])
                        .observe(write_started.elapsed().as_secs_f64());
                    result
                }
            };
            if send_result.is_err() {
                break;
            }
        }
    });

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
                let state = state.clone();
                let ctx = ctx.clone();
                let out_tx = out_tx.clone();
                tokio::spawn(async move {
                    let response = handle_message(&data, &state, &ctx).await;
                    let reserve_started = Instant::now();
                    match out_tx.reserve().await {
                        Ok(permit) => {
                            WS_OUTBOUND_CHANNEL_WAIT_SECONDS
                                .observe(reserve_started.elapsed().as_secs_f64());
                            permit.send(OutboundMessage::Binary {
                                data: response,
                                enqueued_at: Instant::now(),
                            });
                        }
                        Err(_) => {}
                    }
                });
            }
            Message::Ping(data) => {
                let reserve_started = Instant::now();
                match out_tx.reserve().await {
                    Ok(permit) => {
                        WS_OUTBOUND_CHANNEL_WAIT_SECONDS
                            .observe(reserve_started.elapsed().as_secs_f64());
                        permit.send(OutboundMessage::Pong {
                            data,
                            enqueued_at: Instant::now(),
                        });
                    }
                    Err(_) => break,
                }
            }
            Message::Close(_) => {
                info!("Connection closed by {}", addr);
                break;
            }
            _ => {}
        }
    }

    drop(out_tx);
    let _ = writer_task.await;

    Ok(())
}

/// Perform auth handshake with client.
#[tracing::instrument(level = "debug", skip(read, write, state))]
async fn perform_auth_handshake<S: ObjectStore + Send + Sync + 'static, R, W>(
    read: &mut R,
    write: &mut W,
    state: &BrokerState<S>,
) -> Result<Principal, AuthError>
where
    R: futures::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
    W: futures::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    // Wait for Auth message with timeout
    let auth_msg = tokio::time::timeout(state.config.auth_timeout, read.next())
        .await
        .map_err(|_| AuthError::Timeout)?
        .ok_or(AuthError::MissingApiKey)?
        .map_err(|_| AuthError::MissingApiKey)?;

    let data = match auth_msg {
        Message::Binary(d) => d,
        _ => return Err(AuthError::InvalidKeyFormat),
    };

    let (msg, used) = decode_client_message(&data).map_err(|_| AuthError::InvalidKeyFormat)?;
    if used != data.len() {
        return Err(AuthError::InvalidKeyFormat);
    }
    let auth_req = match msg {
        ClientMessage::Auth(req) => req,
        _ => return Err(AuthError::InvalidKeyFormat),
    };

    // Validate API key
    let result = state.api_key_validator.validate(&auth_req.api_key).await;

    // Send response
    let resp = match &result {
        Ok(_) => wire_auth::AuthResponse {
            success: true,
            error_message: String::new(),
            error_code: 0,
        },
        Err(e) => wire_auth::AuthResponse {
            success: false,
            error_message: e.to_string(),
            error_code: e.error_code(),
        },
    };

    let mut buf = vec![0u8; 512];
    let len = encode_server_message(&ServerMessage::Auth(resp), &mut buf)
        .map_err(|_| AuthError::InvalidKeyFormat)?;
    buf.truncate(len);

    write
        .send(Message::Binary(buf))
        .await
        .map_err(|_| AuthError::Timeout)?;

    result
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorResponseKind {
    Append,
    Read,
    JoinGroup,
    Heartbeat,
    Rejoin,
    LeaveGroup,
    Commit,
    Unknown,
}

const RESPONSE_CAPACITY_SMALL: usize = 512;
const RESPONSE_CAPACITY_PRODUCE: usize = 1024;
const RESPONSE_CAPACITY_LARGE: usize = 64 * 1024;
const DEFAULT_WRITER_STATE_BUMP_CAPACITY_BYTES: usize = 512 * 1024 * 1024;
static WRITER_STATE_ROWS_BUMP: LazyLock<StdMutex<Bump>> = LazyLock::new(|| {
    let capacity_bytes = std::env::var("TURBINE_WRITER_STATE_BUMP_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|bytes| *bytes > 0)
        .unwrap_or(DEFAULT_WRITER_STATE_BUMP_CAPACITY_BYTES);
    StdMutex::new(Bump::with_capacity(capacity_bytes))
});

fn server_message_kind(msg: &ServerMessage) -> &'static str {
    match msg {
        ServerMessage::Append(_) => "append",
        ServerMessage::Read(_) => "read",
        ServerMessage::JoinGroup(_) => "join_group",
        ServerMessage::Heartbeat(_) => "heartbeat",
        ServerMessage::Rejoin(_) => "rejoin",
        ServerMessage::LeaveGroup(_) => "leave_group",
        ServerMessage::Commit(_) => "commit",
        ServerMessage::Auth(_) => "auth",
    }
}

fn client_message_kind(msg: &ClientMessage) -> &'static str {
    match msg {
        ClientMessage::Append(_) => "append",
        ClientMessage::Read(_) => "read",
        ClientMessage::JoinGroup(_) => "join_group",
        ClientMessage::Heartbeat(_) => "heartbeat",
        ClientMessage::Rejoin(_) => "rejoin",
        ClientMessage::LeaveGroup(_) => "leave_group",
        ClientMessage::Commit(_) => "commit",
        ClientMessage::Auth(_) => "auth",
    }
}

fn error_kind_label(kind: ErrorResponseKind) -> &'static str {
    match kind {
        ErrorResponseKind::Append => "append",
        ErrorResponseKind::Read => "read",
        ErrorResponseKind::JoinGroup => "join_group",
        ErrorResponseKind::Heartbeat => "heartbeat",
        ErrorResponseKind::Rejoin => "rejoin",
        ErrorResponseKind::LeaveGroup => "leave_group",
        ErrorResponseKind::Commit => "commit",
        ErrorResponseKind::Unknown => "unknown",
    }
}

fn encode_server_message_vec(msg: ServerMessage, capacity: usize) -> Vec<u8> {
    let message_kind = server_message_kind(&msg);
    let encode_started = Instant::now();
    let mut buf_size = capacity.max(256);
    loop {
        let mut buf = vec![0u8; buf_size];
        match encode_server_message(&msg, &mut buf) {
            Ok(len) => {
                buf.truncate(len);
                WS_ENCODE_SERVER_SECONDS
                    .with_label_values(&[message_kind])
                    .observe(encode_started.elapsed().as_secs_f64());
                return buf;
            }
            Err(turbine_wire::EncodeError::BufferTooSmall { needed, .. }) => {
                buf_size = needed.max(buf_size.saturating_mul(2));
            }
            Err(e) => panic!("server message encode failed: {e}"),
        }
    }
}

fn encode_append_response(
    append_seq: AppendSeq,
    success: bool,
    error_code: u16,
    error_message: impl Into<String>,
    append_acks: Vec<BatchAck>,
) -> Vec<u8> {
    let resp = writer::AppendResponse {
        append_seq,
        success,
        error_code,
        error_message: error_message.into(),
        append_acks,
    };
    encode_server_message_vec(ServerMessage::Append(resp), RESPONSE_CAPACITY_PRODUCE)
}

fn encode_error_response(kind: ErrorResponseKind, error_code: u16, error_message: &str) -> Vec<u8> {
    let small_capacity = 96 + error_message.len();
    match kind {
        ErrorResponseKind::Append | ErrorResponseKind::Unknown => {
            encode_append_response(AppendSeq(0), false, error_code, error_message, vec![])
        }
        ErrorResponseKind::Read => {
            let error_resp = reader::ReadResponse {
                success: false,
                error_code,
                error_message: error_message.to_string(),
                results: vec![],
            };
            encode_server_message_vec(ServerMessage::Read(error_resp), small_capacity)
        }
        ErrorResponseKind::JoinGroup => {
            let error_resp = reader::JoinGroupResponse {
                success: false,
                error_code,
                error_message: error_message.to_string(),
                generation: Generation(0),
                assignments: vec![],
            };
            encode_server_message_vec(ServerMessage::JoinGroup(error_resp), small_capacity)
        }
        ErrorResponseKind::Heartbeat => {
            let error_resp = reader::HeartbeatResponseExt {
                success: false,
                error_code,
                error_message: error_message.to_string(),
                generation: Generation(0),
                status: reader::HeartbeatStatus::UnknownMember,
            };
            encode_server_message_vec(ServerMessage::Heartbeat(error_resp), small_capacity)
        }
        ErrorResponseKind::Rejoin => {
            let error_resp = reader::RejoinResponse {
                success: false,
                error_code,
                error_message: error_message.to_string(),
                generation: Generation(0),
                status: reader::RejoinStatus::RebalanceNeeded,
                assignments: vec![],
            };
            encode_server_message_vec(ServerMessage::Rejoin(error_resp), small_capacity)
        }
        ErrorResponseKind::LeaveGroup => {
            let error_resp = reader::LeaveGroupResponse {
                success: false,
                error_code,
                error_message: error_message.to_string(),
            };
            encode_server_message_vec(ServerMessage::LeaveGroup(error_resp), small_capacity)
        }
        ErrorResponseKind::Commit => {
            let error_resp = reader::CommitResponse {
                success: false,
                error_code,
                error_message: error_message.to_string(),
            };
            encode_server_message_vec(ServerMessage::Commit(error_resp), small_capacity)
        }
    }
}

async fn can_append<S: ObjectStore + Send + Sync + 'static>(
    state: &BrokerState<S>,
    principal: Option<&Principal>,
    req: &writer::AppendRequest,
) -> bool {
    if let Some(principal) = principal {
        for batch in &req.batches {
            let topic_name = batch.topic_id.0.to_string();
            let allowed = state
                .acl_checker
                .check(
                    principal,
                    ResourceType::Topic,
                    &topic_name,
                    Operation::Append,
                )
                .await;
            if !allowed {
                return false;
            }
        }
    }
    true
}

async fn can_read<S: ObjectStore + Send + Sync + 'static>(
    state: &BrokerState<S>,
    principal: Option<&Principal>,
    req: &reader::ReadRequest,
) -> bool {
    if let Some(principal) = principal {
        for read in &req.reads {
            let topic_name = read.topic_id.0.to_string();
            let allowed = state
                .acl_checker
                .check(
                    principal,
                    ResourceType::Topic,
                    &topic_name,
                    Operation::Consume,
                )
                .await;
            if !allowed {
                return false;
            }
        }
    }
    true
}

async fn can_group_consume<S: ObjectStore + Send + Sync + 'static>(
    state: &BrokerState<S>,
    principal: Option<&Principal>,
    group_id: &str,
) -> bool {
    if let Some(principal) = principal {
        return state
            .acl_checker
            .check(principal, ResourceType::Group, group_id, Operation::Consume)
            .await;
    }
    true
}

async fn can_consume_topic<S: ObjectStore + Send + Sync + 'static>(
    state: &BrokerState<S>,
    principal: Option<&Principal>,
    topic_id: TopicId,
) -> bool {
    if let Some(principal) = principal {
        let topic_name = topic_id.0.to_string();
        return state
            .acl_checker
            .check(
                principal,
                ResourceType::Topic,
                &topic_name,
                Operation::Consume,
            )
            .await;
    }
    true
}

enum InFlightAppendDecision {
    Proceed,
    Wait(tokio::sync::oneshot::Receiver<InFlightOutcome>),
}

fn total_inflight_requests(
    in_flight: &HashMap<WriterId, WriterInFlightState>,
) -> usize {
    in_flight.values().map(|state| state.entries.len()).sum()
}

async fn in_flight_append_decision<S: ObjectStore + Send + Sync + 'static>(
    state: &BrokerState<S>,
    writer_id: WriterId,
    append_seq: AppendSeq,
) -> InFlightAppendDecision {
    let mut in_flight = state.in_flight_append.lock().await;
    let writer_in_flight = in_flight.entry(writer_id).or_default();
    if let Some(entry) = writer_in_flight.entries.get_mut(&append_seq.0) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        entry.waiters.push(tx);
        return InFlightAppendDecision::Wait(rx);
    }

    writer_in_flight
        .entries
        .insert(append_seq.0, InFlightAppendEntry { waiters: Vec::new() });
    writer_in_flight.seqs.insert(append_seq.0);
    INFLIGHT_APPEND_REQUESTS.set(total_inflight_requests(&in_flight) as f64);
    InFlightAppendDecision::Proceed
}

async fn complete_in_flight_append<S: ObjectStore + Send + Sync + 'static>(
    state: &BrokerState<S>,
    writer_id: WriterId,
    append_seq: AppendSeq,
    outcome: InFlightOutcome,
) {
    let waiters = {
        let mut in_flight = state.in_flight_append.lock().await;
        let mut remove_writer = false;
        let waiters = if let Some(writer_in_flight) = in_flight.get_mut(&writer_id) {
            let waiters = writer_in_flight
                .entries
                .remove(&append_seq.0)
                .map(|entry| entry.waiters)
                .unwrap_or_default();
            writer_in_flight.seqs.remove(&append_seq.0);
            if writer_in_flight.entries.is_empty() {
                remove_writer = true;
            }
            waiters
        } else {
            Vec::new()
        };
        if remove_writer {
            in_flight.remove(&writer_id);
        }
        INFLIGHT_APPEND_REQUESTS.set(total_inflight_requests(&in_flight) as f64);
        waiters
    };

    for waiter in waiters {
        let _ = waiter.send(outcome.clone());
    }
}

async fn has_higher_in_flight_sequence<S: ObjectStore + Send + Sync + 'static>(
    state: &BrokerState<S>,
    writer_id: WriterId,
    append_seq: AppendSeq,
) -> bool {
    let in_flight = state.in_flight_append.lock().await;
    in_flight
        .get(&writer_id)
        .map(|writer_in_flight| {
            writer_in_flight
                .seqs
                .range((Bound::Excluded(append_seq.0), Bound::Unbounded))
                .next()
                .is_some()
        })
        .unwrap_or(false)
}

/// Handle a binary message.
async fn handle_message<S: ObjectStore + Send + Sync + 'static>(
    data: &[u8],
    state: &BrokerState<S>,
    ctx: &ConnectionContext,
) -> Vec<u8> {
    let handle_started = Instant::now();
    let observe_and_return = |kind: &'static str, response: Vec<u8>| -> Vec<u8> {
        WS_HANDLE_MESSAGE_SECONDS
            .with_label_values(&[kind])
            .observe(handle_started.elapsed().as_secs_f64());
        response
    };

    let decode_started = Instant::now();
    let (msg, used) = match decode_client_message(data) {
        Ok(decoded) => decoded,
        Err(_) => {
            WS_DECODE_CLIENT_SECONDS.observe(decode_started.elapsed().as_secs_f64());
            if let Ok((key, _)) = turbine_wire::varint::decode_u64(data) {
                let field = key >> 3;
                let wire_type = key & 0x07;
                if wire_type != 2 {
                    return observe_and_return(
                        "decode_error",
                        encode_error_response(
                            ErrorResponseKind::Unknown,
                            ERR_INVALID_MESSAGE_TYPE,
                            "invalid proto-framed request",
                        ),
                    );
                }
                let (kind, decode_msg) = match field {
                    1 => (
                        ErrorResponseKind::Append,
                        "failed to decode append request",
                    ),
                    2 => (ErrorResponseKind::Read, "failed to decode read request"),
                    3 => (
                        ErrorResponseKind::JoinGroup,
                        "failed to decode join request",
                    ),
                    4 => (
                        ErrorResponseKind::Heartbeat,
                        "failed to decode heartbeat request",
                    ),
                    5 => (ErrorResponseKind::Rejoin, "failed to decode rejoin request"),
                    6 => (
                        ErrorResponseKind::LeaveGroup,
                        "failed to decode leave request",
                    ),
                    7 => (ErrorResponseKind::Commit, "failed to decode commit request"),
                    8 => (
                        ErrorResponseKind::Unknown,
                        "auth request is only valid during handshake",
                    ),
                    _ => (ErrorResponseKind::Unknown, "invalid proto-framed request"),
                };
                if matches!(
                    kind,
                    ErrorResponseKind::Append
                        | ErrorResponseKind::Read
                        | ErrorResponseKind::JoinGroup
                        | ErrorResponseKind::Heartbeat
                        | ErrorResponseKind::Rejoin
                        | ErrorResponseKind::LeaveGroup
                        | ErrorResponseKind::Commit
                ) {
                    return observe_and_return(
                        error_kind_label(kind),
                        encode_error_response(kind, ERR_DECODE_ERROR, decode_msg),
                    );
                }
                return observe_and_return(
                    error_kind_label(kind),
                    encode_error_response(kind, ERR_INVALID_MESSAGE_TYPE, decode_msg),
                );
            }
            return observe_and_return(
                "decode_error",
                encode_error_response(
                    ErrorResponseKind::Unknown,
                    ERR_INVALID_MESSAGE_TYPE,
                    "invalid proto-framed request",
                ),
            );
        }
    };
    WS_DECODE_CLIENT_SECONDS.observe(decode_started.elapsed().as_secs_f64());

    if used != data.len() {
        return observe_and_return(
            "decode_error",
            encode_error_response(
                ErrorResponseKind::Unknown,
                ERR_DECODE_ERROR,
                "trailing bytes in request",
            ),
        );
    }

    let kind = client_message_kind(&msg);
    let response = match msg {
        ClientMessage::Append(req) => {
            if !can_append(state, ctx.principal.as_ref(), &req).await {
                encode_error_response(
                    ErrorResponseKind::Append,
                    ERR_AUTHZ_DENIED,
                    "append not authorized",
                )
            } else {
                handle_append_request(req, state).await
            }
        }
        ClientMessage::Read(req) => {
            if !can_read(state, ctx.principal.as_ref(), &req).await {
                encode_error_response(
                    ErrorResponseKind::Read,
                    ERR_AUTHZ_DENIED,
                    "read not authorized",
                )
            } else {
                handle_read_request(req, state).await
            }
        }
        ClientMessage::JoinGroup(req) => {
            if !can_group_consume(state, ctx.principal.as_ref(), &req.group_id).await {
                encode_error_response(
                    ErrorResponseKind::JoinGroup,
                    ERR_AUTHZ_DENIED,
                    "group consume not authorized",
                )
            } else {
                let mut denied = false;
                for topic_id in &req.topic_ids {
                    if !can_consume_topic(state, ctx.principal.as_ref(), *topic_id).await {
                        denied = true;
                        break;
                    }
                }
                if denied {
                    encode_error_response(
                        ErrorResponseKind::JoinGroup,
                        ERR_AUTHZ_DENIED,
                        "topic consume not authorized",
                    )
                } else {
                    handle_join_group(req, state).await
                }
            }
        }
        ClientMessage::Heartbeat(req) => {
            if !can_group_consume(state, ctx.principal.as_ref(), &req.group_id).await
                || !can_consume_topic(state, ctx.principal.as_ref(), req.topic_id).await
            {
                encode_error_response(
                    ErrorResponseKind::Heartbeat,
                    ERR_AUTHZ_DENIED,
                    "heartbeat not authorized",
                )
            } else {
                handle_heartbeat(req, state).await
            }
        }
        ClientMessage::Rejoin(req) => {
            if !can_group_consume(state, ctx.principal.as_ref(), &req.group_id).await
                || !can_consume_topic(state, ctx.principal.as_ref(), req.topic_id).await
            {
                encode_error_response(
                    ErrorResponseKind::Rejoin,
                    ERR_AUTHZ_DENIED,
                    "rejoin not authorized",
                )
            } else {
                handle_rejoin(req, state).await
            }
        }
        ClientMessage::LeaveGroup(req) => {
            if !can_group_consume(state, ctx.principal.as_ref(), &req.group_id).await
                || !can_consume_topic(state, ctx.principal.as_ref(), req.topic_id).await
            {
                encode_error_response(
                    ErrorResponseKind::LeaveGroup,
                    ERR_AUTHZ_DENIED,
                    "leave not authorized",
                )
            } else {
                handle_leave_group(req, state).await
            }
        }
        ClientMessage::Commit(req) => {
            if !can_group_consume(state, ctx.principal.as_ref(), &req.group_id).await {
                encode_error_response(
                    ErrorResponseKind::Commit,
                    ERR_AUTHZ_DENIED,
                    "commit group not authorized",
                )
            } else {
                let mut denied = false;
                for commit in &req.commits {
                    if !can_consume_topic(state, ctx.principal.as_ref(), commit.topic_id).await {
                        denied = true;
                        break;
                    }
                }
                if denied {
                    encode_error_response(
                        ErrorResponseKind::Commit,
                        ERR_AUTHZ_DENIED,
                        "commit topic not authorized",
                    )
                } else {
                    handle_commit(req, state).await
                }
            }
        }
        ClientMessage::Auth(_) => encode_error_response(
            ErrorResponseKind::Unknown,
            ERR_INVALID_MESSAGE_TYPE,
            "auth request is only valid during handshake",
        ),
    };

    WS_HANDLE_MESSAGE_SECONDS
        .with_label_values(&[kind])
        .observe(handle_started.elapsed().as_secs_f64());
    response
}

/// Handle a AppendRequest with batching.
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(
        writer_id = ?req.writer_id,
        append_seq = req.append_seq.0,
        segment_count = req.batches.len()
    )
)]
async fn handle_append_request<S: ObjectStore + Send + Sync + 'static>(
    req: writer::AppendRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    let _timer = LatencyTimer::new(&APPEND_LATENCY_SECONDS);
    APPEND_REQUESTS_TOTAL.inc();

    // Calculate bytes for metrics
    let total_bytes: usize = req
        .batches
        .iter()
        .flat_map(|s| &s.records)
        .map(|r| r.value.len() + r.key.as_ref().map(|k| k.len()).unwrap_or(0))
        .sum();
    APPEND_BYTES_TOTAL.inc_by(total_bytes as f64);

    match in_flight_append_decision(state, req.writer_id, req.append_seq).await {
        InFlightAppendDecision::Proceed => {}
        InFlightAppendDecision::Wait(rx) => match rx.await {
            Ok(InFlightOutcome::Success(append_acks)) => {
                return encode_append_response(req.append_seq, true, STATUS_OK, "", append_acks);
            }
            Ok(InFlightOutcome::Error { code, message }) => {
                return encode_append_response(req.append_seq, false, code, message, vec![]);
            }
            Err(_) => {
                return encode_append_response(
                    req.append_seq,
                    false,
                    ERR_INTERNAL_ERROR,
                    "in-flight request canceled",
                    vec![],
                );
            }
        },
    }

    // Check backpressure
    if state.is_backpressure_active().await {
        debug!(
            "Backpressure active, rejecting request from {:?}",
            req.writer_id
        );
        let result = encode_append_response(
            req.append_seq,
            false,
            ERR_BACKPRESSURE,
            "backpressure active",
            vec![],
        );
        complete_in_flight_append(
            state,
            req.writer_id,
            req.append_seq,
            InFlightOutcome::Error {
                code: ERR_BACKPRESSURE,
                message: "backpressure active".to_string(),
            },
        )
        .await;
        return result;
    }

    // Check for duplicates
    let dedup_started = Instant::now();
    let dedup_result = state.dedup_cache.check(req.writer_id, req.append_seq).await;
    DEDUP_CHECK_LATENCY_SECONDS.observe(dedup_started.elapsed().as_secs_f64());

    match dedup_result {
        Ok(DedupResult::Duplicate(append_acks)) => {
            DEDUP_RESULT_TOTAL.with_label_values(&["duplicate"]).inc();
            debug!(
                "Duplicate request from {:?} append_seq={}",
                req.writer_id, req.append_seq.0
            );
            let result = encode_append_response(req.append_seq, true, STATUS_OK, "", append_acks.clone());
            complete_in_flight_append(
                state,
                req.writer_id,
                req.append_seq,
                InFlightOutcome::Success(append_acks),
            )
            .await;
            return result;
        }
        Ok(DedupResult::Stale) => {
            DEDUP_RESULT_TOTAL.with_label_values(&["stale"]).inc();
            if has_higher_in_flight_sequence(state, req.writer_id, req.append_seq).await {
                debug!(
                    "Allowing out-of-order in-flight request from {:?} append_seq={}",
                    req.writer_id, req.append_seq.0
                );
            } else {
                debug!("Stale request from {:?} append_seq={}", req.writer_id, req.append_seq.0);
                let result = encode_append_response(
                    req.append_seq,
                    false,
                    ERR_STALE_SEQUENCE,
                    "stale writer sequence",
                    vec![],
                );
                complete_in_flight_append(
                    state,
                    req.writer_id,
                    req.append_seq,
                    InFlightOutcome::Error {
                        code: ERR_STALE_SEQUENCE,
                        message: "stale writer sequence".to_string(),
                    },
                )
                .await;
                return result;
            }
        }
        Ok(DedupResult::Accept) => {
            DEDUP_RESULT_TOTAL.with_label_values(&["accept"]).inc();
            // Continue processing
        }
        Err(e) => {
            DEDUP_RESULT_TOTAL.with_label_values(&["error"]).inc();
            error!("Dedup check error: {}", e);
            // Continue processing on error (fail open)
        }
    }

    // Send to flush task
    let (response_tx, response_rx) = tokio::sync::oneshot::channel();
    let enqueue_started = Instant::now();
    let pending = state
        .pending_flush_commands
        .fetch_add(1, Ordering::Relaxed)
        .saturating_add(1);
    FLUSH_QUEUE_DEPTH.set(pending as f64);

    if state
        .flush_tx
        .send(FlushCommand::Insert {
            writer_id: req.writer_id,
            append_seq: req.append_seq,
            batches: req.batches,
            enqueued_at: Instant::now(),
            response_tx,
        })
        .await
        .is_err()
    {
        let previous = state.pending_flush_commands.fetch_sub(1, Ordering::Relaxed);
        FLUSH_QUEUE_DEPTH.set(previous.saturating_sub(1) as f64);
        error!("Failed to send to flush task");
        let result = encode_append_response(
            req.append_seq,
            false,
            ERR_INTERNAL_ERROR,
            "failed to enqueue append request",
            vec![],
        );
        complete_in_flight_append(
            state,
            req.writer_id,
            req.append_seq,
            InFlightOutcome::Error {
                code: ERR_INTERNAL_ERROR,
                message: "failed to enqueue append request".to_string(),
            },
        )
        .await;
        return result;
    }

    // Wait for append_acks
    match response_rx.await {
        Ok(append_acks) => {
            APPEND_ENQUEUE_TO_ACK_SECONDS.observe(enqueue_started.elapsed().as_secs_f64());
            // Update dedup cache
            state
                .dedup_cache
                .update(req.writer_id, req.append_seq, append_acks.clone())
                .await;
            let result = encode_append_response(req.append_seq, true, STATUS_OK, "", append_acks.clone());
            complete_in_flight_append(
                state,
                req.writer_id,
                req.append_seq,
                InFlightOutcome::Success(append_acks),
            )
            .await;
            result
        }
        Err(_) => {
            APPEND_ENQUEUE_TO_ACK_SECONDS.observe(enqueue_started.elapsed().as_secs_f64());
            error!("Response channel closed");
            let result = encode_append_response(
                req.append_seq,
                false,
                ERR_INTERNAL_ERROR,
                "flush response channel closed",
                vec![],
            );
            complete_in_flight_append(
                state,
                req.writer_id,
                req.append_seq,
                InFlightOutcome::Error {
                    code: ERR_INTERNAL_ERROR,
                    message: "flush response channel closed".to_string(),
                },
            )
            .await;
            result
        }
    }
}

/// Handle a ReadRequest (same as non-batched, reads from S3).
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(
        group_id = %req.group_id,
        reader_id = %req.reader_id,
        fetch_count = req.reads.len()
    )
)]
async fn handle_read_request<S: ObjectStore + Send + Sync>(
    req: reader::ReadRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    let _timer = LatencyTimer::new(&READ_LATENCY_SECONDS);
    READ_REQUESTS_TOTAL.inc();

    let result = process_read(req, state).await;

    let response = match result {
        Ok(results) => reader::ReadResponse {
            success: true,
            error_code: STATUS_OK,
            error_message: String::new(),
            results,
        },
        Err(e) => {
            error!("Read error: {}", e);
            reader::ReadResponse {
                success: false,
                error_code: ERR_INTERNAL_ERROR,
                error_message: "read failed".to_string(),
                results: vec![],
            }
        }
    };

    // Calculate required buffer size: headers + records
    let total_record_bytes: usize = response
        .results
        .iter()
        .flat_map(|r| &r.records)
        .map(|rec| rec.value.len() + rec.key.as_ref().map(|k| k.len()).unwrap_or(0) + 32)
        .sum();
    let buf_size = (total_record_bytes + 1024).max(64 * 1024);

    encode_server_message_vec(ServerMessage::Read(response), buf_size + 16)
}

/// Process a read request.
async fn process_read<S: ObjectStore + Send + Sync>(
    req: reader::ReadRequest,
    state: &BrokerState<S>,
) -> Result<Vec<reader::PartitionResult>, BrokerError> {
    let mut results = Vec::with_capacity(req.reads.len().saturating_mul(2));

    for read in &req.reads {
        let batches: Vec<(i32, i64, i64, String, i64, i64, i64)> = sqlx::query_as(
            r#"
            SELECT schema_id, start_offset, end_offset, s3_key, byte_offset, byte_length, crc32
            FROM topic_batches
            WHERE topic_id = $1
              AND partition_id = $2
              AND end_offset > $3
            ORDER BY start_offset
            LIMIT 10
            "#,
        )
        .bind(read.topic_id.0 as i32)
        .bind(read.partition_id.0 as i32)
        .bind(read.offset.0 as i64)
        .fetch_all(&state.pool)
        .await?;

        let high_watermark: i64 = sqlx::query_scalar(
            "SELECT COALESCE(next_offset, 0) FROM partition_offsets WHERE topic_id = $1 AND partition_id = $2",
        )
        .bind(read.topic_id.0 as i32)
        .bind(read.partition_id.0 as i32)
        .fetch_optional(&state.pool)
        .await?
        .unwrap_or(0);

        let mut grouped_records = Vec::with_capacity(batches.len().max(1));
        let mut current_schema: Option<SchemaId> = None;
        let expected_records = batches
            .iter()
            .map(|(_, start_offset, end_offset, _, _, _, _)| {
                end_offset.saturating_sub(*start_offset) as usize
            })
            .sum::<usize>();
        let mut current_records = Vec::with_capacity(expected_records.min(16_384));
        let mut total_bytes: usize = 0;
        let mut reached_limit = false;
        let max_bytes = read.max_bytes as usize;

        for (sid, start_offset, end_offset, s3_key, byte_offset, byte_length, crc32) in batches {
            let schema_id = SchemaId(sid as u32);

            // S3 range read - read only this batch's bytes
            let segment_bytes = state
                .store
                .get_range(&s3_key, byte_offset as u64, byte_length as u64)
                .await?;

            // Create metadata for decoding (byte_offset=0 since we have just the batch bytes)
            let meta = SegmentMeta {
                topic_id: read.topic_id,
                partition_id: read.partition_id,
                schema_id,
                start_offset: Offset(start_offset as u64),
                end_offset: Offset(end_offset as u64),
                record_count: 0, // Not needed for read_segment
                byte_offset: 0,  // Data starts at 0 in our slice
                byte_length: byte_length as u64,
                ingest_time: 0,
                compression: Codec::Zstd,
                crc32: crc32 as u32,
            };

            let records = TbinReader::read_segment(&segment_bytes, &meta, true)?;
            let skip = (read.offset.0 as i64 - start_offset).max(0) as usize;
            if skip >= records.len() {
                continue;
            }

            if current_schema != Some(schema_id) {
                if let Some(prev_schema) = current_schema {
                    if !current_records.is_empty() {
                        grouped_records.push(reader::PartitionResult {
                            topic_id: read.topic_id,
                            partition_id: read.partition_id,
                            schema_id: prev_schema,
                            high_watermark: Offset(high_watermark as u64),
                            records: std::mem::take(&mut current_records),
                        });
                    }
                }
                current_schema = Some(schema_id);
            }

            for record in records.into_iter().skip(skip) {
                total_bytes +=
                    record.value.len() + record.key.as_ref().map(|k| k.len()).unwrap_or(0);
                current_records.push(record);
                if total_bytes >= max_bytes {
                    reached_limit = true;
                    break;
                }
            }

            if reached_limit {
                break;
            }
        }

        if let Some(schema_id) = current_schema {
            if !current_records.is_empty() {
                grouped_records.push(reader::PartitionResult {
                    topic_id: read.topic_id,
                    partition_id: read.partition_id,
                    schema_id,
                    high_watermark: Offset(high_watermark as u64),
                    records: current_records,
                });
            }
        } else {
            grouped_records.push(reader::PartitionResult {
                topic_id: read.topic_id,
                partition_id: read.partition_id,
                schema_id: SchemaId(0),
                high_watermark: Offset(high_watermark as u64),
                records: vec![],
            });
        }

        results.reserve(grouped_records.len());
        results.extend(grouped_records.into_iter());
    }

    Ok(results)
}

/// Flush loop that buffers requests and periodically flushes to S3.
#[tracing::instrument(level = "debug", skip(rx, state))]
async fn flush_loop<S: ObjectStore + Send + Sync + 'static>(
    mut rx: mpsc::Receiver<FlushCommand>,
    state: Arc<BrokerState<S>>,
) {
    let mut buffer = BrokerBuffer::with_config(state.config.buffer.clone());
    let mut flush_interval = tokio::time::interval(state.config.flush_interval);
    const MAX_COMMANDS_PER_DRAIN: usize = 2048;

    loop {
        tokio::select! {
            Some(cmd) = rx.recv() => {
                let mut should_force_flush = false;
                let mut should_shutdown = false;
                let mut processed = 0usize;
                let mut maybe_cmd = Some(cmd);

                while let Some(command) = maybe_cmd.take() {
                    processed += 1;
                    match command {
                        FlushCommand::Insert { writer_id, append_seq, batches, enqueued_at, response_tx } => {
                            let previous = state.pending_flush_commands.fetch_sub(1, Ordering::Relaxed);
                            FLUSH_QUEUE_DEPTH.set(previous.saturating_sub(1) as f64);
                            FLUSH_QUEUE_WAIT_SECONDS.observe(enqueued_at.elapsed().as_secs_f64());

                            // Insert into buffer
                            buffer.insert_with_sender(writer_id, append_seq, batches, response_tx);

                            // Update backpressure state
                            if buffer.should_apply_backpressure() {
                                *state.backpressure.write().await = true;
                                BACKPRESSURE_ACTIVE.set(1.0);
                            }
                        }
                        FlushCommand::ForceFlush => {
                            should_force_flush = true;
                        }
                        FlushCommand::Shutdown => {
                            should_shutdown = true;
                            break;
                        }
                    }

                    if processed >= MAX_COMMANDS_PER_DRAIN {
                        break;
                    }

                    maybe_cmd = rx.try_recv().ok();
                }

                BUFFER_SIZE_BYTES.set(buffer.size_bytes() as f64);

                if should_force_flush || buffer.should_flush() {
                    flush_buffer(&mut buffer, &state).await;
                    BUFFER_SIZE_BYTES.set(buffer.size_bytes() as f64);
                }

                if should_shutdown {
                    if !buffer.is_empty() {
                        flush_buffer(&mut buffer, &state).await;
                        BUFFER_SIZE_BYTES.set(buffer.size_bytes() as f64);
                    }
                    break;
                }
            }
            _ = flush_interval.tick() => {
                // Periodic flush
                if !buffer.is_empty() {
                    flush_buffer(&mut buffer, &state).await;
                    BUFFER_SIZE_BYTES.set(buffer.size_bytes() as f64);
                }
            }
        }
    }
}

/// Flush the buffer to S3 and commit to database.
#[tracing::instrument(level = "debug", skip(buffer, state))]
async fn flush_buffer<S: ObjectStore + Send + Sync>(
    buffer: &mut BrokerBuffer,
    state: &BrokerState<S>,
) {
    let flush_start = std::time::Instant::now();
    let drain_result = buffer.drain();

    if drain_result.batches.is_empty() {
        return;
    }

    let total_records = drain_result
        .batches
        .iter()
        .map(|batch| batch.records.len())
        .sum::<usize>();
    FLUSH_BATCH_SEGMENTS.observe(drain_result.batches.len() as f64);
    FLUSH_BATCH_PENDING_WRITERS.observe(drain_result.pending_writers.len() as f64);
    FLUSH_BATCH_RECORDS.observe(total_records as f64);
    let residency_now = Instant::now();
    for pending in &drain_result.pending_writers {
        FLUSH_BUFFER_RESIDENCY_SECONDS
            .observe(residency_now.duration_since(pending.buffered_at).as_secs_f64());
    }

    debug!(
        "Flushing {} batches, {} pending writers",
        drain_result.batches.len(),
        drain_result.pending_writers.len()
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
    let tbin_start = std::time::Instant::now();
    let (segment_metas, tbin_data) = {
        let _span = tracing::debug_span!("tbin_build").entered();
        let mut writer = TbinWriter::new();
        for batch in &drain_result.batches {
            if let Err(e) = writer.add_segment(batch) {
                error!("Failed to add batch to TBIN: {}", e);
                FLUSH_TOTAL.with_label_values(&["error"]).inc();
                ERRORS_TOTAL.with_label_values(&["tbin_write"]).inc();
                return;
            }
        }
        (writer.segment_metas().to_vec(), writer.finish())
    };
    FLUSH_TBIN_BUILD_SECONDS.observe(tbin_start.elapsed().as_secs_f64());

    // Write to S3
    let s3_start = std::time::Instant::now();
    if let Err(e) = state
        .store
        .put(&key, tbin_data)
        .instrument(tracing::debug_span!("s3_put"))
        .await
    {
        error!("Failed to write to S3: {}", e);
        FLUSH_TOTAL.with_label_values(&["error"]).inc();
        ERRORS_TOTAL.with_label_values(&["s3_put"]).inc();
        return;
    }
    S3_PUT_LATENCY_SECONDS.observe(s3_start.elapsed().as_secs_f64());

    // Commit to database
    let db_start = std::time::Instant::now();
    match commit_batch(
        &drain_result.batches,
        &segment_metas,
        &drain_result.key_to_index,
        &drain_result.pending_writers,
        &key,
        &state.pool,
    )
    .instrument(tracing::debug_span!("db_commit_batch"))
    .await
    {
        Ok(segment_offsets) => {
            DB_COMMIT_LATENCY_SECONDS.observe(db_start.elapsed().as_secs_f64());

            // Distribute append_acks to writers
            let ack_start = std::time::Instant::now();
            {
                let _span = tracing::debug_span!("distribute_acks").entered();
                BrokerBuffer::distribute_acks(drain_result, &segment_offsets);
            }
            FLUSH_ACK_DISTRIBUTE_SECONDS.observe(ack_start.elapsed().as_secs_f64());

            // Release backpressure if below low water mark
            if buffer.can_release_backpressure() {
                *state.backpressure.write().await = false;
                BACKPRESSURE_ACTIVE.set(0.0);
            }

            FLUSH_TOTAL.with_label_values(&["success"]).inc();
        }
        Err(e) => {
            error!("Failed to commit batch: {}", e);
            FLUSH_TOTAL.with_label_values(&["error"]).inc();
            ERRORS_TOTAL.with_label_values(&["db_commit"]).inc();
            // Acks won't be sent, writers will retry
        }
    }

    FLUSH_LATENCY_SECONDS.observe(flush_start.elapsed().as_secs_f64());
}

/// Commit a batch of batches to the database.
async fn commit_batch(
    batches: &[RecordBatch],
    segment_metas: &[SegmentMeta],
    key_to_index: &std::collections::HashMap<BatchKey, usize>,
    pending_writers: &[crate::buffer::PendingWriter],
    s3_key: &str,
    pool: &PgPool,
) -> Result<Vec<(u64, u64)>, sqlx::Error> {
    struct SegmentCommitInput {
        key: BatchKey,
        seg_idx: usize,
        record_count: i64,
    }

    let mut tx = pool.begin().await?;
    let ingest_time = chrono::Utc::now();

    let mut segment_offsets = vec![(0u64, 0u64); batches.len()];
    let mut segment_inputs: Vec<SegmentCommitInput> = key_to_index
        .iter()
        .map(|(key, &seg_idx)| SegmentCommitInput {
            key: *key,
            seg_idx,
            record_count: batches[seg_idx].records.len() as i64,
        })
        .collect();
    segment_inputs.sort_by_key(|s| s.seg_idx);

    let mut partition_to_segment_inputs: std::collections::HashMap<(i32, i32), Vec<usize>> =
        std::collections::HashMap::with_capacity(segment_inputs.len());
    let mut partition_deltas: std::collections::HashMap<(i32, i32), i64> =
        std::collections::HashMap::with_capacity(segment_inputs.len());

    for (input_idx, input) in segment_inputs.iter().enumerate() {
        let partition_key = (input.key.topic_id.0 as i32, input.key.partition_id.0 as i32);
        partition_to_segment_inputs
            .entry(partition_key)
            .or_default()
            .push(input_idx);
        *partition_deltas.entry(partition_key).or_insert(0) += input.record_count;
    }

    if !partition_deltas.is_empty() {
        let offsets_start = std::time::Instant::now();
        let partition_delta_rows: Vec<(i32, i32, i64)> = partition_deltas
            .iter()
            .map(|(&(topic_id, partition_id), &delta)| (topic_id, partition_id, delta))
            .collect();

        let mut offset_qb = QueryBuilder::<Postgres>::new(
            "INSERT INTO partition_offsets (topic_id, partition_id, next_offset) ",
        );
        offset_qb.push_values(
            partition_delta_rows,
            |mut b, (topic_id, partition_id, delta)| {
                b.push_bind(topic_id)
                    .push_bind(partition_id)
                    .push_bind(delta);
            },
        );
        offset_qb.push(
            " ON CONFLICT (topic_id, partition_id) \
             DO UPDATE SET next_offset = partition_offsets.next_offset + EXCLUDED.next_offset \
             RETURNING topic_id, partition_id, next_offset",
        );

        let updated_partition_offsets: Vec<(i32, i32, i64)> =
            offset_qb.build_query_as().fetch_all(&mut *tx).await?;
        let updated_offset_by_partition: std::collections::HashMap<(i32, i32), i64> =
            updated_partition_offsets
                .into_iter()
                .map(|(topic_id, partition_id, next_offset)| {
                    ((topic_id, partition_id), next_offset)
                })
                .collect();

        for (partition_key, input_indices) in &partition_to_segment_inputs {
            let total_delta = *partition_deltas.get(partition_key).ok_or_else(|| {
                sqlx::Error::Protocol(format!(
                    "missing partition delta for topic={} partition={}",
                    partition_key.0, partition_key.1
                ))
            })?;
            let partition_end =
                *updated_offset_by_partition
                    .get(partition_key)
                    .ok_or_else(|| {
                        sqlx::Error::Protocol(format!(
                            "missing updated offset for topic={} partition={}",
                            partition_key.0, partition_key.1
                        ))
                    })?;

            let mut cursor = partition_end - total_delta;
            for input_idx in input_indices {
                let input = &segment_inputs[*input_idx];
                let start_offset = cursor;
                let end_offset = start_offset + input.record_count;
                segment_offsets[input.seg_idx] = (start_offset as u64, end_offset as u64);
                cursor = end_offset;
            }
        }
        DB_COMMIT_OFFSETS_SECONDS.observe(offsets_start.elapsed().as_secs_f64());
    }

    if !segment_inputs.is_empty() {
        let topic_batches_start = std::time::Instant::now();
        let mut batch_qb = QueryBuilder::<Postgres>::new(
            "INSERT INTO topic_batches (\
                 topic_id, partition_id, schema_id, \
                 start_offset, end_offset, record_count, \
                 s3_key, byte_offset, byte_length, ingest_time, crc32\
             ) ",
        );
        batch_qb.push_values(&segment_inputs, |mut b, input| {
            let meta = &segment_metas[input.seg_idx];
            let (start_offset, end_offset) = segment_offsets[input.seg_idx];
            b.push_bind(input.key.topic_id.0 as i32)
                .push_bind(input.key.partition_id.0 as i32)
                .push_bind(input.key.schema_id.0 as i32)
                .push_bind(start_offset as i64)
                .push_bind(end_offset as i64)
                .push_bind(input.record_count as i32)
                .push_bind(s3_key)
                .push_bind(meta.byte_offset as i64)
                .push_bind(meta.byte_length as i64)
                .push_bind(ingest_time)
                .push_bind(meta.crc32 as i64);
        });
        batch_qb.build().execute(&mut *tx).await?;
        DB_COMMIT_TOPIC_BATCHES_SECONDS.observe(topic_batches_start.elapsed().as_secs_f64());
    }

    // Persist latest writer dedup state for this flush in a single DB statement.
    // This keeps dedup durable across cache evictions, restarts, and multi-broker retries.
    if !pending_writers.is_empty() {
        let mut latest_by_writer: std::collections::HashMap<
            WriterId,
            (AppendSeq, Vec<turbine_common::types::BatchAck>),
        > = std::collections::HashMap::with_capacity(pending_writers.len());

        for pending in pending_writers {
            let append_acks =
                BrokerBuffer::calculate_acks_for_pending(pending, key_to_index, &segment_offsets);
            match latest_by_writer.get(&pending.writer_id) {
                Some((existing_seq, _)) if existing_seq.0 >= pending.append_seq.0 => {}
                _ => {
                    latest_by_writer.insert(pending.writer_id, (pending.append_seq, append_acks));
                }
            }
        }

        if !latest_by_writer.is_empty() {
            let prep_start = std::time::Instant::now();
            let rows = build_writer_state_rows(latest_by_writer)?;
            DB_COMMIT_WRITER_STATE_PREP_SECONDS.observe(prep_start.elapsed().as_secs_f64());

            let upsert_start = std::time::Instant::now();
            let mut qb = QueryBuilder::<Postgres>::new(
                "INSERT INTO writer_state (writer_id, last_seq, last_acks) ",
            );
            qb.push_values(rows, |mut b, (writer_id, append_seq, acks_json)| {
                b.push_bind(writer_id.0)
                    .push_bind(append_seq.0 as i64)
                    .push_bind(acks_json);
            });
            qb.push(
                " ON CONFLICT (writer_id) DO UPDATE SET \
                 last_seq = EXCLUDED.last_seq, \
                 last_acks = EXCLUDED.last_acks, \
                 updated_at = NOW() \
                 WHERE writer_state.last_seq < EXCLUDED.last_seq",
            );
            qb.build().execute(&mut *tx).await?;
            DB_COMMIT_WRITER_STATE_UPSERT_SECONDS.observe(upsert_start.elapsed().as_secs_f64());
        }
    }

    let tx_commit_start = std::time::Instant::now();
    tx.commit().await?;
    DB_COMMIT_TX_COMMIT_SECONDS.observe(tx_commit_start.elapsed().as_secs_f64());

    Ok(segment_offsets)
}

fn build_writer_state_rows(
    latest_by_writer: std::collections::HashMap<
        WriterId,
        (AppendSeq, Vec<turbine_common::types::BatchAck>),
    >,
) -> Result<Vec<(WriterId, AppendSeq, serde_json::Value)>, sqlx::Error> {
    let mut bump = WRITER_STATE_ROWS_BUMP
        .lock()
        .expect("writer state bump mutex poisoned");
    bump.reset();
    let mut rows = BumpVec::with_capacity_in(latest_by_writer.len(), &bump);
    for (writer_id, (append_seq, append_acks)) in latest_by_writer {
        let acks_json = serde_json::to_value(append_acks)
            .map_err(|e| sqlx::Error::Protocol(format!("batch ack serialization: {e}")))?;
        rows.push((writer_id, append_seq, acks_json));
    }
    Ok(rows.into_iter().collect())
}

// ============ Reader Group Handlers ============

/// Handle a JoinGroupRequest.
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(group_id = %req.group_id, reader_id = %req.reader_id, topic_count = req.topic_ids.len())
)]
async fn handle_join_group<S: ObjectStore + Send + Sync>(
    req: reader::JoinGroupRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    // For now, we only support single topic joins
    let topic_id = req.topic_ids.first().copied().unwrap_or(TopicId(0));

    let result = state
        .coordinator
        .join_group(&req.group_id, topic_id, &req.reader_id)
        .await;

    let response = match result {
        Ok(join_result) => reader::JoinGroupResponse {
            success: true,
            error_code: STATUS_OK,
            error_message: String::new(),
            generation: join_result.generation,
            assignments: join_result
                .assignments
                .into_iter()
                .map(|a| reader::PartitionAssignment {
                    topic_id,
                    partition_id: a.partition_id,
                    committed_offset: a.committed_offset,
                })
                .collect(),
        },
        Err(e) => {
            error!("JoinGroup error: {}", e);
            reader::JoinGroupResponse {
                success: false,
                error_code: ERR_INTERNAL_ERROR,
                error_message: "join group failed".to_string(),
                generation: Generation(0),
                assignments: vec![],
            }
        }
    };

    encode_server_message_vec(ServerMessage::JoinGroup(response), RESPONSE_CAPACITY_LARGE)
}

/// Handle a HeartbeatRequest.
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(group_id = %req.group_id, reader_id = %req.reader_id, topic_id = req.topic_id.0, generation = req.generation.0)
)]
async fn handle_heartbeat<S: ObjectStore + Send + Sync>(
    req: reader::HeartbeatRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    let result = state
        .coordinator
        .heartbeat(
            &req.group_id,
            req.topic_id,
            &req.reader_id,
            req.generation,
        )
        .await;

    let response = match result {
        Ok(hb_result) => reader::HeartbeatResponseExt {
            success: true,
            error_code: STATUS_OK,
            error_message: String::new(),
            generation: hb_result.generation,
            status: match hb_result.status {
                crate::coordinator::HeartbeatStatus::Ok => reader::HeartbeatStatus::Ok,
                crate::coordinator::HeartbeatStatus::RebalanceNeeded => {
                    reader::HeartbeatStatus::RebalanceNeeded
                }
                crate::coordinator::HeartbeatStatus::UnknownMember => {
                    reader::HeartbeatStatus::UnknownMember
                }
            },
        },
        Err(e) => {
            error!("Heartbeat error: {}", e);
            reader::HeartbeatResponseExt {
                success: false,
                error_code: ERR_INTERNAL_ERROR,
                error_message: "heartbeat failed".to_string(),
                generation: Generation(0),
                status: reader::HeartbeatStatus::UnknownMember,
            }
        }
    };

    encode_server_message_vec(ServerMessage::Heartbeat(response), RESPONSE_CAPACITY_SMALL)
}

/// Handle a RejoinRequest.
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(group_id = %req.group_id, reader_id = %req.reader_id, topic_id = req.topic_id.0, generation = req.generation.0)
)]
async fn handle_rejoin<S: ObjectStore + Send + Sync>(
    req: reader::RejoinRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    let result = state
        .coordinator
        .rejoin(
            &req.group_id,
            req.topic_id,
            &req.reader_id,
            req.generation,
        )
        .await;

    let response = match result {
        Ok(rejoin_result) => reader::RejoinResponse {
            success: true,
            error_code: STATUS_OK,
            error_message: String::new(),
            generation: rejoin_result.generation,
            status: match rejoin_result.status {
                crate::coordinator::RejoinStatus::Ok => reader::RejoinStatus::Ok,
                crate::coordinator::RejoinStatus::RebalanceNeeded => {
                    reader::RejoinStatus::RebalanceNeeded
                }
            },
            assignments: rejoin_result
                .assignments
                .into_iter()
                .map(|a| reader::PartitionAssignment {
                    topic_id: req.topic_id,
                    partition_id: a.partition_id,
                    committed_offset: a.committed_offset,
                })
                .collect(),
        },
        Err(e) => {
            error!("Rejoin error: {}", e);
            reader::RejoinResponse {
                success: false,
                error_code: ERR_INTERNAL_ERROR,
                error_message: "rejoin failed".to_string(),
                generation: Generation(0),
                status: reader::RejoinStatus::RebalanceNeeded,
                assignments: vec![],
            }
        }
    };

    encode_server_message_vec(ServerMessage::Rejoin(response), RESPONSE_CAPACITY_LARGE)
}

/// Handle a LeaveGroupRequest.
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(group_id = %req.group_id, reader_id = %req.reader_id, topic_id = req.topic_id.0)
)]
async fn handle_leave_group<S: ObjectStore + Send + Sync>(
    req: reader::LeaveGroupRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    let result = state
        .coordinator
        .leave_group(&req.group_id, req.topic_id, &req.reader_id)
        .await;

    let response = reader::LeaveGroupResponse {
        success: result.is_ok(),
        error_code: if result.is_ok() {
            STATUS_OK
        } else {
            ERR_INTERNAL_ERROR
        },
        error_message: if result.is_ok() {
            String::new()
        } else {
            "leave group failed".to_string()
        },
    };

    if let Err(e) = result {
        error!("LeaveGroup error: {}", e);
    }

    encode_server_message_vec(ServerMessage::LeaveGroup(response), RESPONSE_CAPACITY_SMALL)
}

/// Handle a CommitRequest.
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(group_id = %req.group_id, reader_id = %req.reader_id, generation = req.generation.0, commit_count = req.commits.len())
)]
async fn handle_commit<S: ObjectStore + Send + Sync>(
    req: reader::CommitRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    let mut all_ok = true;
    let mut error_code = STATUS_OK;
    let mut error_message = String::new();

    for commit in &req.commits {
        let result = state
            .coordinator
            .commit_offset(
                &req.group_id,
                commit.topic_id,
                &req.reader_id,
                req.generation,
                commit.partition_id,
                commit.offset,
            )
            .await;

        match result {
            Ok(crate::coordinator::CommitStatus::Ok) => {}
            Ok(crate::coordinator::CommitStatus::StaleGeneration) => {
                warn!(
                    "Commit rejected: stale generation for reader {} on partition {}",
                    req.reader_id, commit.partition_id.0
                );
                all_ok = false;
                error_code = ERR_NOT_OWNER;
                error_message = "commit rejected: stale generation".to_string();
            }
            Ok(crate::coordinator::CommitStatus::NotOwner) => {
                warn!(
                    "Commit rejected: reader {} doesn't own partition {}",
                    req.reader_id, commit.partition_id.0
                );
                all_ok = false;
                error_code = ERR_NOT_OWNER;
                error_message = "commit rejected: not owner".to_string();
            }
            Err(e) => {
                error!("Commit error: {}", e);
                all_ok = false;
                error_code = ERR_INTERNAL_ERROR;
                error_message = "commit failed".to_string();
            }
        }
    }

    let response = reader::CommitResponse {
        success: all_ok,
        error_code,
        error_message,
    };

    encode_server_message_vec(ServerMessage::Commit(response), RESPONSE_CAPACITY_SMALL)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{AssertUnwindSafe, catch_unwind};

    #[test]
    fn test_batched_agent_config_default() {
        let config = BrokerConfig::default();
        assert_eq!(config.bind_addr.port(), 9000);
        assert_eq!(config.flush_interval, Duration::from_millis(100));
    }

    #[test]
    fn test_encode_error_response_shapes() {
        let append = encode_error_response(ErrorResponseKind::Append, 123, "append error");
        let (produce_msg, produce_len) = turbine_wire::decode_server_message(&append).unwrap();
        assert_eq!(produce_len, append.len());
        let produce_resp = match produce_msg {
            ServerMessage::Append(resp) => resp,
            _ => panic!("expected append response"),
        };
        assert_eq!(produce_resp.append_seq.0, 0);
        assert!(!produce_resp.success);
        assert_eq!(produce_resp.error_code, 123);
        assert_eq!(produce_resp.error_message, "append error");
        assert!(produce_resp.append_acks.is_empty());

        let read = encode_error_response(ErrorResponseKind::Read, 124, "read error");
        let (fetch_msg, fetch_len) = turbine_wire::decode_server_message(&read).unwrap();
        assert_eq!(fetch_len, read.len());
        let fetch_resp = match fetch_msg {
            ServerMessage::Read(resp) => resp,
            _ => panic!("expected read response"),
        };
        assert!(!fetch_resp.success);
        assert_eq!(fetch_resp.error_code, 124);
        assert_eq!(fetch_resp.error_message, "read error");
        assert!(fetch_resp.results.is_empty());

        let join = encode_error_response(ErrorResponseKind::JoinGroup, 125, "join error");
        let (join_msg, join_len) = turbine_wire::decode_server_message(&join).unwrap();
        assert_eq!(join_len, join.len());
        let join_resp = match join_msg {
            ServerMessage::JoinGroup(resp) => resp,
            _ => panic!("expected join response"),
        };
        assert!(!join_resp.success);
        assert_eq!(join_resp.error_code, 125);
        assert_eq!(join_resp.error_message, "join error");
        assert_eq!(join_resp.generation.0, 0);
        assert!(join_resp.assignments.is_empty());

        let commit = encode_error_response(ErrorResponseKind::Commit, 126, "commit error");
        let (commit_msg, commit_len) = turbine_wire::decode_server_message(&commit).unwrap();
        assert_eq!(commit_len, commit.len());
        let commit_resp = match commit_msg {
            ServerMessage::Commit(resp) => resp,
            _ => panic!("expected commit response"),
        };
        assert!(!commit_resp.success);
        assert_eq!(commit_resp.error_code, 126);
        assert_eq!(commit_resp.error_message, "commit error");
    }

    #[test]
    fn test_large_join_response_encoding_does_not_panic() {
        let assignment_count = 20_000u32;
        let response = reader::JoinGroupResponse {
            success: true,
            error_code: STATUS_OK,
            error_message: String::new(),
            generation: Generation(7),
            assignments: (0..assignment_count)
                .map(|partition_id| reader::PartitionAssignment {
                    topic_id: TopicId(42),
                    partition_id: turbine_common::ids::PartitionId(partition_id),
                    committed_offset: Offset(0),
                })
                .collect(),
        };

        let encoded = catch_unwind(AssertUnwindSafe(|| {
            encode_server_message_vec(ServerMessage::JoinGroup(response), 8192)
        }));
        assert!(
            encoded.is_ok(),
            "large JoinGroup response encoding panicked"
        );

        let encoded = encoded.expect("expected encoded response bytes");
        let (decoded, used) = turbine_wire::decode_server_message(&encoded).unwrap();
        assert_eq!(used, encoded.len());
        let join_resp = match decoded {
            ServerMessage::JoinGroup(resp) => resp,
            _ => panic!("expected join response"),
        };
        assert!(join_resp.success);
        assert_eq!(join_resp.generation.0, 7);
        assert_eq!(join_resp.assignments.len(), assignment_count as usize);
    }
}
