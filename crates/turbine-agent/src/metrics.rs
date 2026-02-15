//! Prometheus metrics for the turbine broker.

use axum::http::StatusCode;
use axum::response::IntoResponse;
use lazy_static::lazy_static;
use prometheus::{
    Counter, CounterVec, Encoder, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec, Opts,
    Registry, TextEncoder,
};

lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    // Append metrics
    pub static ref APPEND_REQUESTS_TOTAL: Counter = Counter::with_opts(
        Opts::new("turbine_append_requests_total", "Total number of append requests")
    ).unwrap();

    pub static ref APPEND_LATENCY_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("turbine_append_latency_seconds", "Append request latency in seconds")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])
    ).unwrap();

    pub static ref APPEND_BYTES_TOTAL: Counter = Counter::with_opts(
        Opts::new("turbine_append_bytes_total", "Total bytes appended")
    ).unwrap();

    pub static ref APPEND_ENQUEUE_TO_ACK_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "turbine_append_enqueue_to_ack_seconds",
            "Time from enqueueing append to receiving ack"
        )
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])
    ).unwrap();

    pub static ref INFLIGHT_APPEND_REQUESTS: Gauge = Gauge::with_opts(
        Opts::new(
            "turbine_inflight_append_requests",
            "Current number of in-flight append requests tracked by the broker"
        )
    ).unwrap();

    // Buffer metrics
    pub static ref BUFFER_SIZE_BYTES: Gauge = Gauge::with_opts(
        Opts::new("turbine_buffer_size_bytes", "Current buffer size in bytes")
    ).unwrap();

    pub static ref FLUSH_QUEUE_DEPTH: Gauge = Gauge::with_opts(
        Opts::new("turbine_flush_queue_depth", "Current number of queued append commands")
    ).unwrap();

    pub static ref FLUSH_QUEUE_WAIT_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "turbine_flush_queue_wait_seconds",
            "Time spent waiting in the flush queue before processing"
        )
            .buckets(vec![0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])
    ).unwrap();

    pub static ref FLUSH_BUFFER_RESIDENCY_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "turbine_flush_buffer_residency_seconds",
            "Time an append request spends buffered before flush completion"
        )
            .buckets(vec![0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])
    ).unwrap();

    // Flush metrics
    pub static ref FLUSH_TOTAL: CounterVec = CounterVec::new(
        Opts::new("turbine_flush_total", "Total number of buffer flushes"),
        &["status"]
    ).unwrap();

    pub static ref FLUSH_LATENCY_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("turbine_flush_latency_seconds", "Buffer flush latency in seconds")
            .buckets(vec![0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0])
    ).unwrap();

    pub static ref FLUSH_TBIN_BUILD_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "turbine_flush_tbin_build_seconds",
            "Time spent constructing TBIN payloads per flush"
        )
            .buckets(vec![0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])
    ).unwrap();

    pub static ref FLUSH_ACK_DISTRIBUTE_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "turbine_flush_ack_distribute_seconds",
            "Time spent calculating and distributing writer acknowledgements"
        )
            .buckets(vec![0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])
    ).unwrap();

    pub static ref FLUSH_BATCH_SEGMENTS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "turbine_flush_batch_segments",
            "Number of merged batches per flush batch"
        )
            .buckets(vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0])
    ).unwrap();

    pub static ref FLUSH_BATCH_PENDING_WRITERS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "turbine_flush_batch_pending_writers",
            "Number of pending writers acked per flush batch"
        )
            .buckets(vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0, 1024.0])
    ).unwrap();

    pub static ref FLUSH_BATCH_RECORDS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "turbine_flush_batch_records",
            "Number of records written per flush batch"
        )
            .buckets(vec![1.0, 4.0, 16.0, 64.0, 256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0])
    ).unwrap();

    // S3 metrics
    pub static ref S3_PUT_LATENCY_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("turbine_s3_put_latency_seconds", "S3 PUT latency in seconds")
            .buckets(vec![0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0])
    ).unwrap();

    // Database metrics
    pub static ref DB_COMMIT_LATENCY_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("turbine_db_commit_latency_seconds", "Database commit latency in seconds")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5])
    ).unwrap();

    pub static ref DB_COMMIT_OFFSETS_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "turbine_db_commit_offsets_seconds",
            "Time spent updating partition offsets in commit_batch"
        )
            .buckets(vec![0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])
    ).unwrap();

    pub static ref DB_COMMIT_TOPIC_BATCHES_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "turbine_db_commit_topic_batches_seconds",
            "Time spent inserting topic_batches rows in commit_batch"
        )
            .buckets(vec![0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])
    ).unwrap();

    pub static ref DB_COMMIT_WRITER_STATE_PREP_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "turbine_db_commit_writer_state_prep_seconds",
            "Time spent preparing writer_state rows in commit_batch"
        )
            .buckets(vec![0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])
    ).unwrap();

    pub static ref DB_COMMIT_WRITER_STATE_UPSERT_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "turbine_db_commit_writer_state_upsert_seconds",
            "Time spent upserting writer_state rows in commit_batch"
        )
            .buckets(vec![0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])
    ).unwrap();

    pub static ref DB_COMMIT_TX_COMMIT_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "turbine_db_commit_tx_commit_seconds",
            "Time spent committing the SQL transaction in commit_batch"
        )
            .buckets(vec![0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])
    ).unwrap();

    // Read metrics
    pub static ref READ_REQUESTS_TOTAL: Counter = Counter::with_opts(
        Opts::new("turbine_read_requests_total", "Total number of read requests")
    ).unwrap();

    pub static ref READ_LATENCY_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new("turbine_read_latency_seconds", "Read request latency in seconds")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0])
    ).unwrap();

    // Connection metrics
    pub static ref ACTIVE_CONNECTIONS: Gauge = Gauge::with_opts(
        Opts::new("turbine_active_connections", "Number of active WebSocket connections")
    ).unwrap();

    pub static ref WS_DECODE_CLIENT_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "turbine_ws_decode_client_seconds",
            "Time spent decoding client frames"
        )
            .buckets(vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05])
    ).unwrap();

    pub static ref WS_HANDLE_MESSAGE_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "turbine_ws_handle_message_seconds",
            "End-to-end time spent handling a decoded client message by kind"
        )
            .buckets(vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5]),
        &["kind"]
    ).unwrap();

    pub static ref WS_ENCODE_SERVER_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "turbine_ws_encode_server_seconds",
            "Time spent encoding server responses by kind"
        )
            .buckets(vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05]),
        &["kind"]
    ).unwrap();

    pub static ref WS_OUTBOUND_CHANNEL_WAIT_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "turbine_ws_outbound_channel_wait_seconds",
            "Time waiting to reserve capacity on the outbound writer channel"
        )
            .buckets(vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25])
    ).unwrap();

    pub static ref WS_OUTBOUND_QUEUE_WAIT_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "turbine_ws_outbound_queue_wait_seconds",
            "Time an outbound message waits in the writer queue before being sent"
        )
            .buckets(vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.25])
    ).unwrap();

    pub static ref WS_WRITE_SECONDS: HistogramVec = HistogramVec::new(
        HistogramOpts::new(
            "turbine_ws_write_seconds",
            "Time spent in websocket write.send() by outbound message kind"
        )
            .buckets(vec![0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1]),
        &["kind"]
    ).unwrap();

    // Error metrics
    pub static ref ERRORS_TOTAL: CounterVec = CounterVec::new(
        Opts::new("turbine_errors_total", "Total number of errors by type"),
        &["type"]
    ).unwrap();

    pub static ref DEDUP_CHECK_LATENCY_SECONDS: Histogram = Histogram::with_opts(
        HistogramOpts::new(
            "turbine_dedup_check_latency_seconds",
            "Deduplication check latency in seconds"
        )
            .buckets(vec![0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1])
    ).unwrap();

    pub static ref DEDUP_RESULT_TOTAL: CounterVec = CounterVec::new(
        Opts::new("turbine_dedup_result_total", "Deduplication outcomes"),
        &["result"]
    ).unwrap();

    // Backpressure metrics
    pub static ref BACKPRESSURE_ACTIVE: Gauge = Gauge::with_opts(
        Opts::new("turbine_backpressure_active", "Whether backpressure is currently active (0 or 1)")
    ).unwrap();

    // Reader group metrics
    pub static ref READER_GROUPS_ACTIVE: GaugeVec = GaugeVec::new(
        Opts::new("turbine_reader_groups_active", "Number of active readers per group"),
        &["group"]
    ).unwrap();

    pub static ref REBALANCES_TOTAL: Counter = Counter::with_opts(
        Opts::new("turbine_rebalances_total", "Total number of reader group rebalances")
    ).unwrap();
}

/// Register all metrics with the registry.
pub fn register_metrics() {
    REGISTRY
        .register(Box::new(APPEND_REQUESTS_TOTAL.clone()))
        .ok();
    REGISTRY
        .register(Box::new(APPEND_LATENCY_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(APPEND_BYTES_TOTAL.clone()))
        .ok();
    REGISTRY
        .register(Box::new(APPEND_ENQUEUE_TO_ACK_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(INFLIGHT_APPEND_REQUESTS.clone()))
        .ok();
    REGISTRY.register(Box::new(BUFFER_SIZE_BYTES.clone())).ok();
    REGISTRY.register(Box::new(FLUSH_QUEUE_DEPTH.clone())).ok();
    REGISTRY
        .register(Box::new(FLUSH_QUEUE_WAIT_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(FLUSH_BUFFER_RESIDENCY_SECONDS.clone()))
        .ok();
    REGISTRY.register(Box::new(FLUSH_TOTAL.clone())).ok();
    REGISTRY
        .register(Box::new(FLUSH_LATENCY_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(FLUSH_TBIN_BUILD_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(FLUSH_ACK_DISTRIBUTE_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(FLUSH_BATCH_SEGMENTS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(FLUSH_BATCH_PENDING_WRITERS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(FLUSH_BATCH_RECORDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(S3_PUT_LATENCY_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(DB_COMMIT_LATENCY_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(DB_COMMIT_OFFSETS_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(DB_COMMIT_TOPIC_BATCHES_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(DB_COMMIT_WRITER_STATE_PREP_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(DB_COMMIT_WRITER_STATE_UPSERT_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(DB_COMMIT_TX_COMMIT_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(READ_REQUESTS_TOTAL.clone()))
        .ok();
    REGISTRY
        .register(Box::new(READ_LATENCY_SECONDS.clone()))
        .ok();
    REGISTRY.register(Box::new(ACTIVE_CONNECTIONS.clone())).ok();
    REGISTRY
        .register(Box::new(WS_DECODE_CLIENT_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(WS_HANDLE_MESSAGE_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(WS_ENCODE_SERVER_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(WS_OUTBOUND_CHANNEL_WAIT_SECONDS.clone()))
        .ok();
    REGISTRY
        .register(Box::new(WS_OUTBOUND_QUEUE_WAIT_SECONDS.clone()))
        .ok();
    REGISTRY.register(Box::new(WS_WRITE_SECONDS.clone())).ok();
    REGISTRY.register(Box::new(ERRORS_TOTAL.clone())).ok();
    REGISTRY
        .register(Box::new(DEDUP_CHECK_LATENCY_SECONDS.clone()))
        .ok();
    REGISTRY.register(Box::new(DEDUP_RESULT_TOTAL.clone())).ok();
    REGISTRY
        .register(Box::new(BACKPRESSURE_ACTIVE.clone()))
        .ok();
    REGISTRY
        .register(Box::new(READER_GROUPS_ACTIVE.clone()))
        .ok();
    REGISTRY.register(Box::new(REBALANCES_TOTAL.clone())).ok();
}

const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// Axum handler for the /metrics endpoint.
pub async fn metrics_handler() -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();

    let mut buffer = Vec::new();
    match encoder.encode(&metric_families, &mut buffer) {
        Ok(()) => (
            StatusCode::OK,
            [("Content-Type", PROMETHEUS_CONTENT_TYPE)],
            buffer,
        ),
        Err(e) => {
            tracing::error!("Failed to encode metrics: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [("Content-Type", "text/plain; charset=utf-8")],
                format!("Failed to encode metrics: {}", e).into_bytes(),
            )
        }
    }
}

/// Guard that increments a counter on creation and decrements on drop.
pub struct ConnectionGuard;

impl ConnectionGuard {
    pub fn new() -> Self {
        ACTIVE_CONNECTIONS.inc();
        Self
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        ACTIVE_CONNECTIONS.dec();
    }
}

impl Default for ConnectionGuard {
    fn default() -> Self {
        Self::new()
    }
}

/// Timer that records latency to a histogram when dropped.
pub struct LatencyTimer {
    histogram: Histogram,
    start: std::time::Instant,
}

impl LatencyTimer {
    pub fn new(histogram: &Histogram) -> Self {
        Self {
            histogram: histogram.clone(),
            start: std::time::Instant::now(),
        }
    }

    pub fn observe(self) {
        // Drop will record the latency
    }
}

impl Drop for LatencyTimer {
    fn drop(&mut self) {
        let elapsed = self.start.elapsed().as_secs_f64();
        self.histogram.observe(elapsed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_metrics() {
        // Just ensure it doesn't panic
        register_metrics();
    }

    #[test]
    fn test_connection_guard() {
        let initial = ACTIVE_CONNECTIONS.get() as i64;
        {
            let _guard = ConnectionGuard::new();
            assert_eq!(ACTIVE_CONNECTIONS.get() as i64, initial + 1);
        }
        assert_eq!(ACTIVE_CONNECTIONS.get() as i64, initial);
    }
}
