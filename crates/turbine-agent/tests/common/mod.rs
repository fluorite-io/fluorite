//! Common test utilities for integration tests.

#![allow(dead_code)]

use bytes::Bytes;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Instant;
use tempfile::TempDir;
use turbine_agent::{Coordinator, CoordinatorConfig, LocalFsStore, ObjectStore, TbinWriter};
use turbine_common::ids::{Offset, PartitionId, WriterId, SchemaId, TopicId};
use turbine_common::types::{Record, RecordBatch};

// ============ Test-Only Broker Types ============
// These are simpler versions of the production types for testing core logic.

/// Simple broker configuration for tests.
#[derive(Debug, Clone)]
pub struct AgentConfig {
    pub bind_addr: SocketAddr,
    pub bucket: String,
    pub key_prefix: String,
}

/// Simple broker state for tests (doesn't spawn background tasks).
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

static TEST_DB_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Test database configuration.
pub struct TestDb {
    pub pool: PgPool,
    #[allow(dead_code)]
    pub db_name: String,
    #[allow(dead_code)]
    admin_pool: PgPool,
}

impl TestDb {
    /// Create a new test database with a unique name.
    ///
    /// Requires DATABASE_URL to point to a Postgres instance (not a specific database).
    /// Example: postgres://postgres:postgres@localhost:5433
    pub async fn new() -> Self {
        let base_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5433".to_string());

        // Connect to postgres database for admin operations
        let admin_url = format!("{}/postgres", base_url);
        let admin_pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&admin_url)
            .await
            .expect("Failed to connect to postgres database");

        // Create unique database name
        let counter = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let db_name = format!("turbine_test_{}_{}", std::process::id(), counter);

        // Create database
        sqlx::query(&format!("DROP DATABASE IF EXISTS {}", db_name))
            .execute(&admin_pool)
            .await
            .expect("Failed to drop existing test database");

        sqlx::query(&format!("CREATE DATABASE {}", db_name))
            .execute(&admin_pool)
            .await
            .expect("Failed to create test database");

        // Connect to test database
        let test_url = format!("{}/{}", base_url, db_name);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&test_url)
            .await
            .expect("Failed to connect to test database");

        // Run migrations
        Self::run_migrations(&pool).await;

        Self {
            pool,
            db_name,
            admin_pool,
        }
    }

    /// Run database migrations.
    async fn run_migrations(pool: &PgPool) {
        let migration_path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations/001_init.sql");
        let migration_sql = std::fs::read_to_string(&migration_path).unwrap_or_else(|e| {
            panic!(
                "Failed to read migration file at {}: {}",
                migration_path.display(),
                e
            )
        });

        sqlx::raw_sql(&migration_sql)
            .execute(pool)
            .await
            .expect("Failed to execute consolidated migration");
    }

    /// Create a test topic with partitions.
    pub async fn create_topic(&self, name: &str, partition_count: i32) -> i32 {
        let topic_id: i32 = sqlx::query_scalar(
            "INSERT INTO topics (name, partition_count) VALUES ($1, $2) RETURNING topic_id",
        )
        .bind(name)
        .bind(partition_count)
        .fetch_one(&self.pool)
        .await
        .expect("Failed to create topic");

        topic_id
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        // Note: Can't do async cleanup in Drop, so we leave the database
        // In a real test suite, you'd want to clean up old test databases periodically
        // Or use a test harness that supports async cleanup
    }
}

// ============ Append Records Helper ============

static APPEND_KEY_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Append records and return (start_offset, end_offset).
///
/// This is a common helper for jepsen tests that need to append records
/// directly to the database and object store.
pub async fn produce_records<S: ObjectStore + Send + Sync>(
    state: &AgentState<S>,
    topic_id: TopicId,
    partition_id: PartitionId,
    records: Vec<Record>,
) -> Result<(Offset, Offset), Box<dyn std::error::Error + Send + Sync>> {
    let counter = APPEND_KEY_COUNTER.fetch_add(1, Ordering::SeqCst);
    let timestamp = chrono::Utc::now().timestamp_millis();
    let key = format!(
        "{}/{}/{}-{}.tbin",
        state.config.key_prefix,
        chrono::Utc::now().format("%Y-%m-%d"),
        timestamp,
        counter
    );

    let batch = RecordBatch {
        topic_id,
        partition_id,
        schema_id: SchemaId(1),
        records: records.clone(),
    };

    let mut writer = TbinWriter::new();
    writer.add_segment(&batch)?;
    let segment_metas = writer.segment_metas().to_vec();
    let tbin_data = writer.finish();
    state.store.put(&key, tbin_data).await?;

    let mut tx = state.pool.begin().await?;
    let ingest_time = chrono::Utc::now();

    let current_offset: i64 = sqlx::query_scalar(
        r#"
        SELECT next_offset FROM partition_offsets
        WHERE topic_id = $1 AND partition_id = $2
        FOR UPDATE
        "#,
    )
    .bind(topic_id.0 as i32)
    .bind(partition_id.0 as i32)
    .fetch_optional(&mut *tx)
    .await?
    .unwrap_or(0);

    let record_count = records.len() as i64;
    let start_offset = current_offset;
    let end_offset = current_offset + record_count;

    sqlx::query(
        r#"
        INSERT INTO partition_offsets (topic_id, partition_id, next_offset)
        VALUES ($1, $2, $3)
        ON CONFLICT (topic_id, partition_id)
        DO UPDATE SET next_offset = $3
        "#,
    )
    .bind(topic_id.0 as i32)
    .bind(partition_id.0 as i32)
    .bind(end_offset)
    .execute(&mut *tx)
    .await?;

    // Use batch metadata for byte_offset, byte_length, crc32
    let meta = &segment_metas[0];

    sqlx::query(
        r#"
        INSERT INTO topic_batches (
            topic_id, partition_id, schema_id,
            start_offset, end_offset, record_count,
            s3_key, byte_offset, byte_length, ingest_time, crc32
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        "#,
    )
    .bind(topic_id.0 as i32)
    .bind(partition_id.0 as i32)
    .bind(1i32)
    .bind(start_offset)
    .bind(end_offset)
    .bind(record_count as i32)
    .bind(&key)
    .bind(meta.byte_offset as i64)
    .bind(meta.byte_length as i64)
    .bind(ingest_time)
    .bind(meta.crc32 as i64)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    Ok((Offset(start_offset as u64), Offset(end_offset as u64)))
}

/// Clean up test database (call manually if needed).
#[allow(dead_code)]
pub async fn cleanup_test_db(admin_pool: &PgPool, db_name: &str) {
    // Terminate connections
    let _ = sqlx::query(&format!(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}'",
        db_name
    ))
    .execute(admin_pool)
    .await;

    // Drop database
    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {}", db_name))
        .execute(admin_pool)
        .await;
}

// ============ Fault Injection Infrastructure ============
// These utilities are provided for tests - warnings about unused code are expected
// since different test files use different subsets of this infrastructure.

/// Wraps an ObjectStore with fault injection capability.
#[derive(Clone)]
pub struct FaultyObjectStore {
    inner: Arc<LocalFsStore>,
    fail_next_put: Arc<AtomicBool>,
    fail_next_get: Arc<AtomicBool>,
    /// 1-indexed: fail on this put number and all subsequent puts
    fail_on_put_n: Arc<AtomicU32>,
    put_count: Arc<AtomicU32>,
}

impl FaultyObjectStore {
    /// Create a new faulty object store wrapping a local FS store.
    pub fn new(store: LocalFsStore) -> Self {
        Self {
            inner: Arc::new(store),
            fail_next_put: Arc::new(AtomicBool::new(false)),
            fail_next_get: Arc::new(AtomicBool::new(false)),
            fail_on_put_n: Arc::new(AtomicU32::new(u32::MAX)),
            put_count: Arc::new(AtomicU32::new(0)),
        }
    }

    /// Make the next put operation fail.
    pub fn fail_next_put(&self) {
        self.fail_next_put.store(true, Ordering::SeqCst);
    }

    /// Make the next get operation fail.
    pub fn fail_next_get(&self) {
        self.fail_next_get.store(true, Ordering::SeqCst);
    }

    /// Fail on the Nth put and all subsequent puts.
    /// `fail_on_put_n(3)` means puts 1-2 succeed, put 3 fails.
    pub fn fail_on_put_n(&self, n: u32) {
        self.fail_on_put_n.store(n, Ordering::SeqCst);
        self.put_count.store(0, Ordering::SeqCst);
    }

    /// Reset all fault injection flags.
    pub fn reset(&self) {
        self.fail_next_put.store(false, Ordering::SeqCst);
        self.fail_next_get.store(false, Ordering::SeqCst);
        self.fail_on_put_n.store(u32::MAX, Ordering::SeqCst);
        self.put_count.store(0, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl turbine_agent::ObjectStore for FaultyObjectStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
    ) -> Result<(), turbine_agent::object_store::ObjectStoreError> {
        // put_count is 0-indexed, fail_on_put_n is 1-indexed
        // So we add 1 to count for comparison: put 1 has count=0, we check 1 >= n
        let count = self.put_count.fetch_add(1, Ordering::SeqCst);
        let put_number = count + 1; // 1-indexed put number
        if put_number >= self.fail_on_put_n.load(Ordering::SeqCst) {
            return Err(turbine_agent::object_store::ObjectStoreError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, "injected fault: put failed"),
            ));
        }
        if self.fail_next_put.swap(false, Ordering::SeqCst) {
            return Err(turbine_agent::object_store::ObjectStoreError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, "injected fault: put failed"),
            ));
        }
        self.inner.put(key, data).await
    }

    async fn get(&self, key: &str) -> Result<Bytes, turbine_agent::object_store::ObjectStoreError> {
        if self.fail_next_get.swap(false, Ordering::SeqCst) {
            return Err(turbine_agent::object_store::ObjectStoreError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, "injected fault: get failed"),
            ));
        }
        self.inner.get(key).await
    }

    async fn get_range(
        &self,
        key: &str,
        start: u64,
        len: u64,
    ) -> Result<Bytes, turbine_agent::object_store::ObjectStoreError> {
        self.inner.get_range(key, start, len).await
    }

    async fn delete(&self, key: &str) -> Result<(), turbine_agent::object_store::ObjectStoreError> {
        self.inner.delete(key).await
    }

    async fn list(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, turbine_agent::object_store::ObjectStoreError> {
        self.inner.list(prefix).await
    }

    async fn size(&self, key: &str) -> Result<u64, turbine_agent::object_store::ObjectStoreError> {
        self.inner.size(key).await
    }
}

/// Simulates broker crash/restart cycle for testing recovery behavior.
pub struct CrashableAgent {
    state: Option<Arc<AgentState<FaultyObjectStore>>>,
    pub pool: PgPool,
    store: FaultyObjectStore,
    config: AgentConfig,
    #[allow(dead_code)]
    temp_dir: TempDir,
}

impl CrashableAgent {
    /// Create a new crashable broker.
    pub async fn new(pool: PgPool) -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));
        let config = AgentConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            bucket: "test-bucket".to_string(),
            key_prefix: "jepsen-test".to_string(),
        };
        // Use the test-local AgentState from this module
        let state = AgentState::new(pool.clone(), store.clone(), config.clone());

        Self {
            state: Some(Arc::new(state)),
            pool,
            store,
            config,
            temp_dir,
        }
    }

    /// Get a reference to the broker state.
    pub fn state(&self) -> &Arc<AgentState<FaultyObjectStore>> {
        self.state.as_ref().expect("Broker has crashed")
    }

    /// Simulate a crash by dropping all in-memory state.
    pub fn crash(&mut self) {
        self.state = None;
    }

    /// Restart the broker, recreating state from persistent storage only.
    pub fn restart(&mut self) {
        self.store.reset();
        // Use the test-local AgentState from this module
        let state = AgentState::new(self.pool.clone(), self.store.clone(), self.config.clone());
        self.state = Some(Arc::new(state));
    }

    /// Check if the broker has crashed.
    pub fn is_crashed(&self) -> bool {
        self.state.is_none()
    }

    /// Get the faulty object store for fault injection.
    pub fn faulty_store(&self) -> &FaultyObjectStore {
        &self.store
    }
}

// ============ Operation History for Linearizability Checking ============

/// A write operation for linearizability tracking.
#[derive(Debug, Clone)]
pub struct WriteOp {
    pub writer_id: WriterId,
    pub offset: Option<Offset>, // None if write failed
    pub value: Bytes,
    pub start_time: Instant,
    pub end_time: Option<Instant>,
    pub success: bool,
}

/// A read operation for linearizability tracking.
#[derive(Debug, Clone)]
pub struct ReadOp {
    pub requested_offset: Offset,
    pub returned_values: Vec<Bytes>,
    pub high_watermark: Offset,
    pub start_time: Instant,
    pub end_time: Option<Instant>,
    pub success: bool,
}

/// Track operation history for linearizability checking.
#[derive(Default)]
pub struct OperationHistory {
    pub writes: Vec<WriteOp>,
    pub reads: Vec<ReadOp>,
}

impl OperationHistory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_write_start(&mut self, writer_id: WriterId, value: Bytes) -> usize {
        let idx = self.writes.len();
        self.writes.push(WriteOp {
            writer_id,
            offset: None,
            value,
            start_time: Instant::now(),
            end_time: None,
            success: false,
        });
        idx
    }

    pub fn record_write_complete(&mut self, idx: usize, offset: Option<Offset>, success: bool) {
        if let Some(op) = self.writes.get_mut(idx) {
            op.offset = offset;
            op.end_time = Some(Instant::now());
            op.success = success;
        }
    }

    pub fn record_read_start(&mut self, offset: Offset) -> usize {
        let idx = self.reads.len();
        self.reads.push(ReadOp {
            requested_offset: offset,
            returned_values: vec![],
            high_watermark: Offset(0),
            start_time: Instant::now(),
            end_time: None,
            success: false,
        });
        idx
    }

    pub fn record_read_complete(
        &mut self,
        idx: usize,
        values: Vec<Bytes>,
        high_watermark: Offset,
        success: bool,
    ) {
        if let Some(op) = self.reads.get_mut(idx) {
            op.returned_values = values;
            op.high_watermark = high_watermark;
            op.end_time = Some(Instant::now());
            op.success = success;
        }
    }

    /// Check that all acknowledged writes are eventually visible.
    ///
    /// This properly accounts for concurrent read/write timing:
    /// - Only writes that completed BEFORE the final read started are expected to be visible
    /// - Writes that completed during or after the read may or may not be visible (race window)
    pub fn verify_acknowledged_writes_visible(&self) -> Result<(), String> {
        let successful_writes: Vec<_> = self
            .writes
            .iter()
            .filter(|w| w.success && w.offset.is_some())
            .collect();

        if successful_writes.is_empty() {
            return Ok(());
        }

        // Find the final read (one with highest watermark)
        let final_read = self
            .reads
            .iter()
            .filter(|r| r.success)
            .max_by_key(|r| r.high_watermark.0);

        let Some(final_read) = final_read else {
            return Err("No successful reads to verify against".to_string());
        };

        // Check writes that completed before the final read started
        for write in &successful_writes {
            if let (Some(offset), Some(write_end_time)) = (write.offset, write.end_time) {
                // Only expect visibility if write completed BEFORE read started
                // This avoids false positives from the race window where a write
                // might complete during or after a read
                if write_end_time < final_read.start_time && offset.0 < final_read.high_watermark.0
                {
                    // This write definitely should be visible
                    if !final_read.returned_values.contains(&write.value) {
                        return Err(format!(
                            "Acknowledged write at offset {} with value {:?} not visible in final read \
                             (watermark {}, write ended {:?} before read started {:?})",
                            offset.0,
                            write.value,
                            final_read.high_watermark.0,
                            write_end_time,
                            final_read.start_time
                        ));
                    }
                }
                // Writes that completed after read started may or may not be visible (race)
                // - we don't assert on these as it depends on exact timing
            }
        }

        Ok(())
    }

    /// Check that offsets are unique (no two writes got the same offset).
    pub fn verify_unique_offsets(&self) -> Result<(), String> {
        let mut seen = std::collections::HashSet::new();
        for write in &self.writes {
            if write.success {
                if let Some(offset) = write.offset {
                    if !seen.insert(offset.0) {
                        return Err(format!(
                            "Duplicate offset {} assigned to multiple writes",
                            offset.0
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Check that watermark never regresses across reads.
    pub fn verify_watermark_monotonic(&self) -> Result<(), String> {
        let successful_reads: Vec<_> = self.reads.iter().filter(|r| r.success).collect();

        for window in successful_reads.windows(2) {
            let (prev, curr) = (&window[0], &window[1]);
            if curr.high_watermark.0 < prev.high_watermark.0 {
                return Err(format!(
                    "Watermark regressed from {} to {}",
                    prev.high_watermark.0, curr.high_watermark.0
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use turbine_agent::ObjectStore;

    #[tokio::test]
    async fn test_faulty_object_store_fail_on_put_n() {
        let temp_dir = TempDir::new().unwrap();
        let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));

        // fail_on_put_n(3) should make puts 1-2 succeed, put 3 fail
        store.fail_on_put_n(3);

        // Put 1: should succeed
        let result = store.put("key1", Bytes::from("value1")).await;
        assert!(result.is_ok(), "Put 1 should succeed");

        // Put 2: should succeed
        let result = store.put("key2", Bytes::from("value2")).await;
        assert!(result.is_ok(), "Put 2 should succeed");

        // Put 3: should fail
        let result = store.put("key3", Bytes::from("value3")).await;
        assert!(result.is_err(), "Put 3 should fail");

        // Put 4: should also fail
        let result = store.put("key4", Bytes::from("value4")).await;
        assert!(result.is_err(), "Put 4 should also fail");

        // After reset, puts should succeed again
        store.reset();
        let result = store.put("key5", Bytes::from("value5")).await;
        assert!(result.is_ok(), "Put after reset should succeed");
    }

    #[tokio::test]
    async fn test_faulty_object_store_fail_on_put_1() {
        let temp_dir = TempDir::new().unwrap();
        let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));

        // fail_on_put_n(1) should make the very first put fail
        store.fail_on_put_n(1);

        let result = store.put("key1", Bytes::from("value1")).await;
        assert!(result.is_err(), "Put 1 should fail immediately");
    }
}
