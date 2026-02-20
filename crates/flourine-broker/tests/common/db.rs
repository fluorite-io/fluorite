//! Test database setup and record production helpers.

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use flourine_broker::{ObjectStore, TbinWriter};
use flourine_common::ids::{Offset, PartitionId, SchemaId, TopicId};
use flourine_common::types::{Record, RecordBatch};

use super::TestBrokerState;

static TEST_DB_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Test database configuration.
pub struct TestDb {
    pub pool: PgPool,
    #[allow(dead_code)]
    pub db_name: String,
    #[allow(dead_code)]
    admin_pool: PgPool,
    url: String,
}

impl TestDb {
    /// Create a new test database with a unique name.
    pub async fn new() -> Self {
        let base_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5433".to_string());

        let admin_url = format!("{}/postgres", base_url);
        let admin_pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&admin_url)
            .await
            .expect("Failed to connect to postgres database");

        let counter = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let db_name = format!("flourine_test_{}_{}", std::process::id(), counter);

        sqlx::query(&format!("DROP DATABASE IF EXISTS {}", db_name))
            .execute(&admin_pool)
            .await
            .expect("Failed to drop existing test database");

        sqlx::query(&format!("CREATE DATABASE {}", db_name))
            .execute(&admin_pool)
            .await
            .expect("Failed to create test database");

        let test_url = format!("{}/{}", base_url, db_name);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&test_url)
            .await
            .expect("Failed to connect to test database");

        Self::run_migrations(&pool).await;

        Self {
            pool,
            db_name,
            admin_pool,
            url: test_url,
        }
    }

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

    /// Connection URL for this test database.
    pub fn url(&self) -> &str {
        &self.url
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
    fn drop(&mut self) {}
}

static APPEND_KEY_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Append records and return (start_offset, end_offset).
pub async fn produce_records<S: ObjectStore + Send + Sync>(
    state: &TestBrokerState<S>,
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
    let _ = sqlx::query(&format!(
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}'",
        db_name
    ))
    .execute(admin_pool)
    .await;

    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {}", db_name))
        .execute(admin_pool)
        .await;
}
