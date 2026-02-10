//! Common test utilities for integration tests.

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::sync::atomic::{AtomicU32, Ordering};

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
    /// Example: postgres://postgres:turbine@localhost:5433
    pub async fn new() -> Self {
        let base_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:turbine@localhost:5433".to_string());

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
        // Create tables manually (simplified version of migrations)
        sqlx::query(
            r#"
            CREATE TABLE topics (
                topic_id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL,
                partition_count INT NOT NULL DEFAULT 1,
                retention_hours INT NOT NULL DEFAULT 168,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(pool)
        .await
        .expect("Failed to create topics table");

        sqlx::query(
            r#"
            CREATE TABLE partition_offsets (
                topic_id INT NOT NULL REFERENCES topics(topic_id) ON DELETE CASCADE,
                partition_id INT NOT NULL,
                next_offset BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (topic_id, partition_id)
            )
            "#,
        )
        .execute(pool)
        .await
        .expect("Failed to create partition_offsets table");

        sqlx::query(
            r#"
            CREATE TABLE topic_batches (
                batch_id BIGSERIAL PRIMARY KEY,
                topic_id INT NOT NULL,
                partition_id INT NOT NULL,
                schema_id INT NOT NULL,
                start_offset BIGINT NOT NULL,
                end_offset BIGINT NOT NULL,
                record_count INT NOT NULL,
                s3_key VARCHAR(512) NOT NULL,
                ingest_time TIMESTAMPTZ NOT NULL
            )
            "#,
        )
        .execute(pool)
        .await
        .expect("Failed to create topic_batches table");

        sqlx::query(
            r#"
            CREATE INDEX idx_topic_batches_fetch
            ON topic_batches (topic_id, partition_id, end_offset)
            "#,
        )
        .execute(pool)
        .await
        .expect("Failed to create topic_batches index");

        sqlx::query(
            r#"
            CREATE TABLE producer_state (
                producer_id UUID PRIMARY KEY,
                last_seq_num BIGINT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(pool)
        .await
        .expect("Failed to create producer_state table");

        sqlx::query(
            r#"
            CREATE TABLE schemas (
                schema_id SERIAL PRIMARY KEY,
                schema_hash BYTEA NOT NULL UNIQUE,
                schema_json JSONB NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            "#,
        )
        .execute(pool)
        .await
        .expect("Failed to create schemas table");

        sqlx::query(
            r#"
            CREATE TABLE topic_schemas (
                topic_id INT NOT NULL REFERENCES topics(topic_id) ON DELETE CASCADE,
                schema_id INT NOT NULL REFERENCES schemas(schema_id) ON DELETE CASCADE,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (topic_id, schema_id)
            )
            "#,
        )
        .execute(pool)
        .await
        .expect("Failed to create topic_schemas table");
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

        // Create partition offsets
        for i in 0..partition_count {
            sqlx::query(
                "INSERT INTO partition_offsets (topic_id, partition_id, next_offset) VALUES ($1, $2, 0)",
            )
            .bind(topic_id)
            .bind(i)
            .execute(&self.pool)
            .await
            .expect("Failed to create partition offset");
        }

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
