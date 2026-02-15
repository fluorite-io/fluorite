//! Reader group coordinator benchmarks.

use criterion::{BenchmarkId, Criterion, black_box};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;
use tokio::runtime::Runtime;
use turbine_agent::{Coordinator, CoordinatorConfig};
use turbine_common::ids::{Offset, PartitionId, TopicId};

static DB_COUNTER: AtomicU32 = AtomicU32::new(0);

fn db_available() -> bool {
    std::env::var("DATABASE_URL").is_ok() || std::env::var("TURBINE_BENCH_DB").is_ok()
}

/// Wrapper struct for benchmark database with automatic cleanup on drop.
pub struct BenchDb {
    pub pool: PgPool,
    db_name: String,
    base_url: String,
}

impl Drop for BenchDb {
    fn drop(&mut self) {
        // Close all connections to allow dropping (returns future but we can't await in Drop)
        let _ = self.pool.close();

        // Use a new runtime for cleanup since we can't use async in Drop
        let admin_url = format!("{}/postgres", self.base_url);
        let db_name = self.db_name.clone();

        std::thread::spawn(move || {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                if let Ok(admin_pool) = PgPoolOptions::new()
                    .max_connections(1)
                    .connect(&admin_url)
                    .await
                {
                    // Force disconnect other sessions
                    let _ = sqlx::query(&format!(
                        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}'",
                        db_name
                    ))
                    .execute(&admin_pool)
                    .await;

                    let _ = sqlx::query(&format!("DROP DATABASE IF EXISTS {}", db_name))
                        .execute(&admin_pool)
                        .await;
                }
            });
        })
        .join()
        .ok();
    }
}

async fn create_test_db() -> Option<BenchDb> {
    let base_url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("TURBINE_BENCH_DB"))
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5433".to_string());

    let admin_url = format!("{}/postgres", base_url);
    let admin_pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&admin_url)
        .await
        .ok()?;

    let counter = DB_COUNTER.fetch_add(1, Ordering::SeqCst);
    let db_name = format!("turbine_coord_bench_{}_{}", std::process::id(), counter);

    sqlx::query(&format!("DROP DATABASE IF EXISTS {}", db_name))
        .execute(&admin_pool)
        .await
        .ok()?;

    sqlx::query(&format!("CREATE DATABASE {}", db_name))
        .execute(&admin_pool)
        .await
        .ok()?;

    let test_url = format!("{}/{}", base_url, db_name);
    let pool = PgPoolOptions::new()
        .max_connections(10)
        .connect(&test_url)
        .await
        .ok()?;

    // Create required tables
    sqlx::query(
        r#"
        CREATE TABLE topics (
            topic_id SERIAL PRIMARY KEY,
            name VARCHAR(255) UNIQUE NOT NULL,
            partition_count INT NOT NULL DEFAULT 1
        )
        "#,
    )
    .execute(&pool)
    .await
    .ok()?;

    sqlx::query(
        r#"
        CREATE TABLE reader_groups (
            group_id TEXT NOT NULL,
            topic_id INT NOT NULL,
            generation BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (group_id, topic_id)
        )
        "#,
    )
    .execute(&pool)
    .await
    .ok()?;

    sqlx::query(
        r#"
        CREATE TABLE reader_assignments (
            group_id TEXT NOT NULL,
            topic_id INT NOT NULL,
            partition_id INT NOT NULL,
            reader_id TEXT,
            lease_expires_at TIMESTAMPTZ,
            committed_offset BIGINT NOT NULL DEFAULT 0,
            generation BIGINT NOT NULL DEFAULT 0,
            PRIMARY KEY (group_id, topic_id, partition_id)
        )
        "#,
    )
    .execute(&pool)
    .await
    .ok()?;

    sqlx::query(
        "CREATE INDEX idx_assignments ON reader_assignments (group_id, topic_id, reader_id)",
    )
    .execute(&pool)
    .await
    .ok()?;

    sqlx::query(
        r#"
        CREATE TABLE reader_members (
            group_id TEXT NOT NULL,
            topic_id INT NOT NULL,
            reader_id TEXT NOT NULL,
            last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (group_id, topic_id, reader_id)
        )
        "#,
    )
    .execute(&pool)
    .await
    .ok()?;

    // Create test topic
    sqlx::query("INSERT INTO topics (name, partition_count) VALUES ('bench-topic', 16)")
        .execute(&pool)
        .await
        .ok()?;

    Some(BenchDb {
        pool,
        db_name,
        base_url,
    })
}

/// Benchmark reader join_group operation.
pub fn bench_coordinator_join(c: &mut Criterion) {
    if !db_available() {
        eprintln!("Skipping coordinator benchmarks: DATABASE_URL not set");
        return;
    }

    let rt = Runtime::new().unwrap();

    let bench_db = match rt.block_on(create_test_db()) {
        Some(db) => db,
        None => {
            eprintln!("Failed to create test database");
            return;
        }
    };
    let pool = bench_db.pool.clone();

    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(30),
        session_timeout: Duration::from_secs(60),
    };
    let _coordinator = Coordinator::new(pool.clone(), config);

    let mut group = c.benchmark_group("coordinator_join");
    let counter = AtomicU32::new(0);

    // First reader join (creates group)
    group.bench_function("first_consumer", |b| {
        b.to_async(&rt).iter(|| {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            let group_id = format!("bench-group-{}", n);
            let reader_id = format!("reader-{}", n);
            let pool = pool.clone();
            let config = CoordinatorConfig {
                lease_duration: Duration::from_secs(30),
                session_timeout: Duration::from_secs(60),
            };
            async move {
                let coord = Coordinator::new(pool, config);
                let result = coord.join_group(&group_id, TopicId(1), &reader_id).await;
                black_box(result)
            }
        })
    });

    group.finish();
}

/// Benchmark heartbeat operation.
pub fn bench_coordinator_heartbeat(c: &mut Criterion) {
    if !db_available() {
        return;
    }

    let rt = Runtime::new().unwrap();

    let bench_db = match rt.block_on(create_test_db()) {
        Some(db) => db,
        None => return,
    };
    let pool = bench_db.pool.clone();

    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(30),
        session_timeout: Duration::from_secs(60),
    };
    let coordinator = Coordinator::new(pool.clone(), config);

    // Pre-join a reader
    let generation = rt.block_on(async {
        coordinator
            .join_group("heartbeat-bench", TopicId(1), "reader-1")
            .await
            .unwrap()
            .generation
    });

    let mut group = c.benchmark_group("coordinator_heartbeat");

    group.bench_function("single_consumer", |b| {
        b.to_async(&rt).iter(|| {
            let pool = pool.clone();
            let consumer_gen = generation;
            async move {
                let config = CoordinatorConfig {
                    lease_duration: Duration::from_secs(30),
                    session_timeout: Duration::from_secs(60),
                };
                let coord = Coordinator::new(pool, config);
                let result = coord
                    .heartbeat("heartbeat-bench", TopicId(1), "reader-1", consumer_gen)
                    .await;
                black_box(result)
            }
        })
    });

    group.finish();
}

/// Benchmark commit offset operation.
pub fn bench_coordinator_commit(c: &mut Criterion) {
    if !db_available() {
        return;
    }

    let rt = Runtime::new().unwrap();

    let bench_db = match rt.block_on(create_test_db()) {
        Some(db) => db,
        None => return,
    };
    let pool = bench_db.pool.clone();

    let config = CoordinatorConfig {
        lease_duration: Duration::from_secs(30),
        session_timeout: Duration::from_secs(60),
    };
    let coordinator = Coordinator::new(pool.clone(), config);

    // Pre-join a reader
    rt.block_on(async {
        coordinator
            .join_group("commit-bench", TopicId(1), "reader-1")
            .await
            .unwrap()
    });

    let mut group = c.benchmark_group("coordinator_commit");
    let offset_counter = AtomicU64::new(0);

    group.bench_function("single_partition", |b| {
        b.to_async(&rt).iter(|| {
            let off = offset_counter.fetch_add(100, Ordering::SeqCst);
            let pool = pool.clone();
            async move {
                let config = CoordinatorConfig {
                    lease_duration: Duration::from_secs(30),
                    session_timeout: Duration::from_secs(60),
                };
                let coord = Coordinator::new(pool, config);
                let result = coord
                    .commit_offset(
                        "commit-bench",
                        TopicId(1),
                        "reader-1",
                        PartitionId(0),
                        Offset(off),
                    )
                    .await;
                black_box(result)
            }
        })
    });

    group.finish();
}

/// Benchmark rebalance with multiple consumers.
pub fn bench_coordinator_rebalance(c: &mut Criterion) {
    if !db_available() {
        return;
    }

    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("coordinator_rebalance");

    for consumer_count in &[2, 4, 8] {
        group.bench_with_input(
            BenchmarkId::new("consumers", consumer_count),
            consumer_count,
            |b, &count| {
                b.to_async(&rt).iter_batched(
                    || {
                        let rt_inner = Runtime::new().unwrap();
                        let bench_db = rt_inner.block_on(create_test_db()).unwrap();
                        let pool = bench_db.pool.clone();

                        let config = CoordinatorConfig {
                            lease_duration: Duration::from_secs(30),
                            session_timeout: Duration::from_secs(60),
                        };

                        // Pre-join consumers except the last
                        for i in 0..(count - 1) {
                            let reader_id = format!("reader-{}", i);
                            let pool_clone = pool.clone();
                            rt_inner.block_on(async {
                                let coord = Coordinator::new(pool_clone, config.clone());
                                coord
                                    .join_group("rebalance-bench", TopicId(1), &reader_id)
                                    .await
                                    .unwrap();
                            });
                        }

                        (bench_db, count)
                    },
                    |(bench_db, count)| async move {
                        let config = CoordinatorConfig {
                            lease_duration: Duration::from_secs(30),
                            session_timeout: Duration::from_secs(60),
                        };
                        let coord = Coordinator::new(bench_db.pool.clone(), config);
                        // Join the last reader (triggers rebalance)
                        let reader_id = format!("reader-{}", count - 1);
                        let result = coord
                            .join_group("rebalance-bench", TopicId(1), &reader_id)
                            .await;
                        // BenchDb is dropped here, cleaning up the database
                        drop(bench_db);
                        black_box(result)
                    },
                    criterion::BatchSize::PerIteration,
                )
            },
        );
    }

    group.finish();
}
