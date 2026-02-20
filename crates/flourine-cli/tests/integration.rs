//! Integration tests for the Flourine CLI.
//!
//! Requires a running PostgreSQL instance (see DATABASE_URL env var).

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use tempfile::TempDir;
use tokio::net::TcpListener;

use flourine_broker::{
    AclChecker, AdminConfig, AdminState, ApiKeyValidator, BrokerConfig, BrokerState, Coordinator,
    CoordinatorConfig, LocalFsStore, Operation, ResourceType,
};
use flourine_broker::buffer::BufferConfig;
use flourine_common::ids::{PartitionId, SchemaId, TopicId};
use flourine_common::types::Record;
use flourine_sdk::reader::{Reader, ReaderConfig};
use flourine_sdk::writer::{Writer, WriterConfig};

use flourine_cli::client::FlourineClient;
use flourine_cli::tail::{TailRecord, parse_offsets};

// ============ Test infrastructure ============

static TEST_DB_COUNTER: AtomicU32 = AtomicU32::new(0);

struct TestCluster {
    ws_addr: SocketAddr,
    admin_addr: SocketAddr,
    api_key: String,
    #[allow(dead_code)]
    pool: PgPool,
    _admin_pool: PgPool,
    _db_name: String,
    _temp_dir: TempDir,
}

impl TestCluster {
    async fn start() -> Self {
        let base_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5433".to_string());

        // Create isolated test database
        let admin_url = format!("{}/postgres", base_url);
        let admin_pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&admin_url)
            .await
            .expect("Failed to connect to postgres");

        let counter = TEST_DB_COUNTER.fetch_add(1, Ordering::SeqCst);
        let db_name = format!("flourine_cli_test_{}_{}", std::process::id(), counter);

        sqlx::query(&format!("DROP DATABASE IF EXISTS {db_name}"))
            .execute(&admin_pool)
            .await
            .unwrap();
        sqlx::query(&format!("CREATE DATABASE {db_name}"))
            .execute(&admin_pool)
            .await
            .unwrap();

        let test_url = format!("{}/{}", base_url, db_name);
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&test_url)
            .await
            .unwrap();

        // Run migrations
        let migration_path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../migrations/001_init.sql");
        let migration_sql = std::fs::read_to_string(&migration_path).unwrap_or_else(|e| {
            panic!(
                "Failed to read migration at {}: {}",
                migration_path.display(),
                e
            )
        });
        sqlx::raw_sql(&migration_sql).execute(&pool).await.unwrap();

        // Create admin API key + ACL
        let validator = ApiKeyValidator::new(pool.clone());
        let checker = AclChecker::new(pool.clone());
        let (api_key, _) = validator
            .create_key("cli-test-admin", "admin:cli-test", None)
            .await
            .unwrap();
        checker
            .create_acl(
                "admin:cli-test",
                ResourceType::Cluster,
                "*",
                Operation::Admin,
                true,
            )
            .await
            .unwrap();

        let temp_dir = TempDir::new().unwrap();

        // Start WebSocket server
        let ws_addr = find_available_port().await;
        let store = LocalFsStore::new(temp_dir.path().to_path_buf());
        let broker_config = BrokerConfig {
            bind_addr: ws_addr,
            bucket: "test".to_string(),
            key_prefix: "data".to_string(),
            buffer: BufferConfig::default(),
            flush_interval: Duration::from_millis(50),
            require_auth: false,
            auth_timeout: Duration::from_secs(10),
        };
        let broker_state = BrokerState::new(pool.clone(), store, broker_config).await;
        tokio::spawn(async move {
            let _ = flourine_broker::run(broker_state).await;
        });

        // Start Admin HTTP server
        let admin_addr = find_available_port().await;
        let admin_config = AdminConfig {
            bind_addr: admin_addr,
        };
        let admin_state = AdminState::new(
            pool.clone(),
            Coordinator::new(pool.clone(), CoordinatorConfig::default()),
        );
        tokio::spawn(async move {
            let _ = flourine_broker::run_admin_with_shutdown(
                admin_config,
                admin_state,
                std::future::pending::<()>(),
            )
            .await;
        });

        // Wait for servers to start
        tokio::time::sleep(Duration::from_millis(300)).await;

        Self {
            ws_addr,
            admin_addr,
            api_key,
            pool,
            _admin_pool: admin_pool,
            _db_name: db_name,
            _temp_dir: temp_dir,
        }
    }

    fn admin_url(&self) -> String {
        format!("http://{}", self.admin_addr)
    }

    fn ws_url(&self) -> String {
        format!("ws://{}", self.ws_addr)
    }

    fn client(&self) -> FlourineClient {
        FlourineClient::new(&self.admin_url(), Some(&self.api_key))
    }

    async fn writer(&self) -> std::sync::Arc<Writer> {
        let config = WriterConfig {
            url: self.ws_url(),
            ..Default::default()
        };
        Writer::connect_with_config(config).await.unwrap()
    }
}

async fn find_available_port() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap()
}

// ============ Topic tests ============

#[tokio::test]
async fn test_topic_create_and_list() {
    let cluster = TestCluster::start().await;
    let client = cluster.client();

    let topics = client.list_topics().await.unwrap();
    assert!(topics.is_empty());

    let resp = client.create_topic("test-topic", 3, None).await.unwrap();
    assert!(resp.topic_id > 0);

    let topics = client.list_topics().await.unwrap();
    assert_eq!(topics.len(), 1);
    assert_eq!(topics[0].name, "test-topic");
    assert_eq!(topics[0].partition_count, 3);
}

#[tokio::test]
async fn test_topic_get_and_update() {
    let cluster = TestCluster::start().await;
    let client = cluster.client();

    let resp = client.create_topic("get-test", 1, Some(24)).await.unwrap();
    let id = resp.topic_id;

    let topic = client.get_topic(id).await.unwrap();
    assert_eq!(topic.name, "get-test");
    assert_eq!(topic.retention_hours, 24);

    client.update_topic(id, 48).await.unwrap();
    let topic = client.get_topic(id).await.unwrap();
    assert_eq!(topic.retention_hours, 48);
}

#[tokio::test]
async fn test_topic_delete() {
    let cluster = TestCluster::start().await;
    let client = cluster.client();

    let resp = client.create_topic("delete-me", 1, None).await.unwrap();
    client.delete_topic(resp.topic_id).await.unwrap();

    let topics = client.list_topics().await.unwrap();
    assert!(topics.is_empty());
}

// ============ Write + Read tests ============

#[tokio::test]
async fn test_write_and_read_records() {
    let cluster = TestCluster::start().await;
    let client = cluster.client();

    let topic = client.create_topic("write-test", 1, None).await.unwrap();
    let topic_id = topic.topic_id as u32;

    let writer = cluster.writer().await;

    for i in 0..3 {
        writer
            .append(
                TopicId(topic_id),
                PartitionId(0),
                SchemaId(1),
                vec![Record::with_key(format!("key-{i}"), format!("value-{i}"))],
            )
            .await
            .unwrap();
    }

    // Wait for flush
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Read via SDK reader
    let config = ReaderConfig {
        url: cluster.ws_url(),
        group_id: format!("read-test-{}", uuid::Uuid::new_v4()),
        topic_id: TopicId(topic_id),
        ..Default::default()
    };
    let reader = Reader::join(config).await.unwrap();
    reader.start_heartbeat();

    let results = reader.poll().await.unwrap();
    let total: usize = results.iter().map(|r| r.records.len()).sum();
    assert_eq!(total, 3);

    let _ = reader.stop().await;
}

// ============ Tail tests ============

#[tokio::test]
async fn test_tail_json_with_end_offset() {
    let cluster = TestCluster::start().await;
    let client = cluster.client();

    let topic = client.create_topic("tail-test", 1, None).await.unwrap();
    let topic_id = topic.topic_id as u32;

    let writer = cluster.writer().await;

    for i in 0..5 {
        writer
            .append(
                TopicId(topic_id),
                PartitionId(0),
                SchemaId(1),
                vec![Record::with_key(format!("k{i}"), format!("v{i}"))],
            )
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Tail with start=0:0, end=0:3 should yield offsets 0,1,2
    let (tx, mut rx) = tokio::sync::mpsc::channel::<TailRecord>(100);

    let start_offsets = parse_offsets("0:0").unwrap();
    let end_offsets = parse_offsets("0:3").unwrap();

    let config = ReaderConfig {
        url: cluster.ws_url(),
        group_id: format!("tail-{}", uuid::Uuid::new_v4()),
        topic_id: TopicId(topic_id),
        ..Default::default()
    };

    let reader = Reader::join(config).await.unwrap();
    reader.start_heartbeat();

    for (&partition, &offset) in &start_offsets {
        reader
            .seek(PartitionId(partition), flourine_common::ids::Offset(offset))
            .await;
    }

    let reader_clone = reader.clone();
    let handle = tokio::spawn(async move {
        flourine_cli::tail::poll_loop(reader_clone, tx, Some(end_offsets)).await;
    });

    let mut records = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(record)) => records.push(record),
            Ok(None) => break,
            Err(_) => panic!("Timed out waiting for tail records"),
        }
    }

    handle.await.unwrap();
    let _ = reader.stop().await;

    assert_eq!(records.len(), 3, "expected 3 records (offsets 0,1,2)");
    assert_eq!(records[0].offset, 0);
    assert_eq!(records[1].offset, 1);
    assert_eq!(records[2].offset, 2);
}

#[tokio::test]
async fn test_tail_start_offset_skips_records() {
    let cluster = TestCluster::start().await;
    let client = cluster.client();

    let topic = client
        .create_topic("tail-skip-test", 1, None)
        .await
        .unwrap();
    let topic_id = topic.topic_id as u32;

    let writer = cluster.writer().await;

    for i in 0..5 {
        writer
            .append(
                TopicId(topic_id),
                PartitionId(0),
                SchemaId(1),
                vec![Record::with_key(format!("k{i}"), format!("v{i}"))],
            )
            .await
            .unwrap();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Tail with start=0:3, end=0:5 should yield offsets 3,4
    let (tx, mut rx) = tokio::sync::mpsc::channel::<TailRecord>(100);

    let start_offsets = parse_offsets("0:3").unwrap();
    let end_offsets = parse_offsets("0:5").unwrap();

    let config = ReaderConfig {
        url: cluster.ws_url(),
        group_id: format!("tail-skip-{}", uuid::Uuid::new_v4()),
        topic_id: TopicId(topic_id),
        ..Default::default()
    };

    let reader = Reader::join(config).await.unwrap();
    reader.start_heartbeat();

    for (&partition, &offset) in &start_offsets {
        reader
            .seek(PartitionId(partition), flourine_common::ids::Offset(offset))
            .await;
    }

    let reader_clone = reader.clone();
    let handle = tokio::spawn(async move {
        flourine_cli::tail::poll_loop(reader_clone, tx, Some(end_offsets)).await;
    });

    let mut records = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match tokio::time::timeout_at(deadline, rx.recv()).await {
            Ok(Some(record)) => records.push(record),
            Ok(None) => break,
            Err(_) => panic!("Timed out waiting for tail records"),
        }
    }

    handle.await.unwrap();
    let _ = reader.stop().await;

    assert_eq!(records.len(), 2, "expected 2 records (offsets 3,4)");
    assert_eq!(records[0].offset, 3);
    assert_eq!(records[1].offset, 4);
}
