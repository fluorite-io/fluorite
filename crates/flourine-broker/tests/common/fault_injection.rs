//! Fault injection infrastructure for testing error handling and recovery.

use bytes::Bytes;
use sqlx::PgPool;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use tempfile::TempDir;

use flourine_broker::LocalFsStore;

use super::{TestBrokerConfig, TestBrokerState};

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
    pub fn new(store: LocalFsStore) -> Self {
        Self {
            inner: Arc::new(store),
            fail_next_put: Arc::new(AtomicBool::new(false)),
            fail_next_get: Arc::new(AtomicBool::new(false)),
            fail_on_put_n: Arc::new(AtomicU32::new(u32::MAX)),
            put_count: Arc::new(AtomicU32::new(0)),
        }
    }

    pub fn fail_next_put(&self) {
        self.fail_next_put.store(true, Ordering::SeqCst);
    }

    pub fn fail_next_get(&self) {
        self.fail_next_get.store(true, Ordering::SeqCst);
    }

    /// Fail on the Nth put and all subsequent puts.
    pub fn fail_on_put_n(&self, n: u32) {
        self.fail_on_put_n.store(n, Ordering::SeqCst);
        self.put_count.store(0, Ordering::SeqCst);
    }

    pub fn reset(&self) {
        self.fail_next_put.store(false, Ordering::SeqCst);
        self.fail_next_get.store(false, Ordering::SeqCst);
        self.fail_on_put_n.store(u32::MAX, Ordering::SeqCst);
        self.put_count.store(0, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl flourine_broker::ObjectStore for FaultyObjectStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
    ) -> Result<(), flourine_broker::object_store::ObjectStoreError> {
        let count = self.put_count.fetch_add(1, Ordering::SeqCst);
        let put_number = count + 1;
        if put_number >= self.fail_on_put_n.load(Ordering::SeqCst) {
            return Err(flourine_broker::object_store::ObjectStoreError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, "injected fault: put failed"),
            ));
        }
        if self.fail_next_put.swap(false, Ordering::SeqCst) {
            return Err(flourine_broker::object_store::ObjectStoreError::Io(
                std::io::Error::new(std::io::ErrorKind::Other, "injected fault: put failed"),
            ));
        }
        self.inner.put(key, data).await
    }

    async fn get(&self, key: &str) -> Result<Bytes, flourine_broker::object_store::ObjectStoreError> {
        if self.fail_next_get.swap(false, Ordering::SeqCst) {
            return Err(flourine_broker::object_store::ObjectStoreError::Io(
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
    ) -> Result<Bytes, flourine_broker::object_store::ObjectStoreError> {
        self.inner.get_range(key, start, len).await
    }

    async fn delete(&self, key: &str) -> Result<(), flourine_broker::object_store::ObjectStoreError> {
        self.inner.delete(key).await
    }

    async fn list(
        &self,
        prefix: &str,
    ) -> Result<Vec<String>, flourine_broker::object_store::ObjectStoreError> {
        self.inner.list(prefix).await
    }

    async fn size(&self, key: &str) -> Result<u64, flourine_broker::object_store::ObjectStoreError> {
        self.inner.size(key).await
    }
}

/// Simulates broker crash/restart cycle for testing recovery behavior.
pub struct CrashableBroker {
    state: Option<Arc<TestBrokerState<FaultyObjectStore>>>,
    pub pool: PgPool,
    store: FaultyObjectStore,
    config: TestBrokerConfig,
    #[allow(dead_code)]
    temp_dir: TempDir,
}

impl CrashableBroker {
    pub async fn new(pool: PgPool) -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));
        let config = TestBrokerConfig {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            bucket: "test-bucket".to_string(),
            key_prefix: "jepsen-test".to_string(),
        };
        let state = TestBrokerState::new(pool.clone(), store.clone(), config.clone());

        Self {
            state: Some(Arc::new(state)),
            pool,
            store,
            config,
            temp_dir,
        }
    }

    pub fn state(&self) -> &Arc<TestBrokerState<FaultyObjectStore>> {
        self.state.as_ref().expect("Broker has crashed")
    }

    pub fn crash(&mut self) {
        self.state = None;
    }

    pub fn restart(&mut self) {
        self.store.reset();
        let state = TestBrokerState::new(self.pool.clone(), self.store.clone(), self.config.clone());
        self.state = Some(Arc::new(state));
    }

    pub fn is_crashed(&self) -> bool {
        self.state.is_none()
    }

    pub fn faulty_store(&self) -> &FaultyObjectStore {
        &self.store
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flourine_broker::ObjectStore;

    #[tokio::test]
    async fn test_faulty_object_store_fail_on_put_n() {
        let temp_dir = TempDir::new().unwrap();
        let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));

        store.fail_on_put_n(3);

        let result = store.put("key1", Bytes::from("value1")).await;
        assert!(result.is_ok(), "Put 1 should succeed");

        let result = store.put("key2", Bytes::from("value2")).await;
        assert!(result.is_ok(), "Put 2 should succeed");

        let result = store.put("key3", Bytes::from("value3")).await;
        assert!(result.is_err(), "Put 3 should fail");

        let result = store.put("key4", Bytes::from("value4")).await;
        assert!(result.is_err(), "Put 4 should also fail");

        store.reset();
        let result = store.put("key5", Bytes::from("value5")).await;
        assert!(result.is_ok(), "Put after reset should succeed");
    }

    #[tokio::test]
    async fn test_faulty_object_store_fail_on_put_1() {
        let temp_dir = TempDir::new().unwrap();
        let store = FaultyObjectStore::new(LocalFsStore::new(temp_dir.path().to_path_buf()));

        store.fail_on_put_n(1);

        let result = store.put("key1", Bytes::from("value1")).await;
        assert!(result.is_err(), "Put 1 should fail immediately");
    }
}
