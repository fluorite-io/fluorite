//! Common test utilities for integration tests.

#![allow(dead_code)]

mod db;
mod fault_injection;
mod operation_history;
pub mod ws_helpers;

use std::net::SocketAddr;
use std::sync::Arc;

use sqlx::PgPool;

use flourine_broker::{Coordinator, CoordinatorConfig, ObjectStore};

// Re-export sub-module types so test files don't need to change imports.
pub use db::{TestDb, cleanup_test_db, produce_records};
pub use fault_injection::{
    CrashableBroker, CrashableWsBroker, DbBlocker, FaultyObjectStore, MultiBrokerCluster,
};
pub use operation_history::{OperationHistory, ReadOp, SharedHistory, WriteOp};

// ============ Test-Only Broker Types ============

/// Simple broker configuration for tests.
#[derive(Debug, Clone)]
pub struct TestBrokerConfig {
    pub bind_addr: SocketAddr,
    pub bucket: String,
    pub key_prefix: String,
}

/// Simple broker state for tests (doesn't spawn background tasks).
pub struct TestBrokerState<S: ObjectStore> {
    pub pool: PgPool,
    pub store: Arc<S>,
    pub config: TestBrokerConfig,
    pub coordinator: Coordinator,
}

impl<S: ObjectStore> TestBrokerState<S> {
    pub fn new(pool: PgPool, store: S, config: TestBrokerConfig) -> Self {
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
        config: TestBrokerConfig,
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
