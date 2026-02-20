//! Broker error types.

use thiserror::Error;

/// Broker error type.
#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Object store error: {0}")]
    ObjectStore(String),

    #[error("TBIN error: {0}")]
    Tbin(String),

    #[error("Wire protocol error: {0}")]
    Wire(String),
}

impl From<crate::object_store::ObjectStoreError> for BrokerError {
    fn from(e: crate::object_store::ObjectStoreError) -> Self {
        BrokerError::ObjectStore(e.to_string())
    }
}

impl From<crate::tbin::TbinError> for BrokerError {
    fn from(e: crate::tbin::TbinError) -> Self {
        BrokerError::Tbin(e.to_string())
    }
}
