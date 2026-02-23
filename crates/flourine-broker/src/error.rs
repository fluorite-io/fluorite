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

    #[error("FL error: {0}")]
    Fl(String),

    #[error("Wire protocol error: {0}")]
    Wire(String),
}

impl From<crate::object_store::ObjectStoreError> for BrokerError {
    fn from(e: crate::object_store::ObjectStoreError) -> Self {
        BrokerError::ObjectStore(e.to_string())
    }
}

impl From<crate::fl::FlError> for BrokerError {
    fn from(e: crate::fl::FlError) -> Self {
        BrokerError::Fl(e.to_string())
    }
}
