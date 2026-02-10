//! SDK error types.

use thiserror::Error;

/// SDK error type.
#[derive(Debug, Error)]
pub enum SdkError {
    #[error("connection error: {0}")]
    Connection(String),

    #[error("WebSocket error: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),

    #[error("encoding error: {0}")]
    Encode(String),

    #[error("decoding error: {0}")]
    Decode(String),

    #[error("timeout")]
    Timeout,

    #[error("backpressure: server is overloaded")]
    Backpressure,

    #[error("disconnected")]
    Disconnected,

    #[error("invalid response")]
    InvalidResponse,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid state: {0}")]
    InvalidState(String),

    #[error("commit failed")]
    CommitFailed,
}
