//! Error types for Turbine eventbus

use thiserror::Error;
use crate::{TopicId, PartitionId, ProducerId, Generation};

/// Error codes matching wire protocol error messages
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ErrorCode {
    /// Unknown error
    Unknown = 0,
    /// Topic not found
    TopicNotFound = 1,
    /// Partition not found
    PartitionNotFound = 2,
    /// Schema not found
    SchemaNotFound = 3,
    /// Schema incompatible with existing schemas
    IncompatibleSchema = 4,
    /// Invalid offset requested
    InvalidOffset = 5,
    /// Consumer group not found
    GroupNotFound = 6,
    /// Consumer not a member of the group
    NotMember = 7,
    /// Consumer generation is stale (rebalance in progress)
    StaleGeneration = 8,
    /// Consumer does not own the requested partition
    NotOwner = 9,
    /// Request rate limit exceeded
    RateLimited = 10,
    /// Internal server error
    InternalError = 11,
    /// Authentication failed
    Unauthenticated = 12,
    /// Authorization failed
    Unauthorized = 13,
    /// Duplicate producer sequence number
    DuplicateSequence = 14,
    /// Invalid sequence number (gap detected)
    InvalidSequence = 15,
    /// Rebalance is in progress
    RebalanceNeeded = 16,
}

impl ErrorCode {
    /// Whether this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ErrorCode::RateLimited
                | ErrorCode::InternalError
                | ErrorCode::StaleGeneration
                | ErrorCode::RebalanceNeeded
        )
    }
}

/// Main error type for Turbine operations
#[derive(Debug, Error)]
pub enum TurbineError {
    #[error("topic not found: {topic_id}")]
    TopicNotFound { topic_id: TopicId },

    #[error("partition not found: topic={topic_id}, partition={partition_id}")]
    PartitionNotFound {
        topic_id: TopicId,
        partition_id: PartitionId,
    },

    #[error("schema not found: {schema_id}")]
    SchemaNotFound { schema_id: u32 },

    #[error("incompatible schema: {message}")]
    IncompatibleSchema { message: String },

    #[error("invalid offset: {offset}")]
    InvalidOffset { offset: u64 },

    #[error("group not found: {group_id}")]
    GroupNotFound { group_id: String },

    #[error("not a member of group: {group_id}")]
    NotMember { group_id: String },

    #[error("stale generation: expected {expected}, got {actual}")]
    StaleGeneration {
        expected: Generation,
        actual: Generation,
    },

    #[error("not owner of partition: {partition_id}")]
    NotOwner { partition_id: PartitionId },

    #[error("rate limited")]
    RateLimited,

    #[error("internal error: {message}")]
    InternalError { message: String },

    #[error("unauthenticated")]
    Unauthenticated,

    #[error("unauthorized: {action}")]
    Unauthorized { action: String },

    #[error("duplicate sequence: producer={producer_id}, seq={seq_num}")]
    DuplicateSequence { producer_id: ProducerId, seq_num: u64 },

    #[error("invalid sequence: expected {expected}, got {actual}")]
    InvalidSequence { expected: u64, actual: u64 },

    #[error("rebalance needed")]
    RebalanceNeeded,

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("database error: {message}")]
    Database { message: String },

    #[error("encoding error: {message}")]
    Encoding { message: String },
}

impl TurbineError {
    /// Get the error code for this error
    pub fn code(&self) -> ErrorCode {
        match self {
            TurbineError::TopicNotFound { .. } => ErrorCode::TopicNotFound,
            TurbineError::PartitionNotFound { .. } => ErrorCode::PartitionNotFound,
            TurbineError::SchemaNotFound { .. } => ErrorCode::SchemaNotFound,
            TurbineError::IncompatibleSchema { .. } => ErrorCode::IncompatibleSchema,
            TurbineError::InvalidOffset { .. } => ErrorCode::InvalidOffset,
            TurbineError::GroupNotFound { .. } => ErrorCode::GroupNotFound,
            TurbineError::NotMember { .. } => ErrorCode::NotMember,
            TurbineError::StaleGeneration { .. } => ErrorCode::StaleGeneration,
            TurbineError::NotOwner { .. } => ErrorCode::NotOwner,
            TurbineError::RateLimited => ErrorCode::RateLimited,
            TurbineError::InternalError { .. } => ErrorCode::InternalError,
            TurbineError::Unauthenticated => ErrorCode::Unauthenticated,
            TurbineError::Unauthorized { .. } => ErrorCode::Unauthorized,
            TurbineError::DuplicateSequence { .. } => ErrorCode::DuplicateSequence,
            TurbineError::InvalidSequence { .. } => ErrorCode::InvalidSequence,
            TurbineError::RebalanceNeeded => ErrorCode::RebalanceNeeded,
            TurbineError::Io(_) => ErrorCode::InternalError,
            TurbineError::Database { .. } => ErrorCode::InternalError,
            TurbineError::Encoding { .. } => ErrorCode::InternalError,
        }
    }

    /// Whether this error is retryable
    pub fn is_retryable(&self) -> bool {
        self.code().is_retryable()
    }
}

/// Result type for Turbine operations
pub type Result<T> = std::result::Result<T, TurbineError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = TurbineError::TopicNotFound { topic_id: TopicId(42) };
        assert!(err.to_string().contains("42"));
    }

    #[test]
    fn test_error_is_retryable() {
        assert!(!TurbineError::TopicNotFound { topic_id: TopicId(1) }.is_retryable());
        assert!(TurbineError::InternalError { message: "oops".into() }.is_retryable());
        assert!(TurbineError::RateLimited.is_retryable());
        assert!(TurbineError::RebalanceNeeded.is_retryable());
    }

    #[test]
    fn test_error_code() {
        let err = TurbineError::TopicNotFound { topic_id: TopicId(1) };
        assert_eq!(err.code(), ErrorCode::TopicNotFound);

        let err = TurbineError::RateLimited;
        assert_eq!(err.code(), ErrorCode::RateLimited);
    }

    #[test]
    fn test_error_code_is_retryable() {
        assert!(ErrorCode::RateLimited.is_retryable());
        assert!(ErrorCode::InternalError.is_retryable());
        assert!(!ErrorCode::TopicNotFound.is_retryable());
        assert!(!ErrorCode::Unauthenticated.is_retryable());
    }
}
