// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 Nikhil Simha Raprolu

//! Error types for Fluorite eventbus

use crate::{WriterId, TopicId};
use thiserror::Error;

/// Error codes matching wire protocol error messages
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ErrorCode {
    /// Unknown error
    Unknown = 0,
    /// Topic not found
    TopicNotFound = 1,
    /// Schema not found
    SchemaNotFound = 3,
    /// Schema incompatible with existing schemas
    IncompatibleSchema = 4,
    /// Invalid offset requested
    InvalidOffset = 5,
    /// Reader group not found
    GroupNotFound = 6,
    /// Reader not a member of the group
    NotMember = 7,
    /// Reader does not own the requested range
    NotOwner = 9,
    /// Request rate limit exceeded
    RateLimited = 10,
    /// Internal server error
    InternalError = 11,
    /// Authentication failed
    Unauthenticated = 12,
    /// Authorization failed
    Unauthorized = 13,
    /// Duplicate writer sequence number
    DuplicateSequence = 14,
    /// Invalid sequence number (gap detected)
    InvalidSequence = 15,
}

impl ErrorCode {
    /// Whether this error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ErrorCode::RateLimited | ErrorCode::InternalError
        )
    }
}

/// Main error type for Fluorite operations
#[derive(Debug, Error)]
pub enum FluoriteError {
    #[error("topic not found: {topic_id}")]
    TopicNotFound { topic_id: TopicId },

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

    #[error("not owner of range")]
    NotOwner,

    #[error("rate limited")]
    RateLimited,

    #[error("internal error: {message}")]
    InternalError { message: String },

    #[error("unauthenticated")]
    Unauthenticated,

    #[error("unauthorized: {action}")]
    Unauthorized { action: String },

    #[error("duplicate sequence: writer={writer_id}, append_seq={append_seq}")]
    DuplicateSequence {
        writer_id: WriterId,
        append_seq: u64,
    },

    #[error("invalid sequence: expected {expected}, got {actual}")]
    InvalidSequence { expected: u64, actual: u64 },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("database error: {message}")]
    Database { message: String },

    #[error("encoding error: {message}")]
    Encoding { message: String },
}

impl FluoriteError {
    /// Get the error code for this error
    pub fn code(&self) -> ErrorCode {
        match self {
            FluoriteError::TopicNotFound { .. } => ErrorCode::TopicNotFound,
            FluoriteError::SchemaNotFound { .. } => ErrorCode::SchemaNotFound,
            FluoriteError::IncompatibleSchema { .. } => ErrorCode::IncompatibleSchema,
            FluoriteError::InvalidOffset { .. } => ErrorCode::InvalidOffset,
            FluoriteError::GroupNotFound { .. } => ErrorCode::GroupNotFound,
            FluoriteError::NotMember { .. } => ErrorCode::NotMember,
            FluoriteError::NotOwner => ErrorCode::NotOwner,
            FluoriteError::RateLimited => ErrorCode::RateLimited,
            FluoriteError::InternalError { .. } => ErrorCode::InternalError,
            FluoriteError::Unauthenticated => ErrorCode::Unauthenticated,
            FluoriteError::Unauthorized { .. } => ErrorCode::Unauthorized,
            FluoriteError::DuplicateSequence { .. } => ErrorCode::DuplicateSequence,
            FluoriteError::InvalidSequence { .. } => ErrorCode::InvalidSequence,
            FluoriteError::Io(_) => ErrorCode::InternalError,
            FluoriteError::Database { .. } => ErrorCode::InternalError,
            FluoriteError::Encoding { .. } => ErrorCode::InternalError,
        }
    }

    /// Whether this error is retryable
    pub fn is_retryable(&self) -> bool {
        self.code().is_retryable()
    }
}

/// Result type for Fluorite operations
pub type Result<T> = std::result::Result<T, FluoriteError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = FluoriteError::TopicNotFound {
            topic_id: TopicId(42),
        };
        assert!(err.to_string().contains("42"));
    }

    #[test]
    fn test_error_is_retryable() {
        assert!(
            !FluoriteError::TopicNotFound {
                topic_id: TopicId(1)
            }
            .is_retryable()
        );
        assert!(
            FluoriteError::InternalError {
                message: "oops".into()
            }
            .is_retryable()
        );
        assert!(FluoriteError::RateLimited.is_retryable());
    }

    #[test]
    fn test_error_code() {
        let err = FluoriteError::TopicNotFound {
            topic_id: TopicId(1),
        };
        assert_eq!(err.code(), ErrorCode::TopicNotFound);

        let err = FluoriteError::RateLimited;
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