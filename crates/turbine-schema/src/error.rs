//! Schema registry errors.

use thiserror::Error;
use turbine_common::ids::{SchemaId, TopicId};

/// Schema registry error type.
#[derive(Debug, Error)]
pub enum SchemaError {
    #[error("schema not found: {schema_id}")]
    SchemaNotFound { schema_id: SchemaId },

    #[error("topic not found: {topic_id}")]
    TopicNotFound { topic_id: TopicId },

    #[error("incompatible schema: {message}")]
    IncompatibleSchema { message: String },

    #[error("invalid schema: {message}")]
    InvalidSchema { message: String },

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("avro error: {0}")]
    Avro(String),
}

impl From<apache_avro::Error> for SchemaError {
    fn from(e: apache_avro::Error) -> Self {
        SchemaError::Avro(e.to_string())
    }
}
