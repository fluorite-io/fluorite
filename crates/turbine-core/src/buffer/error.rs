use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TurbineError {
    #[error("Avro parsing error: {0}")]
    AvroParse(#[from] apache_avro::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid schema: {0}")]
    InvalidSchema(String),

    #[error("Conversion error: {0}")]
    Conversion(String),
}

impl IntoResponse for TurbineError {
    fn into_response(self) -> Response {
        let status = match &self {
            TurbineError::AvroParse(_) => StatusCode::BAD_REQUEST,
            TurbineError::InvalidSchema(_) => StatusCode::BAD_REQUEST,
            TurbineError::Conversion(_) => StatusCode::UNPROCESSABLE_ENTITY,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        (status, self.to_string()).into_response()
    }
}

pub type Result<T> = std::result::Result<T, TurbineError>;
