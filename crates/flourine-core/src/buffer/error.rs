use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum FlourineError {
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

impl IntoResponse for FlourineError {
    fn into_response(self) -> Response {
        let status = match &self {
            FlourineError::AvroParse(_) => StatusCode::BAD_REQUEST,
            FlourineError::InvalidSchema(_) => StatusCode::BAD_REQUEST,
            FlourineError::Conversion(_) => StatusCode::UNPROCESSABLE_ENTITY,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };

        (status, self.to_string()).into_response()
    }
}

pub type Result<T> = std::result::Result<T, FlourineError>;
