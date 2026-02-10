//! Wire protocol encoding/decoding for Turbine eventbus.
//!
//! Uses Avro-compatible zigzag varint encoding for integers,
//! length-prefixed bytes, and index-prefixed unions.

pub mod varint;
pub mod record;
pub mod producer;
pub mod consumer;
pub mod error;

pub use error::{DecodeError, EncodeError};
