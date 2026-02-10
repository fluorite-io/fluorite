//! Wire protocol encoding/decoding for Turbine eventbus.
//!
//! Uses Avro-compatible zigzag varint encoding for integers,
//! length-prefixed bytes, and index-prefixed unions.

pub mod consumer;
pub mod error;
pub mod producer;
pub mod record;
pub mod varint;

#[cfg(test)]
mod proptest_tests;

pub use error::{DecodeError, EncodeError};
