//! Wire protocol encoding/decoding for Flourine eventbus.
//!
//! Uses Avro-compatible zigzag varint encoding for integers,
//! length-prefixed bytes, and protobuf oneof-style envelopes.

pub mod auth;
pub mod reader;
pub mod error;
pub mod writer;
pub(crate) mod proto;
pub(crate) mod proto_conv;
pub mod record;
pub mod status;
pub mod union;
pub mod varint;

#[cfg(test)]
mod proptest_tests;

pub use auth::{
    AuthRequest, AuthResponse, decode_auth_request, decode_auth_response, encode_auth_request,
    encode_auth_response,
};
pub use error::{DecodeError, EncodeError};
pub use status::*;
pub use union::{
    ClientMessage, ServerMessage, decode_client_message, decode_server_message,
    encode_client_message, encode_server_message,
};
