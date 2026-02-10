//! Schema registry for Turbine eventbus.
//!
//! Provides centralized schema storage with:
//! - Content-addressed deduplication (SHA-256 hash)
//! - Backward compatibility enforcement
//! - Topic-schema association
//!
//! Reserved schema IDs 1-99 are for protocol schemas.
//! User schemas start at ID 100.

pub mod canonical;
pub mod compat;
pub mod error;
pub mod registry;

pub use canonical::{canonicalize, schema_hash};
pub use compat::is_backward_compatible;
pub use error::SchemaError;
pub use registry::SchemaRegistry;
