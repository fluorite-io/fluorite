//! Turbine agent: stateless worker that handles produce/fetch requests.
//!
//! The agent receives WebSocket connections from producers and consumers,
//! batches writes to S3, and commits metadata to Postgres.

pub mod object_store;
pub mod tbin;

pub use object_store::{LocalFsStore, ObjectStore, S3ObjectStore};
pub use tbin::{TbinReader, TbinWriter};
