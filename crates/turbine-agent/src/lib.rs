//! Turbine agent: stateless worker that handles produce/fetch requests.
//!
//! The agent receives WebSocket connections from producers and consumers,
//! batches writes to S3, and commits metadata to Postgres.

pub mod buffer;
pub mod dedup;
pub mod error;
pub mod object_store;
pub mod server;
pub mod tbin;

pub use buffer::{AgentBuffer, BufferConfig, DrainResult, PendingProducer, SegmentKey};
pub use dedup::{DedupCache, DedupCacheConfig, DedupResult, ProducerState};
pub use error::AgentError;
pub use object_store::{LocalFsStore, ObjectStore, S3ObjectStore};
pub use server::{run, AgentConfig, AgentState};
pub use tbin::{TbinReader, TbinWriter};
