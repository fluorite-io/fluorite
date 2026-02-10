//! Turbine SDK for producing and consuming messages.
//!
//! # Producer Example
//!
//! ```ignore
//! let producer = Producer::connect("ws://localhost:9000").await?;
//! producer.send(topic_id, partition_id, schema_id, records).await?;
//! ```
//!
//! # Consumer Example
//!
//! ```ignore
//! let consumer = Consumer::connect("ws://localhost:9000", "group-1", "consumer-1").await?;
//! let records = consumer.fetch(topic_id, partition_id, offset, max_bytes).await?;
//! ```

pub mod consumer;
pub mod error;
pub mod producer;

pub use consumer::Consumer;
pub use error::SdkError;
pub use producer::Producer;
