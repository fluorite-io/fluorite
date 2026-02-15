//! Core ID types for Turbine eventbus

use std::fmt;

/// Unique identifier for a topic
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct TopicId(pub u32);

/// Unique identifier for a partition within a topic
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct PartitionId(pub u32);

/// Unique identifier for a schema in the registry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct SchemaId(pub u32);

/// Offset within a partition (monotonically increasing)
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Offset(pub u64);

/// Unique identifier for a writer instance
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct WriterId(pub uuid::Uuid);

/// Sequence number for writer deduplication
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct AppendSeq(pub u64);

/// Generation number for reader group coordination
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct Generation(pub u64);

// From implementations for TopicId
impl From<TopicId> for u32 {
    fn from(id: TopicId) -> u32 {
        id.0
    }
}

impl From<u32> for TopicId {
    fn from(val: u32) -> TopicId {
        TopicId(val)
    }
}

// From implementations for PartitionId
impl From<PartitionId> for u32 {
    fn from(id: PartitionId) -> u32 {
        id.0
    }
}

impl From<u32> for PartitionId {
    fn from(val: u32) -> PartitionId {
        PartitionId(val)
    }
}

// From implementations for SchemaId
impl From<SchemaId> for u32 {
    fn from(id: SchemaId) -> u32 {
        id.0
    }
}

impl From<u32> for SchemaId {
    fn from(val: u32) -> SchemaId {
        SchemaId(val)
    }
}

// From implementations for Offset
impl From<Offset> for u64 {
    fn from(offset: Offset) -> u64 {
        offset.0
    }
}

impl From<u64> for Offset {
    fn from(val: u64) -> Offset {
        Offset(val)
    }
}

// From implementations for AppendSeq
impl From<AppendSeq> for u64 {
    fn from(append_seq: AppendSeq) -> u64 {
        append_seq.0
    }
}

impl From<u64> for AppendSeq {
    fn from(val: u64) -> AppendSeq {
        AppendSeq(val)
    }
}

// From implementations for Generation
impl From<Generation> for u64 {
    fn from(generation: Generation) -> u64 {
        generation.0
    }
}

impl From<u64> for Generation {
    fn from(val: u64) -> Generation {
        Generation(val)
    }
}

// Display implementations
impl fmt::Display for TopicId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for SchemaId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Offset {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for WriterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for AppendSeq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Generation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

// WriterId specific implementations
impl WriterId {
    /// Create a new random writer ID
    pub fn new() -> Self {
        WriterId(uuid::Uuid::new_v4())
    }

    /// Create a writer ID from a UUID
    pub fn from_uuid(uuid: uuid::Uuid) -> Self {
        WriterId(uuid)
    }

    /// Get the underlying UUID
    pub fn as_uuid(&self) -> &uuid::Uuid {
        &self.0
    }
}

impl Default for WriterId {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_topic_id_into_u32() {
        let id = TopicId(42);
        assert_eq!(u32::from(id), 42);
    }

    #[test]
    fn test_topic_id_from_u32() {
        let id: TopicId = 42u32.into();
        assert_eq!(id.0, 42);
    }

    #[test]
    fn test_offset_ordering() {
        let a = Offset(100);
        let b = Offset(200);
        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, Offset(100));
    }

    #[test]
    fn test_producer_id_display() {
        let id = WriterId(uuid::Uuid::nil());
        assert_eq!(format!("{}", id), "00000000-0000-0000-0000-000000000000");
    }

    #[test]
    fn test_producer_id_new_is_unique() {
        let id1 = WriterId::new();
        let id2 = WriterId::new();
        assert_ne!(id1, id2);
    }

    #[test]
    fn test_seq_num_ordering() {
        let a = AppendSeq(1);
        let b = AppendSeq(2);
        assert!(a < b);
    }

    #[test]
    fn test_generation_ordering() {
        let a = Generation(1);
        let b = Generation(2);
        assert!(a < b);
    }

    #[test]
    fn test_partition_id_display() {
        let id = PartitionId(5);
        assert_eq!(format!("{}", id), "5");
    }

    #[test]
    fn test_schema_id_conversions() {
        let id = SchemaId(100);
        let val: u32 = id.into();
        assert_eq!(val, 100);

        let id2: SchemaId = 100u32.into();
        assert_eq!(id, id2);
    }
}
