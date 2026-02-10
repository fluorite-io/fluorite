//! Agent buffer for batching produce requests.
//!
//! Buffers incoming records from multiple producers, merges them by
//! (topic_id, partition_id, schema_id), and flushes when size or time
//! thresholds are reached.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::sync::oneshot;
use turbine_common::ids::{PartitionId, ProducerId, SchemaId, SeqNum, TopicId};
use turbine_common::types::{Record, Segment, SegmentAck};

/// Key for grouping records in the buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SegmentKey {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub schema_id: SchemaId,
}

/// A pending producer waiting for acknowledgment.
#[derive(Debug)]
pub struct PendingProducer {
    pub producer_id: ProducerId,
    pub seq: SeqNum,
    /// Which segments this producer contributed to.
    pub segment_keys: Vec<SegmentKey>,
    /// Record count per segment key (to calculate offsets).
    pub record_counts: Vec<usize>,
    /// Channel to send acks back to the producer.
    pub ack_sender: oneshot::Sender<Vec<SegmentAck>>,
}

/// Buffered segment data.
#[derive(Debug)]
struct BufferedSegment {
    records: Vec<Record>,
    byte_size: usize,
}

impl BufferedSegment {
    fn new() -> Self {
        Self {
            records: Vec::new(),
            byte_size: 0,
        }
    }

    fn add_record(&mut self, record: Record) {
        self.byte_size += record.value.len();
        if let Some(ref key) = record.key {
            self.byte_size += key.len();
        }
        self.records.push(record);
    }
}

/// Configuration for the agent buffer.
#[derive(Debug, Clone)]
pub struct BufferConfig {
    /// Maximum buffer size in bytes before triggering flush.
    pub max_size_bytes: usize,
    /// Maximum time to wait before flushing.
    pub max_wait: Duration,
    /// High water mark for backpressure (reject new requests).
    pub high_water_bytes: usize,
    /// Low water mark for resuming after backpressure.
    pub low_water_bytes: usize,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 256 * 1024 * 1024, // 256 MB
            max_wait: Duration::from_millis(200),
            high_water_bytes: 384 * 1024 * 1024, // 384 MB
            low_water_bytes: 128 * 1024 * 1024,  // 128 MB
        }
    }
}

/// Result of draining the buffer.
#[derive(Debug)]
pub struct DrainResult {
    /// Merged segments ready to write.
    pub segments: Vec<Segment>,
    /// Pending producers to notify after commit.
    pub pending_producers: Vec<PendingProducer>,
    /// Mapping from segment key to index in segments vec.
    pub key_to_index: HashMap<SegmentKey, usize>,
}

/// Agent buffer for batching produce requests.
pub struct AgentBuffer {
    /// Buffered segments by key.
    segments: HashMap<SegmentKey, BufferedSegment>,
    /// Pending producers waiting for acks.
    pending_producers: Vec<PendingProducer>,
    /// Total bytes in buffer.
    total_bytes: usize,
    /// Time when first record was added to current batch.
    batch_start: Option<Instant>,
    /// Configuration.
    config: BufferConfig,
}

impl AgentBuffer {
    /// Create a new buffer with default config.
    pub fn new() -> Self {
        Self::with_config(BufferConfig::default())
    }

    /// Create a new buffer with custom config.
    pub fn with_config(config: BufferConfig) -> Self {
        Self {
            segments: HashMap::new(),
            pending_producers: Vec::new(),
            total_bytes: 0,
            batch_start: None,
            config,
        }
    }

    /// Insert a produce request into the buffer.
    ///
    /// Returns a receiver that will get the acks when the batch is committed.
    pub fn insert(
        &mut self,
        producer_id: ProducerId,
        seq: SeqNum,
        segments: Vec<Segment>,
    ) -> oneshot::Receiver<Vec<SegmentAck>> {
        let (tx, rx) = oneshot::channel();

        // Track batch start time
        if self.batch_start.is_none() {
            self.batch_start = Some(Instant::now());
        }

        let mut segment_keys = Vec::with_capacity(segments.len());
        let mut record_counts = Vec::with_capacity(segments.len());

        for segment in segments {
            let key = SegmentKey {
                topic_id: segment.topic_id,
                partition_id: segment.partition_id,
                schema_id: segment.schema_id,
            };

            segment_keys.push(key);
            record_counts.push(segment.records.len());

            let buffered = self.segments.entry(key).or_insert_with(BufferedSegment::new);

            for record in segment.records {
                let record_size = record.value.len() + record.key.as_ref().map(|k| k.len()).unwrap_or(0);
                self.total_bytes += record_size;
                buffered.add_record(record);
            }
        }

        self.pending_producers.push(PendingProducer {
            producer_id,
            seq,
            segment_keys,
            record_counts,
            ack_sender: tx,
        });

        rx
    }

    /// Check if the buffer should be flushed.
    pub fn should_flush(&self) -> bool {
        // Flush on size
        if self.total_bytes >= self.config.max_size_bytes {
            return true;
        }

        // Flush on time
        if let Some(start) = self.batch_start {
            if start.elapsed() >= self.config.max_wait {
                return true;
            }
        }

        false
    }

    /// Check if backpressure should be applied.
    pub fn should_apply_backpressure(&self) -> bool {
        self.total_bytes >= self.config.high_water_bytes
    }

    /// Check if backpressure can be released.
    pub fn can_release_backpressure(&self) -> bool {
        self.total_bytes <= self.config.low_water_bytes
    }

    /// Get current buffer size in bytes.
    pub fn size_bytes(&self) -> usize {
        self.total_bytes
    }

    /// Get number of pending producers.
    pub fn pending_count(&self) -> usize {
        self.pending_producers.len()
    }

    /// Get number of segments in buffer.
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// Check if buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Drain the buffer, returning merged segments and pending producers.
    pub fn drain(&mut self) -> DrainResult {
        let mut segments = Vec::with_capacity(self.segments.len());
        let mut key_to_index = HashMap::with_capacity(self.segments.len());

        for (key, buffered) in self.segments.drain() {
            let index = segments.len();
            key_to_index.insert(key, index);

            segments.push(Segment {
                topic_id: key.topic_id,
                partition_id: key.partition_id,
                schema_id: key.schema_id,
                records: buffered.records,
            });
        }

        let pending_producers = std::mem::take(&mut self.pending_producers);

        self.total_bytes = 0;
        self.batch_start = None;

        DrainResult {
            segments,
            pending_producers,
            key_to_index,
        }
    }

    /// Distribute acks to pending producers after successful commit.
    ///
    /// `segment_offsets` maps segment index to (start_offset, end_offset).
    pub fn distribute_acks(
        drain_result: DrainResult,
        segment_offsets: &[(u64, u64)],
    ) {
        // Track current offset within each segment (shared across all producers)
        let mut offset_within_segment: HashMap<SegmentKey, u64> = HashMap::new();

        // Initialize with segment start offsets
        for (key, &seg_idx) in &drain_result.key_to_index {
            let (seg_start, _) = segment_offsets[seg_idx];
            offset_within_segment.insert(*key, seg_start);
        }

        for pending in drain_result.pending_producers {
            let mut acks = Vec::with_capacity(pending.segment_keys.len());

            for (key, record_count) in pending.segment_keys.iter().zip(pending.record_counts.iter()) {
                if let Some(&seg_idx) = drain_result.key_to_index.get(key) {
                    let (_, seg_end) = segment_offsets[seg_idx];

                    // Get this producer's start offset within the segment
                    let producer_start = *offset_within_segment.get(key).unwrap();
                    let producer_end = producer_start + *record_count as u64;

                    acks.push(SegmentAck {
                        topic_id: key.topic_id,
                        partition_id: key.partition_id,
                        schema_id: key.schema_id,
                        start_offset: turbine_common::ids::Offset(producer_start),
                        end_offset: turbine_common::ids::Offset(producer_end),
                    });

                    // Update offset for next producer contributing to same segment
                    *offset_within_segment.get_mut(key).unwrap() = producer_end;

                    // Sanity check
                    debug_assert!(producer_end <= seg_end);
                }
            }

            // Send acks to producer (ignore error if receiver dropped)
            let _ = pending.ack_sender.send(acks);
        }
    }
}

impl Default for AgentBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use uuid::Uuid;

    fn make_record(key: &str, value: &str) -> Record {
        Record {
            key: Some(Bytes::from(key.to_string())),
            value: Bytes::from(value.to_string()),
        }
    }

    fn make_segment(topic: u32, partition: u32, records: Vec<Record>) -> Segment {
        Segment {
            topic_id: TopicId(topic),
            partition_id: PartitionId(partition),
            schema_id: SchemaId(100),
            records,
        }
    }

    #[test]
    fn test_buffer_insert_single() {
        let mut buffer = AgentBuffer::new();

        let producer_id = ProducerId(Uuid::new_v4());
        let segments = vec![make_segment(1, 0, vec![make_record("k1", "v1")])];

        let _rx = buffer.insert(producer_id, SeqNum(1), segments);

        assert_eq!(buffer.segment_count(), 1);
        assert_eq!(buffer.pending_count(), 1);
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_buffer_merge_same_key() {
        let mut buffer = AgentBuffer::new();

        // Two producers send to same partition
        let p1 = ProducerId(Uuid::new_v4());
        let p2 = ProducerId(Uuid::new_v4());

        let _rx1 = buffer.insert(p1, SeqNum(1), vec![
            make_segment(1, 0, vec![make_record("k1", "v1"), make_record("k2", "v2")]),
        ]);

        let _rx2 = buffer.insert(p2, SeqNum(1), vec![
            make_segment(1, 0, vec![make_record("k3", "v3")]),
        ]);

        // Should have 1 merged segment with 3 records
        assert_eq!(buffer.segment_count(), 1);
        assert_eq!(buffer.pending_count(), 2);

        let result = buffer.drain();
        assert_eq!(result.segments.len(), 1);
        assert_eq!(result.segments[0].records.len(), 3);
        assert_eq!(result.pending_producers.len(), 2);
    }

    #[test]
    fn test_buffer_multiple_partitions() {
        let mut buffer = AgentBuffer::new();

        let producer = ProducerId(Uuid::new_v4());
        let segments = vec![
            make_segment(1, 0, vec![make_record("k1", "v1")]),
            make_segment(1, 1, vec![make_record("k2", "v2")]),
            make_segment(2, 0, vec![make_record("k3", "v3")]),
        ];

        let _rx = buffer.insert(producer, SeqNum(1), segments);

        assert_eq!(buffer.segment_count(), 3);
        assert_eq!(buffer.pending_count(), 1);
    }

    #[test]
    fn test_buffer_flush_on_size() {
        let config = BufferConfig {
            max_size_bytes: 100,
            max_wait: Duration::from_secs(60),
            ..Default::default()
        };
        let mut buffer = AgentBuffer::with_config(config);

        // Add small record - should not trigger flush
        let p1 = ProducerId(Uuid::new_v4());
        let _rx1 = buffer.insert(p1, SeqNum(1), vec![
            make_segment(1, 0, vec![make_record("k", "small")]),
        ]);
        assert!(!buffer.should_flush());

        // Add large record - should trigger flush
        let p2 = ProducerId(Uuid::new_v4());
        let large_value = "x".repeat(100);
        let _rx2 = buffer.insert(p2, SeqNum(1), vec![
            make_segment(1, 0, vec![make_record("k", &large_value)]),
        ]);
        assert!(buffer.should_flush());
    }

    #[test]
    fn test_buffer_flush_on_time() {
        let config = BufferConfig {
            max_size_bytes: 1024 * 1024,
            max_wait: Duration::from_millis(1),
            ..Default::default()
        };
        let mut buffer = AgentBuffer::with_config(config);

        let producer = ProducerId(Uuid::new_v4());
        let _rx = buffer.insert(producer, SeqNum(1), vec![
            make_segment(1, 0, vec![make_record("k", "v")]),
        ]);

        // Initially should not flush (time not elapsed)
        // Wait and check again
        std::thread::sleep(Duration::from_millis(5));
        assert!(buffer.should_flush());
    }

    #[test]
    fn test_buffer_drain_clears() {
        let mut buffer = AgentBuffer::new();

        let producer = ProducerId(Uuid::new_v4());
        let _rx = buffer.insert(producer, SeqNum(1), vec![
            make_segment(1, 0, vec![make_record("k", "v")]),
        ]);

        assert!(!buffer.is_empty());
        let _result = buffer.drain();
        assert!(buffer.is_empty());
        assert_eq!(buffer.size_bytes(), 0);
        assert_eq!(buffer.pending_count(), 0);
    }

    #[test]
    fn test_distribute_acks() {
        let mut buffer = AgentBuffer::new();

        // Producer 1 sends 2 records
        let p1 = ProducerId(Uuid::new_v4());
        let mut rx1 = buffer.insert(p1, SeqNum(1), vec![
            make_segment(1, 0, vec![make_record("k1", "v1"), make_record("k2", "v2")]),
        ]);

        // Producer 2 sends 1 record to same partition
        let p2 = ProducerId(Uuid::new_v4());
        let mut rx2 = buffer.insert(p2, SeqNum(1), vec![
            make_segment(1, 0, vec![make_record("k3", "v3")]),
        ]);

        let result = buffer.drain();

        // Simulate DB assigned offsets 0-3 for the merged segment
        let segment_offsets = vec![(0, 3)];

        AgentBuffer::distribute_acks(result, &segment_offsets);

        // Producer 1 should get offsets 0-2
        let acks1 = rx1.try_recv().unwrap();
        assert_eq!(acks1.len(), 1);
        assert_eq!(acks1[0].start_offset.0, 0);
        assert_eq!(acks1[0].end_offset.0, 2);

        // Producer 2 should get offsets 2-3
        let acks2 = rx2.try_recv().unwrap();
        assert_eq!(acks2.len(), 1);
        assert_eq!(acks2[0].start_offset.0, 2);
        assert_eq!(acks2[0].end_offset.0, 3);
    }

    #[test]
    fn test_backpressure() {
        let config = BufferConfig {
            max_size_bytes: 1024,
            max_wait: Duration::from_secs(60),
            high_water_bytes: 200,
            low_water_bytes: 100,
        };
        let mut buffer = AgentBuffer::with_config(config);

        // Add data to exceed high water
        let producer = ProducerId(Uuid::new_v4());
        let large_value = "x".repeat(250);
        let _rx = buffer.insert(producer, SeqNum(1), vec![
            make_segment(1, 0, vec![make_record("k", &large_value)]),
        ]);

        assert!(buffer.should_apply_backpressure());
        assert!(!buffer.can_release_backpressure());

        // Drain to release
        let _result = buffer.drain();
        assert!(!buffer.should_apply_backpressure());
        assert!(buffer.can_release_backpressure());
    }
}
