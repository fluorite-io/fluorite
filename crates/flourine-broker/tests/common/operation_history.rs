//! Operation history for linearizability checking in Jepsen-style tests.

use bytes::Bytes;
use std::time::Instant;

use flourine_common::ids::{Offset, WriterId};

/// A write operation for linearizability tracking.
#[derive(Debug, Clone)]
pub struct WriteOp {
    pub writer_id: WriterId,
    pub offset: Option<Offset>,
    pub value: Bytes,
    pub start_time: Instant,
    pub end_time: Option<Instant>,
    pub success: bool,
}

/// A read operation for linearizability tracking.
#[derive(Debug, Clone)]
pub struct ReadOp {
    pub requested_offset: Offset,
    pub returned_values: Vec<Bytes>,
    pub high_watermark: Offset,
    pub start_time: Instant,
    pub end_time: Option<Instant>,
    pub success: bool,
}

/// Track operation history for linearizability checking.
#[derive(Default)]
pub struct OperationHistory {
    pub writes: Vec<WriteOp>,
    pub reads: Vec<ReadOp>,
}

impl OperationHistory {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_write_start(&mut self, writer_id: WriterId, value: Bytes) -> usize {
        let idx = self.writes.len();
        self.writes.push(WriteOp {
            writer_id,
            offset: None,
            value,
            start_time: Instant::now(),
            end_time: None,
            success: false,
        });
        idx
    }

    pub fn record_write_complete(&mut self, idx: usize, offset: Option<Offset>, success: bool) {
        if let Some(op) = self.writes.get_mut(idx) {
            op.offset = offset;
            op.end_time = Some(Instant::now());
            op.success = success;
        }
    }

    pub fn record_read_start(&mut self, offset: Offset) -> usize {
        let idx = self.reads.len();
        self.reads.push(ReadOp {
            requested_offset: offset,
            returned_values: vec![],
            high_watermark: Offset(0),
            start_time: Instant::now(),
            end_time: None,
            success: false,
        });
        idx
    }

    pub fn record_read_complete(
        &mut self,
        idx: usize,
        values: Vec<Bytes>,
        high_watermark: Offset,
        success: bool,
    ) {
        if let Some(op) = self.reads.get_mut(idx) {
            op.returned_values = values;
            op.high_watermark = high_watermark;
            op.end_time = Some(Instant::now());
            op.success = success;
        }
    }

    /// Check that all acknowledged writes are eventually visible.
    pub fn verify_acknowledged_writes_visible(&self) -> Result<(), String> {
        let successful_writes: Vec<_> = self
            .writes
            .iter()
            .filter(|w| w.success && w.offset.is_some())
            .collect();

        if successful_writes.is_empty() {
            return Ok(());
        }

        let final_read = self
            .reads
            .iter()
            .filter(|r| r.success)
            .max_by_key(|r| r.high_watermark.0);

        let Some(final_read) = final_read else {
            return Err("No successful reads to verify against".to_string());
        };

        for write in &successful_writes {
            if let (Some(offset), Some(write_end_time)) = (write.offset, write.end_time) {
                if write_end_time < final_read.start_time && offset.0 < final_read.high_watermark.0
                {
                    if !final_read.returned_values.contains(&write.value) {
                        return Err(format!(
                            "Acknowledged write at offset {} with value {:?} not visible in final read \
                             (watermark {}, write ended {:?} before read started {:?})",
                            offset.0,
                            write.value,
                            final_read.high_watermark.0,
                            write_end_time,
                            final_read.start_time
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// Check that offsets are unique (no two writes got the same offset).
    pub fn verify_unique_offsets(&self) -> Result<(), String> {
        let mut seen = std::collections::HashSet::new();
        for write in &self.writes {
            if write.success {
                if let Some(offset) = write.offset {
                    if !seen.insert(offset.0) {
                        return Err(format!(
                            "Duplicate offset {} assigned to multiple writes",
                            offset.0
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    /// Check that watermark never regresses across reads.
    pub fn verify_watermark_monotonic(&self) -> Result<(), String> {
        let successful_reads: Vec<_> = self.reads.iter().filter(|r| r.success).collect();

        for window in successful_reads.windows(2) {
            let (prev, curr) = (&window[0], &window[1]);
            if curr.high_watermark.0 < prev.high_watermark.0 {
                return Err(format!(
                    "Watermark regressed from {} to {}",
                    prev.high_watermark.0, curr.high_watermark.0
                ));
            }
        }
        Ok(())
    }
}
