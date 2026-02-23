//! Read request handling.

use tracing::error;

use flourine_common::ids::{Offset, SchemaId};
use flourine_wire::{ERR_INTERNAL_ERROR, STATUS_OK, ServerMessage, reader};

use crate::metrics::{LatencyTimer, READ_LATENCY_SECONDS, READ_REQUESTS_TOTAL};
use crate::object_store::ObjectStore;
use crate::fl::{Codec, SegmentMeta, FlReader};
use crate::BrokerError;

use super::encoding::encode_server_message_vec;
use super::BrokerState;

/// Handle a ReadRequest (reads from S3).
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(
        group_id = %req.group_id,
        reader_id = %req.reader_id,
        fetch_count = req.reads.len()
    )
)]
pub(crate) async fn handle_read_request<S: ObjectStore + Send + Sync>(
    req: reader::ReadRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    let _timer = LatencyTimer::new(&READ_LATENCY_SECONDS);
    READ_REQUESTS_TOTAL.inc();

    let result = process_read(req, state).await;

    let response = match result {
        Ok(results) => reader::ReadResponse {
            success: true,
            error_code: STATUS_OK,
            error_message: String::new(),
            results,
        },
        Err(e) => {
            error!("Read error: {}", e);
            reader::ReadResponse {
                success: false,
                error_code: ERR_INTERNAL_ERROR,
                error_message: "read failed".to_string(),
                results: vec![],
            }
        }
    };

    // Calculate required buffer size: headers + records
    let total_record_bytes: usize = response
        .results
        .iter()
        .flat_map(|r| &r.records)
        .map(|rec| rec.value.len() + rec.key.as_ref().map(|k| k.len()).unwrap_or(0) + 32)
        .sum();
    let buf_size = (total_record_bytes + 1024).max(64 * 1024);

    encode_server_message_vec(ServerMessage::Read(response), buf_size + 16)
}

/// Process a read request.
async fn process_read<S: ObjectStore + Send + Sync>(
    req: reader::ReadRequest,
    state: &BrokerState<S>,
) -> Result<Vec<reader::PartitionResult>, BrokerError> {
    let mut results = Vec::with_capacity(req.reads.len().saturating_mul(2));

    for read in &req.reads {
        let batches: Vec<(i32, i64, i64, String, i64, i64, i64)> = sqlx::query_as(
            r#"
            SELECT schema_id, start_offset, end_offset, s3_key, byte_offset, byte_length, crc32
            FROM topic_batches
            WHERE topic_id = $1
              AND partition_id = $2
              AND end_offset > $3
            ORDER BY start_offset
            LIMIT 10
            "#,
        )
        .bind(read.topic_id.0 as i32)
        .bind(read.partition_id.0 as i32)
        .bind(read.offset.0 as i64)
        .fetch_all(&state.pool)
        .await?;

        let high_watermark: i64 = sqlx::query_scalar(
            "SELECT COALESCE(next_offset, 0) FROM partition_offsets WHERE topic_id = $1 AND partition_id = $2",
        )
        .bind(read.topic_id.0 as i32)
        .bind(read.partition_id.0 as i32)
        .fetch_optional(&state.pool)
        .await?
        .unwrap_or(0);

        let mut grouped_records = Vec::with_capacity(batches.len().max(1));
        let mut current_schema: Option<SchemaId> = None;
        let expected_records = batches
            .iter()
            .map(|(_, start_offset, end_offset, _, _, _, _)| {
                end_offset.saturating_sub(*start_offset) as usize
            })
            .sum::<usize>();
        let mut current_records = Vec::with_capacity(expected_records.min(16_384));
        let mut total_bytes: usize = 0;
        let mut reached_limit = false;
        let max_bytes = read.max_bytes as usize;

        for (sid, start_offset, end_offset, s3_key, byte_offset, byte_length, crc32) in batches {
            let schema_id = SchemaId(sid as u32);

            // S3 range read - read only this batch's bytes
            let segment_bytes = state
                .store
                .get_range(&s3_key, byte_offset as u64, byte_length as u64)
                .await?;

            // Create metadata for decoding (byte_offset=0 since we have just the batch bytes)
            let meta = SegmentMeta {
                topic_id: read.topic_id,
                partition_id: read.partition_id,
                schema_id,
                start_offset: Offset(start_offset as u64),
                end_offset: Offset(end_offset as u64),
                record_count: 0, // Not needed for read_segment
                byte_offset: 0,  // Data starts at 0 in our slice
                byte_length: byte_length as u64,
                ingest_time: 0,
                compression: Codec::Zstd,
                crc32: crc32 as u32,
            };

            let records = FlReader::read_segment(&segment_bytes, &meta, true)?;
            let skip = (read.offset.0 as i64 - start_offset).max(0) as usize;
            if skip >= records.len() {
                continue;
            }

            if current_schema != Some(schema_id) {
                if let Some(prev_schema) = current_schema {
                    if !current_records.is_empty() {
                        grouped_records.push(reader::PartitionResult {
                            topic_id: read.topic_id,
                            partition_id: read.partition_id,
                            schema_id: prev_schema,
                            high_watermark: Offset(high_watermark as u64),
                            records: std::mem::take(&mut current_records),
                        });
                    }
                }
                current_schema = Some(schema_id);
            }

            for record in records.into_iter().skip(skip) {
                total_bytes +=
                    record.value.len() + record.key.as_ref().map(|k| k.len()).unwrap_or(0);
                current_records.push(record);
                if total_bytes >= max_bytes {
                    reached_limit = true;
                    break;
                }
            }

            if reached_limit {
                break;
            }
        }

        if let Some(schema_id) = current_schema {
            if !current_records.is_empty() {
                grouped_records.push(reader::PartitionResult {
                    topic_id: read.topic_id,
                    partition_id: read.partition_id,
                    schema_id,
                    high_watermark: Offset(high_watermark as u64),
                    records: current_records,
                });
            }
        } else {
            grouped_records.push(reader::PartitionResult {
                topic_id: read.topic_id,
                partition_id: read.partition_id,
                schema_id: SchemaId(0),
                high_watermark: Offset(high_watermark as u64),
                records: vec![],
            });
        }

        results.reserve(grouped_records.len());
        results.extend(grouped_records.into_iter());
    }

    Ok(results)
}
