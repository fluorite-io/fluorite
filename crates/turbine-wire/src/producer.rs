//! Producer protocol messages: ProduceRequest and ProduceResponse.

use turbine_common::ids::{Offset, PartitionId, ProducerId, SchemaId, SeqNum, TopicId};
use turbine_common::types::{Segment, SegmentAck};
use uuid::Uuid;

use crate::{record, varint, DecodeError};

/// A produce request from a producer to an agent.
#[derive(Debug, Clone)]
pub struct ProduceRequest {
    pub producer_id: ProducerId,
    pub seq: SeqNum,
    pub segments: Vec<Segment>,
}

/// A produce response from an agent to a producer.
#[derive(Debug, Clone)]
pub struct ProduceResponse {
    pub seq: SeqNum,
    pub acks: Vec<SegmentAck>,
}

/// Encode a ProduceRequest into the buffer, returning bytes written.
pub fn encode_request(req: &ProduceRequest, buf: &mut [u8]) -> usize {
    let mut offset = 0;

    // Producer ID: 16 bytes (UUID)
    buf[offset..offset + 16].copy_from_slice(req.producer_id.0.as_bytes());
    offset += 16;

    // Sequence number: varint
    offset += varint::encode_u64(req.seq.0, &mut buf[offset..]);

    // Segments count: varint
    offset += varint::encode_i64(req.segments.len() as i64, &mut buf[offset..]);

    // Each segment
    for segment in &req.segments {
        offset += encode_segment(segment, &mut buf[offset..]);
    }

    offset
}

/// Decode a ProduceRequest from the buffer.
pub fn decode_request(buf: &[u8]) -> Result<(ProduceRequest, usize), DecodeError> {
    let mut offset = 0;

    // Producer ID: 16 bytes
    if buf.len() < 16 {
        return Err(DecodeError::UnexpectedEof { needed: 16 });
    }
    let producer_id = ProducerId(Uuid::from_bytes(
        buf[offset..offset + 16].try_into().unwrap(),
    ));
    offset += 16;

    // Sequence number
    let (seq, len) = varint::decode_u64(&buf[offset..])?;
    offset += len;

    // Segments count
    let (count, len) = varint::decode_i64(&buf[offset..])?;
    offset += len;

    let mut segments = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (segment, len) = decode_segment(&buf[offset..])?;
        offset += len;
        segments.push(segment);
    }

    Ok((
        ProduceRequest {
            producer_id,
            seq: SeqNum(seq),
            segments,
        },
        offset,
    ))
}

/// Encode a ProduceResponse into the buffer, returning bytes written.
pub fn encode_response(resp: &ProduceResponse, buf: &mut [u8]) -> usize {
    let mut offset = 0;

    // Sequence number
    offset += varint::encode_u64(resp.seq.0, &mut buf[offset..]);

    // Acks count
    offset += varint::encode_i64(resp.acks.len() as i64, &mut buf[offset..]);

    // Each ack
    for ack in &resp.acks {
        offset += encode_segment_ack(ack, &mut buf[offset..]);
    }

    offset
}

/// Decode a ProduceResponse from the buffer.
pub fn decode_response(buf: &[u8]) -> Result<(ProduceResponse, usize), DecodeError> {
    let mut offset = 0;

    // Sequence number
    let (seq, len) = varint::decode_u64(&buf[offset..])?;
    offset += len;

    // Acks count
    let (count, len) = varint::decode_i64(&buf[offset..])?;
    offset += len;

    let mut acks = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (ack, len) = decode_segment_ack(&buf[offset..])?;
        offset += len;
        acks.push(ack);
    }

    Ok((
        ProduceResponse {
            seq: SeqNum(seq),
            acks,
        },
        offset,
    ))
}

/// Encode a Segment into the buffer.
fn encode_segment(segment: &Segment, buf: &mut [u8]) -> usize {
    let mut offset = 0;

    // Topic ID: u32 as varint
    offset += varint::encode_i64(segment.topic_id.0 as i64, &mut buf[offset..]);

    // Partition ID: u32 as varint
    offset += varint::encode_i64(segment.partition_id.0 as i64, &mut buf[offset..]);

    // Schema ID: u32 as varint
    offset += varint::encode_i64(segment.schema_id.0 as i64, &mut buf[offset..]);

    // Records count
    offset += varint::encode_i64(segment.records.len() as i64, &mut buf[offset..]);

    // Each record
    for rec in &segment.records {
        offset += record::encode(rec, &mut buf[offset..]);
    }

    offset
}

/// Decode a Segment from the buffer.
fn decode_segment(buf: &[u8]) -> Result<(Segment, usize), DecodeError> {
    let mut offset = 0;

    let (topic_id, len) = varint::decode_i64(&buf[offset..])?;
    offset += len;

    let (partition_id, len) = varint::decode_i64(&buf[offset..])?;
    offset += len;

    let (schema_id, len) = varint::decode_i64(&buf[offset..])?;
    offset += len;

    let (count, len) = varint::decode_i64(&buf[offset..])?;
    offset += len;

    let mut records = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (rec, len) = record::decode(&buf[offset..])?;
        offset += len;
        records.push(rec);
    }

    Ok((
        Segment {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(partition_id as u32),
            schema_id: SchemaId(schema_id as u32),
            records,
        },
        offset,
    ))
}

/// Encode a SegmentAck into the buffer.
fn encode_segment_ack(ack: &SegmentAck, buf: &mut [u8]) -> usize {
    let mut offset = 0;

    offset += varint::encode_i64(ack.topic_id.0 as i64, &mut buf[offset..]);
    offset += varint::encode_i64(ack.partition_id.0 as i64, &mut buf[offset..]);
    offset += varint::encode_i64(ack.schema_id.0 as i64, &mut buf[offset..]);
    offset += varint::encode_u64(ack.start_offset.0, &mut buf[offset..]);
    offset += varint::encode_u64(ack.end_offset.0, &mut buf[offset..]);

    offset
}

/// Decode a SegmentAck from the buffer.
fn decode_segment_ack(buf: &[u8]) -> Result<(SegmentAck, usize), DecodeError> {
    let mut offset = 0;

    let (topic_id, len) = varint::decode_i64(&buf[offset..])?;
    offset += len;

    let (partition_id, len) = varint::decode_i64(&buf[offset..])?;
    offset += len;

    let (schema_id, len) = varint::decode_i64(&buf[offset..])?;
    offset += len;

    let (start_offset, len) = varint::decode_u64(&buf[offset..])?;
    offset += len;

    let (end_offset, len) = varint::decode_u64(&buf[offset..])?;
    offset += len;

    Ok((
        SegmentAck {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(partition_id as u32),
            schema_id: SchemaId(schema_id as u32),
            start_offset: Offset(start_offset),
            end_offset: Offset(end_offset),
        },
        offset,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use turbine_common::types::Record;

    fn sample_segment() -> Segment {
        Segment {
            topic_id: TopicId(1),
            partition_id: PartitionId(0),
            schema_id: SchemaId(100),
            records: vec![
                Record {
                    key: Some(Bytes::from_static(b"key1")),
                    value: Bytes::from_static(b"value1"),
                },
                Record {
                    key: None,
                    value: Bytes::from_static(b"value2"),
                },
            ],
        }
    }

    #[test]
    fn test_produce_request_roundtrip() {
        let req = ProduceRequest {
            producer_id: ProducerId(Uuid::new_v4()),
            seq: SeqNum(42),
            segments: vec![sample_segment()],
        };

        let mut buf = [0u8; 256];
        let encoded_len = encode_request(&req, &mut buf);

        let (decoded, decoded_len) = decode_request(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded.producer_id.0, req.producer_id.0);
        assert_eq!(decoded.seq.0, req.seq.0);
        assert_eq!(decoded.segments.len(), 1);
        assert_eq!(decoded.segments[0].records.len(), 2);
    }

    #[test]
    fn test_produce_response_roundtrip() {
        let resp = ProduceResponse {
            seq: SeqNum(42),
            acks: vec![SegmentAck {
                topic_id: TopicId(1),
                partition_id: PartitionId(0),
                schema_id: SchemaId(100),
                start_offset: Offset(1000),
                end_offset: Offset(1002),
            }],
        };

        let mut buf = [0u8; 128];
        let encoded_len = encode_response(&resp, &mut buf);

        let (decoded, decoded_len) = decode_response(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded.seq.0, 42);
        assert_eq!(decoded.acks.len(), 1);
        assert_eq!(decoded.acks[0].start_offset.0, 1000);
        assert_eq!(decoded.acks[0].end_offset.0, 1002);
    }

    #[test]
    fn test_empty_request() {
        let req = ProduceRequest {
            producer_id: ProducerId(Uuid::nil()),
            seq: SeqNum(0),
            segments: vec![],
        };

        let mut buf = [0u8; 32];
        let encoded_len = encode_request(&req, &mut buf);

        let (decoded, decoded_len) = decode_request(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert!(decoded.segments.is_empty());
    }

    #[test]
    fn test_decode_truncated_uuid() {
        let buf = [0u8; 8]; // Only 8 bytes, need 16
        let result = decode_request(&buf);
        assert!(matches!(result, Err(DecodeError::UnexpectedEof { .. })));
    }
}
