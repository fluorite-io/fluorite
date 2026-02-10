//! Consumer protocol messages: FetchRequest, FetchResponse, and consumer group messages.

use turbine_common::ids::{Generation, Offset, PartitionId, SchemaId, TopicId};
use turbine_common::types::Record;

use crate::{record, varint, DecodeError};

/// A fetch request from a consumer to an agent.
#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub group_id: String,
    pub consumer_id: String,
    pub generation: Generation,
    pub fetches: Vec<PartitionFetch>,
}

/// A single partition fetch within a FetchRequest.
#[derive(Debug, Clone)]
pub struct PartitionFetch {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub offset: Offset,
    pub max_bytes: u32,
}

/// A fetch response from an agent to a consumer.
#[derive(Debug, Clone)]
pub struct FetchResponse {
    pub results: Vec<PartitionResult>,
}

/// Result for a single partition in a FetchResponse.
#[derive(Debug, Clone)]
pub struct PartitionResult {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub schema_id: SchemaId,
    pub high_watermark: Offset,
    pub records: Vec<Record>,
}

/// Consumer group join request.
#[derive(Debug, Clone)]
pub struct JoinGroupRequest {
    pub group_id: String,
    pub consumer_id: String,
    pub topic_ids: Vec<TopicId>,
}

/// Consumer group join response.
#[derive(Debug, Clone)]
pub struct JoinGroupResponse {
    pub generation: Generation,
    pub assignments: Vec<PartitionAssignment>,
}

/// A partition assignment for a consumer.
#[derive(Debug, Clone)]
pub struct PartitionAssignment {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub committed_offset: Offset,
}

/// Heartbeat request to maintain consumer membership.
#[derive(Debug, Clone)]
pub struct HeartbeatRequest {
    pub group_id: String,
    pub consumer_id: String,
    pub generation: Generation,
}

/// Heartbeat response.
#[derive(Debug, Clone)]
pub struct HeartbeatResponse {
    pub rebalance_needed: bool,
}

/// Commit offset request.
#[derive(Debug, Clone)]
pub struct CommitRequest {
    pub group_id: String,
    pub consumer_id: String,
    pub generation: Generation,
    pub commits: Vec<PartitionCommit>,
}

/// A single partition commit.
#[derive(Debug, Clone)]
pub struct PartitionCommit {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub offset: Offset,
}

/// Commit response.
#[derive(Debug, Clone)]
pub struct CommitResponse {
    pub success: bool,
}

// ============ Encoding Functions ============

/// Encode a FetchRequest.
pub fn encode_fetch_request(req: &FetchRequest, buf: &mut [u8]) -> usize {
    let mut offset = 0;
    offset += encode_string(&req.group_id, &mut buf[offset..]);
    offset += encode_string(&req.consumer_id, &mut buf[offset..]);
    offset += varint::encode_u64(req.generation.0, &mut buf[offset..]);
    offset += varint::encode_i64(req.fetches.len() as i64, &mut buf[offset..]);

    for fetch in &req.fetches {
        offset += varint::encode_i64(fetch.topic_id.0 as i64, &mut buf[offset..]);
        offset += varint::encode_i64(fetch.partition_id.0 as i64, &mut buf[offset..]);
        offset += varint::encode_u64(fetch.offset.0, &mut buf[offset..]);
        offset += varint::encode_i64(fetch.max_bytes as i64, &mut buf[offset..]);
    }

    offset
}

/// Decode a FetchRequest.
pub fn decode_fetch_request(buf: &[u8]) -> Result<(FetchRequest, usize), DecodeError> {
    let mut offset = 0;

    let (group_id, len) = decode_string(&buf[offset..])?;
    offset += len;

    let (consumer_id, len) = decode_string(&buf[offset..])?;
    offset += len;

    let (generation, len) = varint::decode_u64(&buf[offset..])?;
    offset += len;

    let (count, len) = varint::decode_i64(&buf[offset..])?;
    offset += len;

    let mut fetches = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (topic_id, len) = varint::decode_i64(&buf[offset..])?;
        offset += len;

        let (partition_id, len) = varint::decode_i64(&buf[offset..])?;
        offset += len;

        let (fetch_offset, len) = varint::decode_u64(&buf[offset..])?;
        offset += len;

        let (max_bytes, len) = varint::decode_i64(&buf[offset..])?;
        offset += len;

        fetches.push(PartitionFetch {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(partition_id as u32),
            offset: Offset(fetch_offset),
            max_bytes: max_bytes as u32,
        });
    }

    Ok((
        FetchRequest {
            group_id,
            consumer_id,
            generation: Generation(generation),
            fetches,
        },
        offset,
    ))
}

/// Encode a FetchResponse.
pub fn encode_fetch_response(resp: &FetchResponse, buf: &mut [u8]) -> usize {
    let mut offset = 0;
    offset += varint::encode_i64(resp.results.len() as i64, &mut buf[offset..]);

    for result in &resp.results {
        offset += varint::encode_i64(result.topic_id.0 as i64, &mut buf[offset..]);
        offset += varint::encode_i64(result.partition_id.0 as i64, &mut buf[offset..]);
        offset += varint::encode_i64(result.schema_id.0 as i64, &mut buf[offset..]);
        offset += varint::encode_u64(result.high_watermark.0, &mut buf[offset..]);
        offset += varint::encode_i64(result.records.len() as i64, &mut buf[offset..]);

        for rec in &result.records {
            offset += record::encode(rec, &mut buf[offset..]);
        }
    }

    offset
}

/// Decode a FetchResponse.
pub fn decode_fetch_response(buf: &[u8]) -> Result<(FetchResponse, usize), DecodeError> {
    let mut offset = 0;

    let (count, len) = varint::decode_i64(&buf[offset..])?;
    offset += len;

    let mut results = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (topic_id, len) = varint::decode_i64(&buf[offset..])?;
        offset += len;

        let (partition_id, len) = varint::decode_i64(&buf[offset..])?;
        offset += len;

        let (schema_id, len) = varint::decode_i64(&buf[offset..])?;
        offset += len;

        let (high_watermark, len) = varint::decode_u64(&buf[offset..])?;
        offset += len;

        let (rec_count, len) = varint::decode_i64(&buf[offset..])?;
        offset += len;

        let mut records = Vec::with_capacity(rec_count as usize);
        for _ in 0..rec_count {
            let (rec, len) = record::decode(&buf[offset..])?;
            offset += len;
            records.push(rec);
        }

        results.push(PartitionResult {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(partition_id as u32),
            schema_id: SchemaId(schema_id as u32),
            high_watermark: Offset(high_watermark),
            records,
        });
    }

    Ok((FetchResponse { results }, offset))
}

/// Encode a JoinGroupRequest.
pub fn encode_join_request(req: &JoinGroupRequest, buf: &mut [u8]) -> usize {
    let mut offset = 0;
    offset += encode_string(&req.group_id, &mut buf[offset..]);
    offset += encode_string(&req.consumer_id, &mut buf[offset..]);
    offset += varint::encode_i64(req.topic_ids.len() as i64, &mut buf[offset..]);
    for topic_id in &req.topic_ids {
        offset += varint::encode_i64(topic_id.0 as i64, &mut buf[offset..]);
    }
    offset
}

/// Decode a JoinGroupRequest.
pub fn decode_join_request(buf: &[u8]) -> Result<(JoinGroupRequest, usize), DecodeError> {
    let mut offset = 0;

    let (group_id, len) = decode_string(&buf[offset..])?;
    offset += len;

    let (consumer_id, len) = decode_string(&buf[offset..])?;
    offset += len;

    let (count, len) = varint::decode_i64(&buf[offset..])?;
    offset += len;

    let mut topic_ids = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (topic_id, len) = varint::decode_i64(&buf[offset..])?;
        offset += len;
        topic_ids.push(TopicId(topic_id as u32));
    }

    Ok((
        JoinGroupRequest {
            group_id,
            consumer_id,
            topic_ids,
        },
        offset,
    ))
}

/// Encode a JoinGroupResponse.
pub fn encode_join_response(resp: &JoinGroupResponse, buf: &mut [u8]) -> usize {
    let mut offset = 0;
    offset += varint::encode_u64(resp.generation.0, &mut buf[offset..]);
    offset += varint::encode_i64(resp.assignments.len() as i64, &mut buf[offset..]);

    for assignment in &resp.assignments {
        offset += varint::encode_i64(assignment.topic_id.0 as i64, &mut buf[offset..]);
        offset += varint::encode_i64(assignment.partition_id.0 as i64, &mut buf[offset..]);
        offset += varint::encode_u64(assignment.committed_offset.0, &mut buf[offset..]);
    }

    offset
}

/// Decode a JoinGroupResponse.
pub fn decode_join_response(buf: &[u8]) -> Result<(JoinGroupResponse, usize), DecodeError> {
    let mut offset = 0;

    let (generation, len) = varint::decode_u64(&buf[offset..])?;
    offset += len;

    let (count, len) = varint::decode_i64(&buf[offset..])?;
    offset += len;

    let mut assignments = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (topic_id, len) = varint::decode_i64(&buf[offset..])?;
        offset += len;

        let (partition_id, len) = varint::decode_i64(&buf[offset..])?;
        offset += len;

        let (committed_offset, len) = varint::decode_u64(&buf[offset..])?;
        offset += len;

        assignments.push(PartitionAssignment {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(partition_id as u32),
            committed_offset: Offset(committed_offset),
        });
    }

    Ok((
        JoinGroupResponse {
            generation: Generation(generation),
            assignments,
        },
        offset,
    ))
}

/// Encode a HeartbeatRequest.
pub fn encode_heartbeat_request(req: &HeartbeatRequest, buf: &mut [u8]) -> usize {
    let mut offset = 0;
    offset += encode_string(&req.group_id, &mut buf[offset..]);
    offset += encode_string(&req.consumer_id, &mut buf[offset..]);
    offset += varint::encode_u64(req.generation.0, &mut buf[offset..]);
    offset
}

/// Decode a HeartbeatRequest.
pub fn decode_heartbeat_request(buf: &[u8]) -> Result<(HeartbeatRequest, usize), DecodeError> {
    let mut offset = 0;

    let (group_id, len) = decode_string(&buf[offset..])?;
    offset += len;

    let (consumer_id, len) = decode_string(&buf[offset..])?;
    offset += len;

    let (generation, len) = varint::decode_u64(&buf[offset..])?;
    offset += len;

    Ok((
        HeartbeatRequest {
            group_id,
            consumer_id,
            generation: Generation(generation),
        },
        offset,
    ))
}

/// Encode a HeartbeatResponse.
pub fn encode_heartbeat_response(resp: &HeartbeatResponse, buf: &mut [u8]) -> usize {
    buf[0] = if resp.rebalance_needed { 1 } else { 0 };
    1
}

/// Decode a HeartbeatResponse.
pub fn decode_heartbeat_response(buf: &[u8]) -> Result<(HeartbeatResponse, usize), DecodeError> {
    if buf.is_empty() {
        return Err(DecodeError::UnexpectedEof { needed: 1 });
    }
    Ok((
        HeartbeatResponse {
            rebalance_needed: buf[0] != 0,
        },
        1,
    ))
}

/// Encode a CommitRequest.
pub fn encode_commit_request(req: &CommitRequest, buf: &mut [u8]) -> usize {
    let mut offset = 0;
    offset += encode_string(&req.group_id, &mut buf[offset..]);
    offset += encode_string(&req.consumer_id, &mut buf[offset..]);
    offset += varint::encode_u64(req.generation.0, &mut buf[offset..]);
    offset += varint::encode_i64(req.commits.len() as i64, &mut buf[offset..]);

    for commit in &req.commits {
        offset += varint::encode_i64(commit.topic_id.0 as i64, &mut buf[offset..]);
        offset += varint::encode_i64(commit.partition_id.0 as i64, &mut buf[offset..]);
        offset += varint::encode_u64(commit.offset.0, &mut buf[offset..]);
    }

    offset
}

/// Decode a CommitRequest.
pub fn decode_commit_request(buf: &[u8]) -> Result<(CommitRequest, usize), DecodeError> {
    let mut offset = 0;

    let (group_id, len) = decode_string(&buf[offset..])?;
    offset += len;

    let (consumer_id, len) = decode_string(&buf[offset..])?;
    offset += len;

    let (generation, len) = varint::decode_u64(&buf[offset..])?;
    offset += len;

    let (count, len) = varint::decode_i64(&buf[offset..])?;
    offset += len;

    let mut commits = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (topic_id, len) = varint::decode_i64(&buf[offset..])?;
        offset += len;

        let (partition_id, len) = varint::decode_i64(&buf[offset..])?;
        offset += len;

        let (commit_offset, len) = varint::decode_u64(&buf[offset..])?;
        offset += len;

        commits.push(PartitionCommit {
            topic_id: TopicId(topic_id as u32),
            partition_id: PartitionId(partition_id as u32),
            offset: Offset(commit_offset),
        });
    }

    Ok((
        CommitRequest {
            group_id,
            consumer_id,
            generation: Generation(generation),
            commits,
        },
        offset,
    ))
}

/// Encode a CommitResponse.
pub fn encode_commit_response(resp: &CommitResponse, buf: &mut [u8]) -> usize {
    buf[0] = if resp.success { 1 } else { 0 };
    1
}

/// Decode a CommitResponse.
pub fn decode_commit_response(buf: &[u8]) -> Result<(CommitResponse, usize), DecodeError> {
    if buf.is_empty() {
        return Err(DecodeError::UnexpectedEof { needed: 1 });
    }
    Ok((CommitResponse { success: buf[0] != 0 }, 1))
}

// ============ String Helpers ============

fn encode_string(s: &str, buf: &mut [u8]) -> usize {
    let bytes = s.as_bytes();
    let offset = varint::encode_i64(bytes.len() as i64, buf);
    buf[offset..offset + bytes.len()].copy_from_slice(bytes);
    offset + bytes.len()
}

fn decode_string(buf: &[u8]) -> Result<(String, usize), DecodeError> {
    let (len, varint_len) = varint::decode_i64(buf)?;
    let str_len = len as usize;
    let total_len = varint_len + str_len;

    if buf.len() < total_len {
        return Err(DecodeError::UnexpectedEof { needed: str_len });
    }

    let s = std::str::from_utf8(&buf[varint_len..total_len])
        .map_err(|_| DecodeError::InvalidUtf8)?
        .to_string();

    Ok((s, total_len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use turbine_common::types::Record;

    #[test]
    fn test_fetch_request_roundtrip() {
        let req = FetchRequest {
            group_id: "my-group".to_string(),
            consumer_id: "consumer-1".to_string(),
            generation: Generation(5),
            fetches: vec![
                PartitionFetch {
                    topic_id: TopicId(1),
                    partition_id: PartitionId(0),
                    offset: Offset(100),
                    max_bytes: 1024,
                },
                PartitionFetch {
                    topic_id: TopicId(1),
                    partition_id: PartitionId(1),
                    offset: Offset(200),
                    max_bytes: 2048,
                },
            ],
        };

        let mut buf = [0u8; 256];
        let encoded_len = encode_fetch_request(&req, &mut buf);

        let (decoded, decoded_len) = decode_fetch_request(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded.group_id, "my-group");
        assert_eq!(decoded.consumer_id, "consumer-1");
        assert_eq!(decoded.generation.0, 5);
        assert_eq!(decoded.fetches.len(), 2);
        assert_eq!(decoded.fetches[0].offset.0, 100);
    }

    #[test]
    fn test_fetch_response_roundtrip() {
        let resp = FetchResponse {
            results: vec![PartitionResult {
                topic_id: TopicId(1),
                partition_id: PartitionId(0),
                schema_id: SchemaId(100),
                high_watermark: Offset(500),
                records: vec![
                    Record {
                        key: Some(Bytes::from_static(b"k1")),
                        value: Bytes::from_static(b"v1"),
                    },
                    Record {
                        key: None,
                        value: Bytes::from_static(b"v2"),
                    },
                ],
            }],
        };

        let mut buf = [0u8; 256];
        let encoded_len = encode_fetch_response(&resp, &mut buf);

        let (decoded, decoded_len) = decode_fetch_response(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded.results.len(), 1);
        assert_eq!(decoded.results[0].high_watermark.0, 500);
        assert_eq!(decoded.results[0].records.len(), 2);
    }

    #[test]
    fn test_join_group_roundtrip() {
        let req = JoinGroupRequest {
            group_id: "test-group".to_string(),
            consumer_id: "consumer-abc".to_string(),
            topic_ids: vec![TopicId(1), TopicId(2), TopicId(3)],
        };

        let mut buf = [0u8; 128];
        let encoded_len = encode_join_request(&req, &mut buf);

        let (decoded, decoded_len) = decode_join_request(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded.group_id, "test-group");
        assert_eq!(decoded.topic_ids.len(), 3);

        let resp = JoinGroupResponse {
            generation: Generation(1),
            assignments: vec![
                PartitionAssignment {
                    topic_id: TopicId(1),
                    partition_id: PartitionId(0),
                    committed_offset: Offset(50),
                },
                PartitionAssignment {
                    topic_id: TopicId(1),
                    partition_id: PartitionId(1),
                    committed_offset: Offset(75),
                },
            ],
        };

        let encoded_len = encode_join_response(&resp, &mut buf);
        let (decoded_resp, decoded_len) = decode_join_response(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded_resp.generation.0, 1);
        assert_eq!(decoded_resp.assignments.len(), 2);
    }

    #[test]
    fn test_heartbeat_roundtrip() {
        let req = HeartbeatRequest {
            group_id: "hb-group".to_string(),
            consumer_id: "hb-consumer".to_string(),
            generation: Generation(10),
        };

        let mut buf = [0u8; 64];
        let encoded_len = encode_heartbeat_request(&req, &mut buf);

        let (decoded, decoded_len) = decode_heartbeat_request(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded.group_id, "hb-group");
        assert_eq!(decoded.generation.0, 10);

        // Response with rebalance
        let resp = HeartbeatResponse {
            rebalance_needed: true,
        };
        let encoded_len = encode_heartbeat_response(&resp, &mut buf);
        let (decoded_resp, _) = decode_heartbeat_response(&buf[..encoded_len]).unwrap();
        assert!(decoded_resp.rebalance_needed);

        // Response without rebalance
        let resp = HeartbeatResponse {
            rebalance_needed: false,
        };
        let encoded_len = encode_heartbeat_response(&resp, &mut buf);
        let (decoded_resp, _) = decode_heartbeat_response(&buf[..encoded_len]).unwrap();
        assert!(!decoded_resp.rebalance_needed);
    }

    #[test]
    fn test_commit_roundtrip() {
        let req = CommitRequest {
            group_id: "commit-group".to_string(),
            consumer_id: "commit-consumer".to_string(),
            generation: Generation(3),
            commits: vec![
                PartitionCommit {
                    topic_id: TopicId(1),
                    partition_id: PartitionId(0),
                    offset: Offset(150),
                },
                PartitionCommit {
                    topic_id: TopicId(1),
                    partition_id: PartitionId(1),
                    offset: Offset(200),
                },
            ],
        };

        let mut buf = [0u8; 128];
        let encoded_len = encode_commit_request(&req, &mut buf);

        let (decoded, decoded_len) = decode_commit_request(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded.group_id, "commit-group");
        assert_eq!(decoded.commits.len(), 2);
        assert_eq!(decoded.commits[0].offset.0, 150);

        let resp = CommitResponse { success: true };
        let encoded_len = encode_commit_response(&resp, &mut buf);
        let (decoded_resp, _) = decode_commit_response(&buf[..encoded_len]).unwrap();
        assert!(decoded_resp.success);
    }

    #[test]
    fn test_empty_string() {
        let mut buf = [0u8; 16];
        let len = encode_string("", &mut buf);
        let (decoded, decoded_len) = decode_string(&buf[..len]).unwrap();
        assert_eq!(decoded, "");
        assert_eq!(decoded_len, len);
    }
}
