//! Reader protocol messages and reader group messages using protobuf.

use flourine_common::ids::{Generation, Offset, PartitionId, SchemaId, TopicId};
use flourine_common::types::Record;

use crate::proto_conv::{
    decode_proto, encode_proto, encode_proto_checked, from_proto_record, to_proto_record,
};
use crate::{DecodeError, EncodeError, proto};

/// A read request from a reader to an broker.
#[derive(Debug, Clone)]
pub struct ReadRequest {
    pub group_id: String,
    pub reader_id: String,
    pub generation: Generation,
    pub reads: Vec<PartitionRead>,
}

/// A single partition read within a ReadRequest.
#[derive(Debug, Clone)]
pub struct PartitionRead {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub offset: Offset,
    pub max_bytes: u32,
}

/// A read response from an broker to a reader.
#[derive(Debug, Clone)]
pub struct ReadResponse {
    pub success: bool,
    pub error_code: u16,
    pub error_message: String,
    pub results: Vec<PartitionResult>,
}

/// Result for a single partition in a ReadResponse.
#[derive(Debug, Clone)]
pub struct PartitionResult {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub schema_id: SchemaId,
    pub high_watermark: Offset,
    pub records: Vec<Record>,
}

/// Reader group join request.
#[derive(Debug, Clone)]
pub struct JoinGroupRequest {
    pub group_id: String,
    pub reader_id: String,
    pub topic_ids: Vec<TopicId>,
}

/// Reader group join response.
#[derive(Debug, Clone)]
pub struct JoinGroupResponse {
    pub success: bool,
    pub error_code: u16,
    pub error_message: String,
    pub generation: Generation,
    pub assignments: Vec<PartitionAssignment>,
}

/// A partition assignment for a reader.
#[derive(Debug, Clone)]
pub struct PartitionAssignment {
    pub topic_id: TopicId,
    pub partition_id: PartitionId,
    pub committed_offset: Offset,
}

/// Heartbeat request to maintain reader membership.
#[derive(Debug, Clone)]
pub struct HeartbeatRequest {
    pub group_id: String,
    pub topic_id: TopicId,
    pub reader_id: String,
    pub generation: Generation,
}

/// Legacy heartbeat response.
#[derive(Debug, Clone)]
pub struct HeartbeatResponse {
    pub rebalance_needed: bool,
}

/// Commit offset request.
#[derive(Debug, Clone)]
pub struct CommitRequest {
    pub group_id: String,
    pub reader_id: String,
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
    pub error_code: u16,
    pub error_message: String,
}

/// Leave group request.
#[derive(Debug, Clone)]
pub struct LeaveGroupRequest {
    pub group_id: String,
    pub topic_id: TopicId,
    pub reader_id: String,
}

/// Leave group response.
#[derive(Debug, Clone)]
pub struct LeaveGroupResponse {
    pub success: bool,
    pub error_code: u16,
    pub error_message: String,
}

/// Rejoin request (after rebalance notification).
#[derive(Debug, Clone)]
pub struct RejoinRequest {
    pub group_id: String,
    pub topic_id: TopicId,
    pub reader_id: String,
    pub generation: Generation,
}

/// Rejoin response status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejoinStatus {
    Ok,
    RebalanceNeeded,
}

/// Rejoin response.
#[derive(Debug, Clone)]
pub struct RejoinResponse {
    pub success: bool,
    pub error_code: u16,
    pub error_message: String,
    pub generation: Generation,
    pub status: RejoinStatus,
    pub assignments: Vec<PartitionAssignment>,
}

/// Heartbeat response status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatStatus {
    Ok,
    RebalanceNeeded,
    UnknownMember,
}

/// Extended heartbeat response with status enum.
#[derive(Debug, Clone)]
pub struct HeartbeatResponseExt {
    pub success: bool,
    pub error_code: u16,
    pub error_message: String,
    pub generation: Generation,
    pub status: HeartbeatStatus,
}

pub fn encode_read_request(req: &ReadRequest, buf: &mut [u8]) -> usize {
    let msg = proto::ReadRequest {
        group_id: req.group_id.clone(),
        reader_id: req.reader_id.clone(),
        generation: req.generation.0,
        reads: req.reads.iter().map(to_proto_partition_read).collect(),
    };
    encode_proto(&msg, buf)
}

pub fn decode_read_request(buf: &[u8]) -> Result<(ReadRequest, usize), DecodeError> {
    let msg = decode_proto::<proto::ReadRequest>(buf, "invalid protobuf read request")?;
    Ok((
        ReadRequest {
            group_id: msg.group_id,
            reader_id: msg.reader_id,
            generation: Generation(msg.generation),
            reads: msg
                .reads
                .into_iter()
                .map(from_proto_partition_read)
                .collect(),
        },
        buf.len(),
    ))
}

pub fn encode_read_response(resp: &ReadResponse, buf: &mut [u8]) -> usize {
    encode_read_response_checked(resp, buf).expect("buffer too small for read response")
}

pub fn encode_read_response_checked(
    resp: &ReadResponse,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let msg = proto::ReadResponse {
        success: resp.success,
        error_code: resp.error_code as u32,
        error_message: resp.error_message.clone(),
        results: resp.results.iter().map(to_proto_partition_result).collect(),
    };
    encode_proto_checked(&msg, buf)
}

pub fn decode_read_response(buf: &[u8]) -> Result<(ReadResponse, usize), DecodeError> {
    let msg = decode_proto::<proto::ReadResponse>(buf, "invalid protobuf read response")?;
    Ok((
        ReadResponse {
            success: msg.success,
            error_code: msg.error_code as u16,
            error_message: msg.error_message,
            results: msg
                .results
                .into_iter()
                .map(from_proto_partition_result)
                .collect(),
        },
        buf.len(),
    ))
}

pub fn encode_join_request(req: &JoinGroupRequest, buf: &mut [u8]) -> usize {
    let msg = proto::JoinGroupRequest {
        group_id: req.group_id.clone(),
        reader_id: req.reader_id.clone(),
        topic_ids: req.topic_ids.iter().map(|id| id.0).collect(),
    };
    encode_proto(&msg, buf)
}

pub fn decode_join_request(buf: &[u8]) -> Result<(JoinGroupRequest, usize), DecodeError> {
    let msg = decode_proto::<proto::JoinGroupRequest>(buf, "invalid protobuf join request")?;
    Ok((
        JoinGroupRequest {
            group_id: msg.group_id,
            reader_id: msg.reader_id,
            topic_ids: msg.topic_ids.into_iter().map(TopicId).collect(),
        },
        buf.len(),
    ))
}

pub fn encode_join_response(resp: &JoinGroupResponse, buf: &mut [u8]) -> usize {
    encode_join_response_checked(resp, buf).expect("buffer too small for join response")
}

pub fn encode_join_response_checked(
    resp: &JoinGroupResponse,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let msg = proto::JoinGroupResponse {
        success: resp.success,
        error_code: resp.error_code as u32,
        error_message: resp.error_message.clone(),
        generation: resp.generation.0,
        assignments: resp.assignments.iter().map(to_proto_assignment).collect(),
    };
    encode_proto_checked(&msg, buf)
}

pub fn decode_join_response(buf: &[u8]) -> Result<(JoinGroupResponse, usize), DecodeError> {
    let msg = decode_proto::<proto::JoinGroupResponse>(buf, "invalid protobuf join response")?;
    Ok((
        JoinGroupResponse {
            success: msg.success,
            error_code: msg.error_code as u16,
            error_message: msg.error_message,
            generation: Generation(msg.generation),
            assignments: msg
                .assignments
                .into_iter()
                .map(from_proto_assignment)
                .collect(),
        },
        buf.len(),
    ))
}

pub fn encode_heartbeat_request(req: &HeartbeatRequest, buf: &mut [u8]) -> usize {
    let msg = proto::HeartbeatRequest {
        group_id: req.group_id.clone(),
        topic_id: req.topic_id.0,
        reader_id: req.reader_id.clone(),
        generation: req.generation.0,
    };
    encode_proto(&msg, buf)
}

pub fn decode_heartbeat_request(buf: &[u8]) -> Result<(HeartbeatRequest, usize), DecodeError> {
    let msg = decode_proto::<proto::HeartbeatRequest>(buf, "invalid protobuf heartbeat request")?;
    Ok((
        HeartbeatRequest {
            group_id: msg.group_id,
            topic_id: TopicId(msg.topic_id),
            reader_id: msg.reader_id,
            generation: Generation(msg.generation),
        },
        buf.len(),
    ))
}

pub fn encode_heartbeat_response(resp: &HeartbeatResponse, buf: &mut [u8]) -> usize {
    let status = if resp.rebalance_needed {
        proto::HeartbeatStatus::RebalanceNeeded
    } else {
        proto::HeartbeatStatus::Ok
    };
    let msg = proto::HeartbeatResponse {
        success: true,
        error_code: 0,
        error_message: String::new(),
        generation: 0,
        status: status as i32,
    };
    encode_proto(&msg, buf)
}

pub fn decode_heartbeat_response(buf: &[u8]) -> Result<(HeartbeatResponse, usize), DecodeError> {
    let msg = decode_proto::<proto::HeartbeatResponse>(buf, "invalid protobuf heartbeat response")?;
    Ok((
        HeartbeatResponse {
            rebalance_needed: proto_to_hb_status(msg.status) == HeartbeatStatus::RebalanceNeeded,
        },
        buf.len(),
    ))
}

pub fn encode_commit_request(req: &CommitRequest, buf: &mut [u8]) -> usize {
    let msg = proto::CommitRequest {
        group_id: req.group_id.clone(),
        reader_id: req.reader_id.clone(),
        generation: req.generation.0,
        commits: req.commits.iter().map(to_proto_commit).collect(),
    };
    encode_proto(&msg, buf)
}

pub fn decode_commit_request(buf: &[u8]) -> Result<(CommitRequest, usize), DecodeError> {
    let msg = decode_proto::<proto::CommitRequest>(buf, "invalid protobuf commit request")?;
    Ok((
        CommitRequest {
            group_id: msg.group_id,
            reader_id: msg.reader_id,
            generation: Generation(msg.generation),
            commits: msg.commits.into_iter().map(from_proto_commit).collect(),
        },
        buf.len(),
    ))
}

pub fn encode_commit_response(resp: &CommitResponse, buf: &mut [u8]) -> usize {
    encode_commit_response_checked(resp, buf).expect("buffer too small for commit response")
}

pub fn encode_commit_response_checked(
    resp: &CommitResponse,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let msg = proto::CommitResponse {
        success: resp.success,
        error_code: resp.error_code as u32,
        error_message: resp.error_message.clone(),
    };
    encode_proto_checked(&msg, buf)
}

pub fn decode_commit_response(buf: &[u8]) -> Result<(CommitResponse, usize), DecodeError> {
    let msg = decode_proto::<proto::CommitResponse>(buf, "invalid protobuf commit response")?;
    Ok((
        CommitResponse {
            success: msg.success,
            error_code: msg.error_code as u16,
            error_message: msg.error_message,
        },
        buf.len(),
    ))
}

pub fn encode_leave_request(req: &LeaveGroupRequest, buf: &mut [u8]) -> usize {
    let msg = proto::LeaveGroupRequest {
        group_id: req.group_id.clone(),
        topic_id: req.topic_id.0,
        reader_id: req.reader_id.clone(),
    };
    encode_proto(&msg, buf)
}

pub fn decode_leave_request(buf: &[u8]) -> Result<(LeaveGroupRequest, usize), DecodeError> {
    let msg = decode_proto::<proto::LeaveGroupRequest>(buf, "invalid protobuf leave request")?;
    Ok((
        LeaveGroupRequest {
            group_id: msg.group_id,
            topic_id: TopicId(msg.topic_id),
            reader_id: msg.reader_id,
        },
        buf.len(),
    ))
}

pub fn encode_leave_response(resp: &LeaveGroupResponse, buf: &mut [u8]) -> usize {
    encode_leave_response_checked(resp, buf).expect("buffer too small for leave response")
}

pub fn encode_leave_response_checked(
    resp: &LeaveGroupResponse,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let msg = proto::LeaveGroupResponse {
        success: resp.success,
        error_code: resp.error_code as u32,
        error_message: resp.error_message.clone(),
    };
    encode_proto_checked(&msg, buf)
}

pub fn decode_leave_response(buf: &[u8]) -> Result<(LeaveGroupResponse, usize), DecodeError> {
    let msg = decode_proto::<proto::LeaveGroupResponse>(buf, "invalid protobuf leave response")?;
    Ok((
        LeaveGroupResponse {
            success: msg.success,
            error_code: msg.error_code as u16,
            error_message: msg.error_message,
        },
        buf.len(),
    ))
}

pub fn encode_rejoin_request(req: &RejoinRequest, buf: &mut [u8]) -> usize {
    let msg = proto::RejoinRequest {
        group_id: req.group_id.clone(),
        topic_id: req.topic_id.0,
        reader_id: req.reader_id.clone(),
        generation: req.generation.0,
    };
    encode_proto(&msg, buf)
}

pub fn decode_rejoin_request(buf: &[u8]) -> Result<(RejoinRequest, usize), DecodeError> {
    let msg = decode_proto::<proto::RejoinRequest>(buf, "invalid protobuf rejoin request")?;
    Ok((
        RejoinRequest {
            group_id: msg.group_id,
            topic_id: TopicId(msg.topic_id),
            reader_id: msg.reader_id,
            generation: Generation(msg.generation),
        },
        buf.len(),
    ))
}

pub fn encode_rejoin_response(resp: &RejoinResponse, buf: &mut [u8]) -> usize {
    encode_rejoin_response_checked(resp, buf).expect("buffer too small for rejoin response")
}

pub fn encode_rejoin_response_checked(
    resp: &RejoinResponse,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let msg = proto::RejoinResponse {
        success: resp.success,
        error_code: resp.error_code as u32,
        error_message: resp.error_message.clone(),
        generation: resp.generation.0,
        status: rejoin_status_to_proto(resp.status) as i32,
        assignments: resp.assignments.iter().map(to_proto_assignment).collect(),
    };
    encode_proto_checked(&msg, buf)
}

pub fn decode_rejoin_response(buf: &[u8]) -> Result<(RejoinResponse, usize), DecodeError> {
    let msg = decode_proto::<proto::RejoinResponse>(buf, "invalid protobuf rejoin response")?;
    Ok((
        RejoinResponse {
            success: msg.success,
            error_code: msg.error_code as u16,
            error_message: msg.error_message,
            generation: Generation(msg.generation),
            status: proto_to_rejoin_status(msg.status),
            assignments: msg
                .assignments
                .into_iter()
                .map(from_proto_assignment)
                .collect(),
        },
        buf.len(),
    ))
}

pub fn encode_heartbeat_response_ext(resp: &HeartbeatResponseExt, buf: &mut [u8]) -> usize {
    encode_heartbeat_response_ext_checked(resp, buf)
        .expect("buffer too small for heartbeat response")
}

pub fn encode_heartbeat_response_ext_checked(
    resp: &HeartbeatResponseExt,
    buf: &mut [u8],
) -> Result<usize, EncodeError> {
    let msg = proto::HeartbeatResponse {
        success: resp.success,
        error_code: resp.error_code as u32,
        error_message: resp.error_message.clone(),
        generation: resp.generation.0,
        status: hb_status_to_proto(resp.status) as i32,
    };
    encode_proto_checked(&msg, buf)
}

pub fn decode_heartbeat_response_ext(
    buf: &[u8],
) -> Result<(HeartbeatResponseExt, usize), DecodeError> {
    let msg = decode_proto::<proto::HeartbeatResponse>(buf, "invalid protobuf heartbeat response")?;
    Ok((
        HeartbeatResponseExt {
            success: msg.success,
            error_code: msg.error_code as u16,
            error_message: msg.error_message,
            generation: Generation(msg.generation),
            status: proto_to_hb_status(msg.status),
        },
        buf.len(),
    ))
}


fn to_proto_partition_read(read: &PartitionRead) -> proto::PartitionRead {
    proto::PartitionRead {
        topic_id: read.topic_id.0,
        partition_id: read.partition_id.0,
        offset: read.offset.0,
        max_bytes: read.max_bytes,
    }
}

fn from_proto_partition_read(read: proto::PartitionRead) -> PartitionRead {
    PartitionRead {
        topic_id: TopicId(read.topic_id),
        partition_id: PartitionId(read.partition_id),
        offset: Offset(read.offset),
        max_bytes: read.max_bytes,
    }
}

fn to_proto_partition_result(result: &PartitionResult) -> proto::PartitionResult {
    proto::PartitionResult {
        topic_id: result.topic_id.0,
        partition_id: result.partition_id.0,
        schema_id: result.schema_id.0,
        high_watermark: result.high_watermark.0,
        records: result.records.iter().map(to_proto_record).collect(),
    }
}

fn from_proto_partition_result(result: proto::PartitionResult) -> PartitionResult {
    PartitionResult {
        topic_id: TopicId(result.topic_id),
        partition_id: PartitionId(result.partition_id),
        schema_id: SchemaId(result.schema_id),
        high_watermark: Offset(result.high_watermark),
        records: result.records.into_iter().map(from_proto_record).collect(),
    }
}

fn to_proto_assignment(assignment: &PartitionAssignment) -> proto::PartitionAssignment {
    proto::PartitionAssignment {
        topic_id: assignment.topic_id.0,
        partition_id: assignment.partition_id.0,
        committed_offset: assignment.committed_offset.0,
    }
}

fn from_proto_assignment(assignment: proto::PartitionAssignment) -> PartitionAssignment {
    PartitionAssignment {
        topic_id: TopicId(assignment.topic_id),
        partition_id: PartitionId(assignment.partition_id),
        committed_offset: Offset(assignment.committed_offset),
    }
}

fn to_proto_commit(commit: &PartitionCommit) -> proto::PartitionCommit {
    proto::PartitionCommit {
        topic_id: commit.topic_id.0,
        partition_id: commit.partition_id.0,
        offset: commit.offset.0,
    }
}

fn from_proto_commit(commit: proto::PartitionCommit) -> PartitionCommit {
    PartitionCommit {
        topic_id: TopicId(commit.topic_id),
        partition_id: PartitionId(commit.partition_id),
        offset: Offset(commit.offset),
    }
}

fn hb_status_to_proto(status: HeartbeatStatus) -> proto::HeartbeatStatus {
    match status {
        HeartbeatStatus::Ok => proto::HeartbeatStatus::Ok,
        HeartbeatStatus::RebalanceNeeded => proto::HeartbeatStatus::RebalanceNeeded,
        HeartbeatStatus::UnknownMember => proto::HeartbeatStatus::UnknownMember,
    }
}

fn proto_to_hb_status(status: i32) -> HeartbeatStatus {
    match proto::HeartbeatStatus::try_from(status).unwrap_or(proto::HeartbeatStatus::UnknownMember)
    {
        proto::HeartbeatStatus::Ok => HeartbeatStatus::Ok,
        proto::HeartbeatStatus::RebalanceNeeded => HeartbeatStatus::RebalanceNeeded,
        proto::HeartbeatStatus::UnknownMember => HeartbeatStatus::UnknownMember,
    }
}

fn rejoin_status_to_proto(status: RejoinStatus) -> proto::RejoinStatus {
    match status {
        RejoinStatus::Ok => proto::RejoinStatus::Ok,
        RejoinStatus::RebalanceNeeded => proto::RejoinStatus::RebalanceNeeded,
    }
}

fn proto_to_rejoin_status(status: i32) -> RejoinStatus {
    match proto::RejoinStatus::try_from(status).unwrap_or(proto::RejoinStatus::RebalanceNeeded) {
        proto::RejoinStatus::Ok => RejoinStatus::Ok,
        proto::RejoinStatus::RebalanceNeeded => RejoinStatus::RebalanceNeeded,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fetch_request_roundtrip() {
        let req = ReadRequest {
            group_id: "my-group".to_string(),
            reader_id: "reader-1".to_string(),
            generation: Generation(5),
            reads: vec![PartitionRead {
                topic_id: TopicId(1),
                partition_id: PartitionId(0),
                offset: Offset(100),
                max_bytes: 1024,
            }],
        };

        let mut buf = [0u8; 512];
        let encoded_len = encode_read_request(&req, &mut buf);
        let (decoded, decoded_len) = decode_read_request(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert_eq!(decoded.group_id, req.group_id);
        assert_eq!(decoded.reads.len(), 1);
    }

    #[test]
    fn test_join_response_roundtrip() {
        let resp = JoinGroupResponse {
            success: true,
            error_code: 0,
            error_message: String::new(),
            generation: Generation(10),
            assignments: vec![PartitionAssignment {
                topic_id: TopicId(1),
                partition_id: PartitionId(0),
                committed_offset: Offset(42),
            }],
        };

        let mut buf = [0u8; 512];
        let encoded_len = encode_join_response(&resp, &mut buf);
        let (decoded, decoded_len) = decode_join_response(&buf[..encoded_len]).unwrap();
        assert_eq!(decoded_len, encoded_len);
        assert!(decoded.success);
        assert_eq!(decoded.generation.0, 10);
        assert_eq!(decoded.assignments.len(), 1);
    }
}
