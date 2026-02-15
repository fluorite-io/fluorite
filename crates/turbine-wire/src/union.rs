//! Protobuf oneof-framed client/server messages.

use prost::Message;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::{DecodeError, EncodeError, auth, reader, writer, proto};

const INITIAL_ENCODE_BUFFER: usize = 64 * 1024;

/// Client-to-broker message variants.
#[derive(Debug, Clone)]
pub enum ClientMessage {
    Append(writer::AppendRequest),
    Read(reader::ReadRequest),
    JoinGroup(reader::JoinGroupRequest),
    Heartbeat(reader::HeartbeatRequest),
    Rejoin(reader::RejoinRequest),
    LeaveGroup(reader::LeaveGroupRequest),
    Commit(reader::CommitRequest),
    Auth(auth::AuthRequest),
}

/// Broker-to-client message variants.
#[derive(Debug, Clone)]
pub enum ServerMessage {
    Append(writer::AppendResponse),
    Read(reader::ReadResponse),
    JoinGroup(reader::JoinGroupResponse),
    Heartbeat(reader::HeartbeatResponseExt),
    Rejoin(reader::RejoinResponse),
    LeaveGroup(reader::LeaveGroupResponse),
    Commit(reader::CommitResponse),
    Auth(auth::AuthResponse),
}

/// Encode a client message into `buf`.
pub fn encode_client_message(msg: &ClientMessage, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let envelope = match msg {
        ClientMessage::Append(req) => proto::ClientMessage {
            message: Some(proto::client_message::Message::Append(
                decode_from_encoded(req, writer::encode_request, "append request")?,
            )),
        },
        ClientMessage::Read(req) => proto::ClientMessage {
            message: Some(proto::client_message::Message::Read(decode_from_encoded(
                req,
                reader::encode_read_request,
                "read request",
            )?)),
        },
        ClientMessage::JoinGroup(req) => proto::ClientMessage {
            message: Some(proto::client_message::Message::JoinGroup(
                decode_from_encoded(req, reader::encode_join_request, "join request")?,
            )),
        },
        ClientMessage::Heartbeat(req) => proto::ClientMessage {
            message: Some(proto::client_message::Message::Heartbeat(
                decode_from_encoded(req, reader::encode_heartbeat_request, "heartbeat request")?,
            )),
        },
        ClientMessage::Rejoin(req) => proto::ClientMessage {
            message: Some(proto::client_message::Message::Rejoin(decode_from_encoded(
                req,
                reader::encode_rejoin_request,
                "rejoin request",
            )?)),
        },
        ClientMessage::LeaveGroup(req) => proto::ClientMessage {
            message: Some(proto::client_message::Message::LeaveGroup(
                decode_from_encoded(req, reader::encode_leave_request, "leave request")?,
            )),
        },
        ClientMessage::Commit(req) => proto::ClientMessage {
            message: Some(proto::client_message::Message::Commit(decode_from_encoded(
                req,
                reader::encode_commit_request,
                "commit request",
            )?)),
        },
        ClientMessage::Auth(req) => proto::ClientMessage {
            message: Some(proto::client_message::Message::Auth(
                decode_from_encoded_result(req, auth::encode_auth_request, "auth request")?,
            )),
        },
    };

    encode_protobuf(&envelope, buf)
}

/// Decode a client message from `buf`.
pub fn decode_client_message(buf: &[u8]) -> Result<(ClientMessage, usize), DecodeError> {
    let envelope = proto::ClientMessage::decode(buf).map_err(|_| DecodeError::InvalidData {
        msg: "invalid protobuf client envelope",
    })?;

    let message = envelope.message.ok_or(DecodeError::InvalidData {
        msg: "empty protobuf client envelope",
    })?;

    let decoded =
        match message {
            proto::client_message::Message::Append(inner) => ClientMessage::Append(
                encode_and_decode(inner, writer::decode_request, "append request")?,
            ),
            proto::client_message::Message::Read(inner) => ClientMessage::Read(
                encode_and_decode(inner, reader::decode_read_request, "read request")?,
            ),
            proto::client_message::Message::JoinGroup(inner) => ClientMessage::JoinGroup(
                encode_and_decode(inner, reader::decode_join_request, "join request")?,
            ),
            proto::client_message::Message::Heartbeat(inner) => {
                ClientMessage::Heartbeat(encode_and_decode(
                    inner,
                    reader::decode_heartbeat_request,
                    "heartbeat request",
                )?)
            }
            proto::client_message::Message::Rejoin(inner) => ClientMessage::Rejoin(
                encode_and_decode(inner, reader::decode_rejoin_request, "rejoin request")?,
            ),
            proto::client_message::Message::LeaveGroup(inner) => ClientMessage::LeaveGroup(
                encode_and_decode(inner, reader::decode_leave_request, "leave request")?,
            ),
            proto::client_message::Message::Commit(inner) => ClientMessage::Commit(
                encode_and_decode(inner, reader::decode_commit_request, "commit request")?,
            ),
            proto::client_message::Message::Auth(inner) => ClientMessage::Auth(encode_and_decode(
                inner,
                auth::decode_auth_request,
                "auth request",
            )?),
        };

    Ok((decoded, buf.len()))
}

/// Encode a server message into `buf`.
pub fn encode_server_message(msg: &ServerMessage, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let envelope = match msg {
        ServerMessage::Append(resp) => proto::ServerMessage {
            message: Some(proto::server_message::Message::Append(
                decode_from_encoded_result(
                    resp,
                    writer::encode_response_checked,
                    "append response",
                )?,
            )),
        },
        ServerMessage::Read(resp) => proto::ServerMessage {
            message: Some(proto::server_message::Message::Read(
                decode_from_encoded_result(
                    resp,
                    reader::encode_read_response_checked,
                    "read response",
                )?,
            )),
        },
        ServerMessage::JoinGroup(resp) => proto::ServerMessage {
            message: Some(proto::server_message::Message::JoinGroup(
                decode_from_encoded_result(
                    resp,
                    reader::encode_join_response_checked,
                    "join response",
                )?,
            )),
        },
        ServerMessage::Heartbeat(resp) => proto::ServerMessage {
            message: Some(proto::server_message::Message::Heartbeat(
                decode_from_encoded_result(
                    resp,
                    reader::encode_heartbeat_response_ext_checked,
                    "heartbeat response",
                )?,
            )),
        },
        ServerMessage::Rejoin(resp) => proto::ServerMessage {
            message: Some(proto::server_message::Message::Rejoin(
                decode_from_encoded_result(
                    resp,
                    reader::encode_rejoin_response_checked,
                    "rejoin response",
                )?,
            )),
        },
        ServerMessage::LeaveGroup(resp) => proto::ServerMessage {
            message: Some(proto::server_message::Message::LeaveGroup(
                decode_from_encoded_result(
                    resp,
                    reader::encode_leave_response_checked,
                    "leave response",
                )?,
            )),
        },
        ServerMessage::Commit(resp) => proto::ServerMessage {
            message: Some(proto::server_message::Message::Commit(
                decode_from_encoded_result(
                    resp,
                    reader::encode_commit_response_checked,
                    "commit response",
                )?,
            )),
        },
        ServerMessage::Auth(resp) => proto::ServerMessage {
            message: Some(proto::server_message::Message::Auth(
                decode_from_encoded_result(resp, auth::encode_auth_response, "auth response")?,
            )),
        },
    };

    encode_protobuf(&envelope, buf)
}

/// Decode a server message from `buf`.
pub fn decode_server_message(buf: &[u8]) -> Result<(ServerMessage, usize), DecodeError> {
    let envelope = proto::ServerMessage::decode(buf).map_err(|_| DecodeError::InvalidData {
        msg: "invalid protobuf server envelope",
    })?;

    let message = envelope.message.ok_or(DecodeError::InvalidData {
        msg: "empty protobuf server envelope",
    })?;

    let decoded =
        match message {
            proto::server_message::Message::Append(inner) => ServerMessage::Append(
                encode_and_decode(inner, writer::decode_response, "append response")?,
            ),
            proto::server_message::Message::Read(inner) => ServerMessage::Read(
                encode_and_decode(inner, reader::decode_read_response, "read response")?,
            ),
            proto::server_message::Message::JoinGroup(inner) => ServerMessage::JoinGroup(
                encode_and_decode(inner, reader::decode_join_response, "join response")?,
            ),
            proto::server_message::Message::Heartbeat(inner) => {
                ServerMessage::Heartbeat(encode_and_decode(
                    inner,
                    reader::decode_heartbeat_response_ext,
                    "heartbeat response",
                )?)
            }
            proto::server_message::Message::Rejoin(inner) => ServerMessage::Rejoin(
                encode_and_decode(inner, reader::decode_rejoin_response, "rejoin response")?,
            ),
            proto::server_message::Message::LeaveGroup(inner) => ServerMessage::LeaveGroup(
                encode_and_decode(inner, reader::decode_leave_response, "leave response")?,
            ),
            proto::server_message::Message::Commit(inner) => ServerMessage::Commit(
                encode_and_decode(inner, reader::decode_commit_response, "commit response")?,
            ),
            proto::server_message::Message::Auth(inner) => ServerMessage::Auth(encode_and_decode(
                inner,
                auth::decode_auth_response,
                "auth response",
            )?),
        };

    Ok((decoded, buf.len()))
}

fn encode_protobuf<M: Message>(msg: &M, buf: &mut [u8]) -> Result<usize, EncodeError> {
    let encoded = msg.encode_to_vec();
    if encoded.len() > buf.len() {
        return Err(EncodeError::BufferTooSmall {
            needed: encoded.len(),
            available: buf.len(),
        });
    }
    buf[..encoded.len()].copy_from_slice(&encoded);
    Ok(encoded.len())
}

fn decode_from_encoded<T, M>(
    value: &T,
    encoder: fn(&T, &mut [u8]) -> usize,
    msg: &'static str,
) -> Result<M, EncodeError>
where
    M: Message + Default,
{
    let mut capacity = INITIAL_ENCODE_BUFFER;

    loop {
        let mut buf = vec![0u8; capacity];
        match catch_unwind(AssertUnwindSafe(|| encoder(value, &mut buf))) {
            Ok(len) => {
                return M::decode(&buf[..len])
                    .map_err(|_| EncodeError::ValueTooLarge(format!("invalid {}", msg)));
            }
            Err(_) => {
                capacity = capacity
                    .checked_mul(2)
                    .ok_or_else(|| EncodeError::ValueTooLarge(format!("invalid {}", msg)))?;
            }
        }
    }
}

fn decode_from_encoded_result<T, M>(
    value: &T,
    encoder: fn(&T, &mut [u8]) -> Result<usize, EncodeError>,
    msg: &'static str,
) -> Result<M, EncodeError>
where
    M: Message + Default,
{
    let mut capacity = INITIAL_ENCODE_BUFFER;

    loop {
        let mut buf = vec![0u8; capacity];
        match encoder(value, &mut buf) {
            Ok(len) => {
                return M::decode(&buf[..len])
                    .map_err(|_| EncodeError::ValueTooLarge(format!("invalid {}", msg)));
            }
            Err(EncodeError::BufferTooSmall { needed, .. }) => {
                capacity = needed.max(
                    capacity
                        .checked_mul(2)
                        .ok_or_else(|| EncodeError::ValueTooLarge(format!("invalid {}", msg)))?,
                );
            }
            Err(err) => return Err(err),
        }
    }
}

fn encode_and_decode<M, T>(
    message: M,
    decoder: fn(&[u8]) -> Result<(T, usize), DecodeError>,
    _msg: &'static str,
) -> Result<T, DecodeError>
where
    M: Message,
{
    let buf = message.encode_to_vec();
    let (decoded, used) = decoder(&buf)?;
    if used != buf.len() {
        return Err(DecodeError::InvalidData {
            msg: "trailing bytes in protobuf payload",
        });
    }
    Ok(decoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use turbine_common::ids::{
        Generation, Offset, PartitionId, WriterId, SchemaId, AppendSeq, TopicId,
    };
    use turbine_common::types::{Record, RecordBatch};

    #[test]
    fn test_client_union_roundtrip_produce() {
        let msg = ClientMessage::Append(writer::AppendRequest {
            writer_id: WriterId::new(),
            append_seq: AppendSeq(7),
            batches: vec![RecordBatch {
                topic_id: TopicId(1),
                partition_id: PartitionId(0),
                schema_id: SchemaId(100),
                records: vec![Record {
                    key: Some(Bytes::from_static(b"k")),
                    value: Bytes::from_static(b"v"),
                }],
            }],
        });
        let mut buf = vec![0u8; 8192];
        let len = encode_client_message(&msg, &mut buf).unwrap();
        let (decoded, used) = decode_client_message(&buf[..len]).unwrap();
        assert_eq!(used, len);
        assert!(matches!(decoded, ClientMessage::Append(_)));
    }

    #[test]
    fn test_server_union_roundtrip_fetch() {
        let msg = ServerMessage::Read(reader::ReadResponse {
            success: true,
            error_code: 0,
            error_message: String::new(),
            results: vec![reader::PartitionResult {
                topic_id: TopicId(1),
                partition_id: PartitionId(0),
                schema_id: SchemaId(100),
                high_watermark: Offset(1),
                records: vec![Record {
                    key: None,
                    value: Bytes::from_static(b"v"),
                }],
            }],
        });
        let mut buf = vec![0u8; 8192];
        let len = encode_server_message(&msg, &mut buf).unwrap();
        let (decoded, used) = decode_server_message(&buf[..len]).unwrap();
        assert_eq!(used, len);
        assert!(matches!(decoded, ServerMessage::Read(_)));
    }

    #[test]
    fn test_invalid_union_index() {
        let envelope = proto::ClientMessage {
            message: Some(proto::client_message::Message::Append(
                proto::AppendRequest::default(),
            )),
        };
        let mut raw = envelope.encode_to_vec();
        if let Some(first) = raw.first_mut() {
            *first = 0;
        }
        let err = decode_client_message(&raw).unwrap_err();
        assert!(matches!(err, DecodeError::InvalidData { .. }));
    }

    #[test]
    fn test_server_union_roundtrip_heartbeat() {
        let msg = ServerMessage::Heartbeat(reader::HeartbeatResponseExt {
            success: true,
            error_code: 0,
            error_message: String::new(),
            generation: Generation(3),
            status: reader::HeartbeatStatus::Ok,
        });
        let mut buf = vec![0u8; 1024];
        let len = encode_server_message(&msg, &mut buf).unwrap();
        let (decoded, used) = decode_server_message(&buf[..len]).unwrap();
        assert_eq!(used, len);
        assert!(matches!(decoded, ServerMessage::Heartbeat(_)));
    }
}
