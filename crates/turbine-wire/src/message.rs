//! Message type discrimination for the wire protocol.
//!
//! Each message is prefixed with a single byte indicating the message type.

use crate::DecodeError;

/// Message types supported by the wire protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageType {
    /// ProduceRequest from producer to agent.
    Produce = 1,
    /// ProduceResponse from agent to producer.
    ProduceResponse = 2,
    /// FetchRequest from consumer to agent.
    Fetch = 3,
    /// FetchResponse from agent to consumer.
    FetchResponse = 4,
    /// JoinGroupRequest from consumer to agent.
    JoinGroup = 5,
    /// JoinGroupResponse from agent to consumer.
    JoinGroupResponse = 6,
    /// HeartbeatRequest from consumer to agent.
    Heartbeat = 7,
    /// HeartbeatResponse from agent to consumer.
    HeartbeatResponse = 8,
    /// RejoinRequest from consumer to agent.
    Rejoin = 9,
    /// RejoinResponse from agent to consumer.
    RejoinResponse = 10,
    /// LeaveGroupRequest from consumer to agent.
    LeaveGroup = 11,
    /// LeaveGroupResponse from agent to consumer.
    LeaveGroupResponse = 12,
    /// CommitRequest from consumer to agent.
    Commit = 13,
    /// CommitResponse from agent to consumer.
    CommitResponse = 14,
}

impl MessageType {
    /// Decode message type from first byte.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(MessageType::Produce),
            2 => Some(MessageType::ProduceResponse),
            3 => Some(MessageType::Fetch),
            4 => Some(MessageType::FetchResponse),
            5 => Some(MessageType::JoinGroup),
            6 => Some(MessageType::JoinGroupResponse),
            7 => Some(MessageType::Heartbeat),
            8 => Some(MessageType::HeartbeatResponse),
            9 => Some(MessageType::Rejoin),
            10 => Some(MessageType::RejoinResponse),
            11 => Some(MessageType::LeaveGroup),
            12 => Some(MessageType::LeaveGroupResponse),
            13 => Some(MessageType::Commit),
            14 => Some(MessageType::CommitResponse),
            _ => None,
        }
    }

    /// Encode message type as byte.
    pub fn to_byte(self) -> u8 {
        self as u8
    }
}

/// Peek the message type from a buffer without consuming it.
pub fn peek_message_type(buf: &[u8]) -> Result<MessageType, DecodeError> {
    if buf.is_empty() {
        return Err(DecodeError::UnexpectedEof { needed: 1 });
    }
    MessageType::from_byte(buf[0]).ok_or(DecodeError::InvalidMessageType { tag: buf[0] })
}

/// Write message type prefix to buffer.
pub fn write_message_type(msg_type: MessageType, buf: &mut [u8]) -> usize {
    buf[0] = msg_type.to_byte();
    1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_type_roundtrip() {
        let types = [
            MessageType::Produce,
            MessageType::ProduceResponse,
            MessageType::Fetch,
            MessageType::FetchResponse,
            MessageType::JoinGroup,
            MessageType::JoinGroupResponse,
            MessageType::Heartbeat,
            MessageType::HeartbeatResponse,
            MessageType::Rejoin,
            MessageType::RejoinResponse,
            MessageType::LeaveGroup,
            MessageType::LeaveGroupResponse,
            MessageType::Commit,
            MessageType::CommitResponse,
        ];

        for msg_type in types {
            let byte = msg_type.to_byte();
            let decoded = MessageType::from_byte(byte).unwrap();
            assert_eq!(decoded, msg_type);
        }
    }

    #[test]
    fn test_peek_message_type() {
        let mut buf = [0u8; 10];
        write_message_type(MessageType::JoinGroup, &mut buf);

        let peeked = peek_message_type(&buf).unwrap();
        assert_eq!(peeked, MessageType::JoinGroup);
    }

    #[test]
    fn test_invalid_message_type() {
        let buf = [255u8];
        let result = peek_message_type(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_buffer() {
        let buf: &[u8] = &[];
        let result = peek_message_type(buf);
        assert!(result.is_err());
    }
}
