//! Reader group protocol handlers: join, heartbeat, rejoin, leave, commit.

use tracing::{error, warn};

use flourine_common::ids::{Generation, TopicId};
use flourine_wire::{ERR_INTERNAL_ERROR, ERR_NOT_OWNER, STATUS_OK, ServerMessage, reader};

use crate::object_store::ObjectStore;

use super::encoding::{
    encode_server_message_vec, RESPONSE_CAPACITY_LARGE, RESPONSE_CAPACITY_SMALL,
};
use super::BrokerState;

/// Handle a JoinGroupRequest.
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(group_id = %req.group_id, reader_id = %req.reader_id, topic_count = req.topic_ids.len())
)]
pub(crate) async fn handle_join_group<S: ObjectStore + Send + Sync>(
    req: reader::JoinGroupRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    // For now, we only support single topic joins
    let topic_id = req.topic_ids.first().copied().unwrap_or(TopicId(0));

    let result = state
        .coordinator
        .join_group(&req.group_id, topic_id, &req.reader_id)
        .await;

    let response = match result {
        Ok(join_result) => reader::JoinGroupResponse {
            success: true,
            error_code: STATUS_OK,
            error_message: String::new(),
            generation: join_result.generation,
            assignments: join_result
                .assignments
                .into_iter()
                .map(|a| reader::PartitionAssignment {
                    topic_id,
                    partition_id: a.partition_id,
                    committed_offset: a.committed_offset,
                })
                .collect(),
        },
        Err(e) => {
            error!("JoinGroup error: {}", e);
            reader::JoinGroupResponse {
                success: false,
                error_code: ERR_INTERNAL_ERROR,
                error_message: "join group failed".to_string(),
                generation: Generation(0),
                assignments: vec![],
            }
        }
    };

    encode_server_message_vec(ServerMessage::JoinGroup(response), RESPONSE_CAPACITY_LARGE)
}

/// Handle a HeartbeatRequest.
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(group_id = %req.group_id, reader_id = %req.reader_id, topic_id = req.topic_id.0, generation = req.generation.0)
)]
pub(crate) async fn handle_heartbeat<S: ObjectStore + Send + Sync>(
    req: reader::HeartbeatRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    let result = state
        .coordinator
        .heartbeat(
            &req.group_id,
            req.topic_id,
            &req.reader_id,
            req.generation,
        )
        .await;

    let response = match result {
        Ok(hb_result) => reader::HeartbeatResponseExt {
            success: true,
            error_code: STATUS_OK,
            error_message: String::new(),
            generation: hb_result.generation,
            status: match hb_result.status {
                crate::coordinator::HeartbeatStatus::Ok => reader::HeartbeatStatus::Ok,
                crate::coordinator::HeartbeatStatus::RebalanceNeeded => {
                    reader::HeartbeatStatus::RebalanceNeeded
                }
                crate::coordinator::HeartbeatStatus::UnknownMember => {
                    reader::HeartbeatStatus::UnknownMember
                }
            },
        },
        Err(e) => {
            error!("Heartbeat error: {}", e);
            reader::HeartbeatResponseExt {
                success: false,
                error_code: ERR_INTERNAL_ERROR,
                error_message: "heartbeat failed".to_string(),
                generation: Generation(0),
                status: reader::HeartbeatStatus::UnknownMember,
            }
        }
    };

    encode_server_message_vec(ServerMessage::Heartbeat(response), RESPONSE_CAPACITY_SMALL)
}

/// Handle a RejoinRequest.
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(group_id = %req.group_id, reader_id = %req.reader_id, topic_id = req.topic_id.0, generation = req.generation.0)
)]
pub(crate) async fn handle_rejoin<S: ObjectStore + Send + Sync>(
    req: reader::RejoinRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    let result = state
        .coordinator
        .rejoin(
            &req.group_id,
            req.topic_id,
            &req.reader_id,
            req.generation,
        )
        .await;

    let response = match result {
        Ok(rejoin_result) => reader::RejoinResponse {
            success: true,
            error_code: STATUS_OK,
            error_message: String::new(),
            generation: rejoin_result.generation,
            status: match rejoin_result.status {
                crate::coordinator::RejoinStatus::Ok => reader::RejoinStatus::Ok,
                crate::coordinator::RejoinStatus::RebalanceNeeded => {
                    reader::RejoinStatus::RebalanceNeeded
                }
            },
            assignments: rejoin_result
                .assignments
                .into_iter()
                .map(|a| reader::PartitionAssignment {
                    topic_id: req.topic_id,
                    partition_id: a.partition_id,
                    committed_offset: a.committed_offset,
                })
                .collect(),
        },
        Err(e) => {
            error!("Rejoin error: {}", e);
            reader::RejoinResponse {
                success: false,
                error_code: ERR_INTERNAL_ERROR,
                error_message: "rejoin failed".to_string(),
                generation: Generation(0),
                status: reader::RejoinStatus::RebalanceNeeded,
                assignments: vec![],
            }
        }
    };

    encode_server_message_vec(ServerMessage::Rejoin(response), RESPONSE_CAPACITY_LARGE)
}

/// Handle a LeaveGroupRequest.
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(group_id = %req.group_id, reader_id = %req.reader_id, topic_id = req.topic_id.0)
)]
pub(crate) async fn handle_leave_group<S: ObjectStore + Send + Sync>(
    req: reader::LeaveGroupRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    let result = state
        .coordinator
        .leave_group(&req.group_id, req.topic_id, &req.reader_id)
        .await;

    let response = reader::LeaveGroupResponse {
        success: result.is_ok(),
        error_code: if result.is_ok() {
            STATUS_OK
        } else {
            ERR_INTERNAL_ERROR
        },
        error_message: if result.is_ok() {
            String::new()
        } else {
            "leave group failed".to_string()
        },
    };

    if let Err(e) = result {
        error!("LeaveGroup error: {}", e);
    }

    encode_server_message_vec(ServerMessage::LeaveGroup(response), RESPONSE_CAPACITY_SMALL)
}

/// Handle a CommitRequest.
#[tracing::instrument(
    level = "debug",
    skip(state),
    fields(group_id = %req.group_id, reader_id = %req.reader_id, generation = req.generation.0, commit_count = req.commits.len())
)]
pub(crate) async fn handle_commit<S: ObjectStore + Send + Sync>(
    req: reader::CommitRequest,
    state: &BrokerState<S>,
) -> Vec<u8> {
    let mut all_ok = true;
    let mut error_code = STATUS_OK;
    let mut error_message = String::new();

    for commit in &req.commits {
        let result = state
            .coordinator
            .commit_offset(
                &req.group_id,
                commit.topic_id,
                &req.reader_id,
                req.generation,
                commit.partition_id,
                commit.offset,
            )
            .await;

        match result {
            Ok(crate::coordinator::CommitStatus::Ok) => {}
            Ok(crate::coordinator::CommitStatus::StaleGeneration) => {
                warn!(
                    "Commit rejected: stale generation for reader {} on partition {}",
                    req.reader_id, commit.partition_id.0
                );
                all_ok = false;
                error_code = ERR_NOT_OWNER;
                error_message = "commit rejected: stale generation".to_string();
            }
            Ok(crate::coordinator::CommitStatus::NotOwner) => {
                warn!(
                    "Commit rejected: reader {} doesn't own partition {}",
                    req.reader_id, commit.partition_id.0
                );
                all_ok = false;
                error_code = ERR_NOT_OWNER;
                error_message = "commit rejected: not owner".to_string();
            }
            Err(e) => {
                error!("Commit error: {}", e);
                all_ok = false;
                error_code = ERR_INTERNAL_ERROR;
                error_message = "commit failed".to_string();
            }
        }
    }

    let response = reader::CommitResponse {
        success: all_ok,
        error_code,
        error_message,
    };

    encode_server_message_vec(ServerMessage::Commit(response), RESPONSE_CAPACITY_SMALL)
}
