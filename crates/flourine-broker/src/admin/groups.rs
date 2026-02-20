//! Reader group management endpoints with break-glass operations.

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
};
use serde::Serialize;
use flourine_common::ids::TopicId;

use super::AdminState;

/// Reader group summary.
#[derive(Debug, Serialize)]
pub struct GroupSummary {
    pub group_id: String,
    pub topic_id: i32,
    pub member_count: i32,
    pub generation: i64,
}

/// Reader group member.
#[derive(Debug, Serialize)]
pub struct GroupMember {
    pub reader_id: String,
    pub partitions: Vec<i32>,
    pub last_heartbeat: chrono::DateTime<chrono::Utc>,
}

/// Reader group details.
#[derive(Debug, Serialize)]
pub struct GroupDetails {
    pub group_id: String,
    pub topic_id: i32,
    pub generation: i64,
    pub members: Vec<GroupMember>,
}

/// Response from force rebalance.
#[derive(Debug, Serialize)]
pub struct RebalanceResponse {
    pub new_generation: i64,
}

pub fn router() -> Router<AdminState> {
    Router::new()
        .route("/", get(list_groups))
        .route("/:group_id/topics/:topic_id", get(get_group))
        .route(
            "/:group_id/topics/:topic_id/rebalance",
            post(force_rebalance),
        )
        .route(
            "/:group_id/topics/:topic_id/members/:member_id",
            delete(force_remove_member),
        )
}

async fn list_groups(
    State(state): State<AdminState>,
) -> Result<Json<Vec<GroupSummary>>, StatusCode> {
    let rows: Vec<GroupRow> = sqlx::query_as(
        r#"
        SELECT
            group_id,
            topic_id,
            generation,
            (SELECT COUNT(*)::int FROM reader_members cm
             WHERE cm.group_id = cg.group_id AND cm.topic_id = cg.topic_id) as member_count
        FROM reader_groups cg
        ORDER BY group_id, topic_id
        "#,
    )
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let groups: Vec<GroupSummary> = rows
        .into_iter()
        .map(|r| GroupSummary {
            group_id: r.group_id,
            topic_id: r.topic_id,
            member_count: r.member_count,
            generation: r.generation,
        })
        .collect();

    Ok(Json(groups))
}

async fn get_group(
    State(state): State<AdminState>,
    Path((group_id, topic_id)): Path<(String, i32)>,
) -> Result<Json<GroupDetails>, StatusCode> {
    // Get group info
    let group: Option<(i64,)> = sqlx::query_as(
        r#"
        SELECT generation
        FROM reader_groups
        WHERE group_id = $1 AND topic_id = $2
        "#,
    )
    .bind(&group_id)
    .bind(topic_id)
    .fetch_optional(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let generation = group.ok_or(StatusCode::NOT_FOUND)?.0;

    // Get members with their partitions
    let member_rows: Vec<MemberRow> = sqlx::query_as(
        r#"
        SELECT reader_id, last_heartbeat
        FROM reader_members
        WHERE group_id = $1 AND topic_id = $2
        ORDER BY reader_id
        "#,
    )
    .bind(&group_id)
    .bind(topic_id)
    .fetch_all(&state.pool)
    .await
    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut members = Vec::with_capacity(member_rows.len());
    for member in member_rows {
        let partitions: Vec<(i32,)> = sqlx::query_as(
            r#"
            SELECT partition_id
            FROM reader_assignments
            WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3
            ORDER BY partition_id
            "#,
        )
        .bind(&group_id)
        .bind(topic_id)
        .bind(&member.reader_id)
        .fetch_all(&state.pool)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        members.push(GroupMember {
            reader_id: member.reader_id,
            partitions: partitions.into_iter().map(|(p,)| p).collect(),
            last_heartbeat: member.last_heartbeat,
        });
    }

    Ok(Json(GroupDetails {
        group_id,
        topic_id,
        generation,
        members,
    }))
}

/// BREAK-GLASS: Force a rebalance by bumping the generation.
async fn force_rebalance(
    State(state): State<AdminState>,
    Path((group_id, topic_id)): Path<(String, i32)>,
) -> Result<Json<RebalanceResponse>, StatusCode> {
    // Use coordinator to trigger rebalance
    let result = state
        .coordinator
        .force_rebalance(&group_id, TopicId(topic_id as u32))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(RebalanceResponse {
        new_generation: result.0 as i64,
    }))
}

/// BREAK-GLASS: Force remove a member from the group.
async fn force_remove_member(
    State(state): State<AdminState>,
    Path((group_id, topic_id, member_id)): Path<(String, i32, String)>,
) -> Result<StatusCode, StatusCode> {
    // Use coordinator to remove member
    state
        .coordinator
        .force_remove_member(&group_id, TopicId(topic_id as u32), &member_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(StatusCode::NO_CONTENT)
}

#[derive(Debug, sqlx::FromRow)]
struct GroupRow {
    group_id: String,
    topic_id: i32,
    generation: i64,
    member_count: i32,
}

#[derive(Debug, sqlx::FromRow)]
struct MemberRow {
    reader_id: String,
    last_heartbeat: chrono::DateTime<chrono::Utc>,
}
