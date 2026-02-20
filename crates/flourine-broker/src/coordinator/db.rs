//! SQL operations for reader group coordination.

use sqlx::PgPool;
use tracing::info;
use flourine_common::ids::{Generation, Offset, PartitionId, TopicId};

use super::{
    Assignment, CommitStatus, CoordinatorConfig, HeartbeatResult, HeartbeatStatus, JoinResult,
    RejoinResult, RejoinStatus, compute_assignment,
};

/// Reader group coordinator.
#[derive(Clone)]
pub struct Coordinator {
    pool: PgPool,
    config: CoordinatorConfig,
    broker_id: uuid::Uuid,
}

impl Coordinator {
    /// Create a new coordinator.
    pub fn new(pool: PgPool, config: CoordinatorConfig) -> Self {
        let broker_id = config.broker_id;
        Self {
            pool,
            config,
            broker_id,
        }
    }

    /// Handle a JoinGroup request.
    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(group_id = %group_id, topic_id = topic_id.0, reader_id = %reader_id)
    )]
    pub async fn join_group(
        &self,
        group_id: &str,
        topic_id: TopicId,
        reader_id: &str,
    ) -> Result<JoinResult, sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        // 1. Initialize group if first reader
        self.initialize_group(&mut tx, group_id, topic_id).await?;

        // 2. Register member (upsert)
        sqlx::query(
            r#"
            INSERT INTO reader_members (group_id, topic_id, reader_id, broker_id, last_heartbeat)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (group_id, topic_id, reader_id)
            DO UPDATE SET last_heartbeat = NOW(), broker_id = $4
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .bind(self.broker_id)
        .execute(&mut *tx)
        .await?;

        // 3. Bump generation
        let new_gen: i64 = sqlx::query_scalar(
            r#"
            UPDATE reader_groups
            SET generation = generation + 1
            WHERE group_id = $1 AND topic_id = $2
            RETURNING generation
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .fetch_one(&mut *tx)
        .await?;

        // 4. Get live members
        let session_timeout_secs = self.config.session_timeout.as_secs_f64();
        let members: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT reader_id FROM reader_members
            WHERE group_id = $1 AND topic_id = $2
            AND last_heartbeat > NOW() - make_interval(secs => $3)
            ORDER BY reader_id
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(session_timeout_secs)
        .fetch_all(&mut *tx)
        .await?;

        // 5. Get partition count
        let partition_count: i32 =
            sqlx::query_scalar("SELECT partition_count FROM topics WHERE topic_id = $1")
                .bind(topic_id.0 as i32)
                .fetch_one(&mut *tx)
                .await?;

        // 6. Compute assignment for this reader
        let assigned = compute_assignment(reader_id, &members, partition_count as u32);

        // 7. Claim only unclaimed or expired partitions
        let lease_secs = self.config.lease_duration.as_secs() as i32;
        let mut claimed = Vec::new();

        for p in assigned {
            let result: Option<(i32, i64)> = sqlx::query_as(
                r#"
                UPDATE reader_assignments
                SET reader_id = $1,
                    lease_expires_at = NOW() + make_interval(secs => $2),
                    generation = $3
                WHERE group_id = $4 AND topic_id = $5 AND partition_id = $6
                  AND (reader_id IS NULL OR lease_expires_at < NOW())
                RETURNING partition_id, committed_offset
                "#,
            )
            .bind(reader_id)
            .bind(lease_secs)
            .bind(new_gen)
            .bind(group_id)
            .bind(topic_id.0 as i32)
            .bind(p as i32)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some((part_id, offset)) = result {
                claimed.push(Assignment {
                    partition_id: PartitionId(part_id as u32),
                    committed_offset: Offset(offset as u64),
                });
            }
        }

        tx.commit().await?;

        Ok(JoinResult {
            generation: Generation(new_gen as u64),
            assignments: claimed,
        })
    }

    /// Handle a Heartbeat request.
    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(group_id = %group_id, topic_id = topic_id.0, reader_id = %reader_id, generation = reader_generation.0)
    )]
    pub async fn heartbeat(
        &self,
        group_id: &str,
        topic_id: TopicId,
        reader_id: &str,
        reader_generation: Generation,
    ) -> Result<HeartbeatResult, sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        // 1. Update member heartbeat
        let updated = sqlx::query(
            r#"
            UPDATE reader_members
            SET last_heartbeat = NOW()
            WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .execute(&mut *tx)
        .await?;

        if updated.rows_affected() == 0 {
            tx.commit().await?;
            return Ok(HeartbeatResult {
                generation: Generation(0),
                status: HeartbeatStatus::UnknownMember,
            });
        }

        // 2. Renew leases on owned partitions
        let lease_secs = self.config.lease_duration.as_secs() as i32;
        sqlx::query(
            r#"
            UPDATE reader_assignments
            SET lease_expires_at = NOW() + make_interval(secs => $1)
            WHERE group_id = $2 AND topic_id = $3 AND reader_id = $4
            "#,
        )
        .bind(lease_secs)
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .execute(&mut *tx)
        .await?;

        // 3. Detect and remove expired members
        let session_timeout_secs = self.config.session_timeout.as_secs_f64();
        let expired: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT reader_id FROM reader_members
            WHERE group_id = $1 AND topic_id = $2
            AND last_heartbeat < NOW() - make_interval(secs => $3)
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(session_timeout_secs)
        .fetch_all(&mut *tx)
        .await?;

        let current_gen: i64;

        if !expired.is_empty() {
            // Delete expired members
            sqlx::query(
                r#"
                DELETE FROM reader_members
                WHERE group_id = $1 AND topic_id = $2
                AND reader_id = ANY($3)
                "#,
            )
            .bind(group_id)
            .bind(topic_id.0 as i32)
            .bind(&expired)
            .execute(&mut *tx)
            .await?;

            // Release expired members' partitions
            sqlx::query(
                r#"
                UPDATE reader_assignments
                SET reader_id = NULL, lease_expires_at = NULL
                WHERE group_id = $1 AND topic_id = $2
                AND reader_id = ANY($3)
                "#,
            )
            .bind(group_id)
            .bind(topic_id.0 as i32)
            .bind(&expired)
            .execute(&mut *tx)
            .await?;

            // Bump generation
            current_gen = sqlx::query_scalar(
                r#"
                UPDATE reader_groups
                SET generation = generation + 1
                WHERE group_id = $1 AND topic_id = $2
                RETURNING generation
                "#,
            )
            .bind(group_id)
            .bind(topic_id.0 as i32)
            .fetch_one(&mut *tx)
            .await?;
        } else {
            // Read current generation
            current_gen = sqlx::query_scalar(
                "SELECT generation FROM reader_groups WHERE group_id = $1 AND topic_id = $2",
            )
            .bind(group_id)
            .bind(topic_id.0 as i32)
            .fetch_one(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        let status = if current_gen > reader_generation.0 as i64 {
            info!(
                group_id = group_id,
                topic_id = topic_id.0,
                reader_id = reader_id,
                old_generation = reader_generation.0,
                new_generation = current_gen as u64,
                "rebalance required due to generation change"
            );
            HeartbeatStatus::RebalanceNeeded
        } else {
            HeartbeatStatus::Ok
        };

        Ok(HeartbeatResult {
            generation: Generation(current_gen as u64),
            status,
        })
    }

    /// Handle a Rejoin request (after rebalance notification).
    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(group_id = %group_id, topic_id = topic_id.0, reader_id = %reader_id, generation = generation.0)
    )]
    pub async fn rejoin(
        &self,
        group_id: &str,
        topic_id: TopicId,
        reader_id: &str,
        generation: Generation,
    ) -> Result<RejoinResult, sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        // 1. Verify generation hasn't changed
        let current_gen: i64 = sqlx::query_scalar(
            "SELECT generation FROM reader_groups WHERE group_id = $1 AND topic_id = $2",
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .fetch_one(&mut *tx)
        .await?;

        if current_gen != generation.0 as i64 {
            tx.commit().await?;
            return Ok(RejoinResult {
                generation: Generation(current_gen as u64),
                status: RejoinStatus::RebalanceNeeded,
                assignments: vec![],
            });
        }

        // 2. Get live members
        let session_timeout_secs = self.config.session_timeout.as_secs_f64();
        let members: Vec<String> = sqlx::query_scalar(
            r#"
            SELECT reader_id FROM reader_members
            WHERE group_id = $1 AND topic_id = $2
            AND last_heartbeat > NOW() - make_interval(secs => $3)
            ORDER BY reader_id
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(session_timeout_secs)
        .fetch_all(&mut *tx)
        .await?;

        // 3. Get partition count
        let partition_count: i32 =
            sqlx::query_scalar("SELECT partition_count FROM topics WHERE topic_id = $1")
                .bind(topic_id.0 as i32)
                .fetch_one(&mut *tx)
                .await?;

        // 4. Compute what this reader should own
        let assigned: std::collections::HashSet<u32> =
            compute_assignment(reader_id, &members, partition_count as u32)
                .into_iter()
                .collect();

        // 5. Get what this reader currently owns
        let currently_owned: Vec<i32> = sqlx::query_scalar(
            r#"
            SELECT partition_id FROM reader_assignments
            WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .fetch_all(&mut *tx)
        .await?;

        let currently_owned_set: std::collections::HashSet<u32> =
            currently_owned.iter().map(|&p| p as u32).collect();

        // 6. Release excess partitions
        let excess: Vec<i32> = currently_owned_set
            .difference(&assigned)
            .map(|&p| p as i32)
            .collect();

        if !excess.is_empty() {
            sqlx::query(
                r#"
                UPDATE reader_assignments
                SET reader_id = NULL, lease_expires_at = NULL
                WHERE group_id = $1 AND topic_id = $2
                AND partition_id = ANY($3) AND reader_id = $4
                "#,
            )
            .bind(group_id)
            .bind(topic_id.0 as i32)
            .bind(&excess)
            .bind(reader_id)
            .execute(&mut *tx)
            .await?;
        }

        // 7. Renew leases on kept partitions
        let kept: Vec<i32> = currently_owned_set
            .intersection(&assigned)
            .map(|&p| p as i32)
            .collect();

        let lease_secs = self.config.lease_duration.as_secs() as i32;
        if !kept.is_empty() {
            sqlx::query(
                r#"
                UPDATE reader_assignments
                SET lease_expires_at = NOW() + make_interval(secs => $1), generation = $2
                WHERE group_id = $3 AND topic_id = $4
                AND partition_id = ANY($5) AND reader_id = $6
                "#,
            )
            .bind(lease_secs)
            .bind(generation.0 as i64)
            .bind(group_id)
            .bind(topic_id.0 as i32)
            .bind(&kept)
            .bind(reader_id)
            .execute(&mut *tx)
            .await?;
        }

        // 8. Claim newly-assigned partitions
        let to_claim: Vec<u32> = assigned.difference(&currently_owned_set).copied().collect();
        let mut newly_claimed = Vec::new();

        for p in to_claim {
            let result: Option<(i32, i64)> = sqlx::query_as(
                r#"
                UPDATE reader_assignments
                SET reader_id = $1,
                    lease_expires_at = NOW() + make_interval(secs => $2),
                    generation = $3
                WHERE group_id = $4 AND topic_id = $5 AND partition_id = $6
                  AND (reader_id IS NULL OR lease_expires_at < NOW())
                RETURNING partition_id, committed_offset
                "#,
            )
            .bind(reader_id)
            .bind(lease_secs)
            .bind(generation.0 as i64)
            .bind(group_id)
            .bind(topic_id.0 as i32)
            .bind(p as i32)
            .fetch_optional(&mut *tx)
            .await?;

            if let Some((part_id, offset)) = result {
                newly_claimed.push(Assignment {
                    partition_id: PartitionId(part_id as u32),
                    committed_offset: Offset(offset as u64),
                });
            }
        }

        // 9. Get committed offsets for kept partitions
        let mut all_assignments = Vec::new();

        if !kept.is_empty() {
            let kept_offsets: Vec<(i32, i64)> = sqlx::query_as(
                r#"
                SELECT partition_id, committed_offset FROM reader_assignments
                WHERE group_id = $1 AND topic_id = $2 AND partition_id = ANY($3)
                "#,
            )
            .bind(group_id)
            .bind(topic_id.0 as i32)
            .bind(&kept)
            .fetch_all(&mut *tx)
            .await?;

            for (part_id, offset) in kept_offsets {
                all_assignments.push(Assignment {
                    partition_id: PartitionId(part_id as u32),
                    committed_offset: Offset(offset as u64),
                });
            }
        }

        all_assignments.extend(newly_claimed);

        tx.commit().await?;

        Ok(RejoinResult {
            generation,
            status: RejoinStatus::Ok,
            assignments: all_assignments,
        })
    }

    /// Handle a LeaveGroup request.
    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(group_id = %group_id, topic_id = topic_id.0, reader_id = %reader_id)
    )]
    pub async fn leave_group(
        &self,
        group_id: &str,
        topic_id: TopicId,
        reader_id: &str,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        // 1. Release this reader's partitions
        sqlx::query(
            r#"
            UPDATE reader_assignments
            SET reader_id = NULL, lease_expires_at = NULL
            WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .execute(&mut *tx)
        .await?;

        // 2. Remove member
        sqlx::query(
            r#"
            DELETE FROM reader_members
            WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .execute(&mut *tx)
        .await?;

        // 3. Bump generation
        sqlx::query(
            r#"
            UPDATE reader_groups
            SET generation = generation + 1
            WHERE group_id = $1 AND topic_id = $2
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }

    /// Handle a CommitOffset request.
    #[tracing::instrument(
        level = "debug",
        skip(self),
        fields(
            group_id = %group_id,
            topic_id = topic_id.0,
            reader_id = %reader_id,
            generation = generation.0,
            partition_id = partition_id.0,
            offset = offset.0
        )
    )]
    pub async fn commit_offset(
        &self,
        group_id: &str,
        topic_id: TopicId,
        reader_id: &str,
        generation: Generation,
        partition_id: PartitionId,
        offset: Offset,
    ) -> Result<CommitStatus, sqlx::Error> {
        let result = sqlx::query(
            r#"
            UPDATE reader_assignments
            SET committed_offset = $1
            FROM reader_groups
            WHERE reader_assignments.group_id = $2
              AND reader_assignments.topic_id = $3
              AND reader_assignments.partition_id = $4
              AND reader_id = $5
              AND reader_assignments.generation = $6
              AND reader_groups.group_id = reader_assignments.group_id
              AND reader_groups.topic_id = reader_assignments.topic_id
              AND reader_groups.generation = $6
            "#,
        )
        .bind(offset.0 as i64)
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(partition_id.0 as i32)
        .bind(reader_id)
        .bind(generation.0 as i64)
        .execute(&self.pool)
        .await?;

        if result.rows_affected() > 0 {
            return Ok(CommitStatus::Ok);
        }

        let state: Option<(Option<String>, i64, i64)> = sqlx::query_as(
            r#"
            SELECT reader_assignments.reader_id, reader_assignments.generation, reader_groups.generation
            FROM reader_assignments
            JOIN reader_groups
              ON reader_groups.group_id = reader_assignments.group_id
             AND reader_groups.topic_id = reader_assignments.topic_id
            WHERE reader_assignments.group_id = $1
              AND reader_assignments.topic_id = $2
              AND reader_assignments.partition_id = $3
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(partition_id.0 as i32)
        .fetch_optional(&self.pool)
        .await?;

        let Some((current_owner, assignment_generation, group_generation)) = state else {
            return Ok(CommitStatus::NotOwner);
        };

        if assignment_generation != generation.0 as i64 || group_generation != generation.0 as i64 {
            return Ok(CommitStatus::StaleGeneration);
        }

        if current_owner.as_deref() != Some(reader_id) {
            return Ok(CommitStatus::NotOwner);
        }

        Ok(CommitStatus::NotOwner)
    }

    /// Initialize a reader group and its partition assignments.
    async fn initialize_group(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        group_id: &str,
        topic_id: TopicId,
    ) -> Result<(), sqlx::Error> {
        // Create group if not exists
        sqlx::query(
            r#"
            INSERT INTO reader_groups (group_id, topic_id, generation)
            VALUES ($1, $2, 0)
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .execute(&mut **tx)
        .await?;

        // Get partition count
        let partition_count: i32 =
            sqlx::query_scalar("SELECT partition_count FROM topics WHERE topic_id = $1")
                .bind(topic_id.0 as i32)
                .fetch_one(&mut **tx)
                .await?;

        // Create assignment rows for each partition
        for p in 0..partition_count {
            sqlx::query(
                r#"
                INSERT INTO reader_assignments
                (group_id, topic_id, partition_id, committed_offset, generation)
                VALUES ($1, $2, $3, 0, 0)
                ON CONFLICT DO NOTHING
                "#,
            )
            .bind(group_id)
            .bind(topic_id.0 as i32)
            .bind(p)
            .execute(&mut **tx)
            .await?;
        }

        Ok(())
    }

    /// BREAK-GLASS: Force a rebalance by bumping the generation.
    pub async fn force_rebalance(
        &self,
        group_id: &str,
        topic_id: TopicId,
    ) -> Result<Generation, sqlx::Error> {
        let new_gen: i64 = sqlx::query_scalar(
            r#"
            UPDATE reader_groups
            SET generation = generation + 1
            WHERE group_id = $1 AND topic_id = $2
            RETURNING generation
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .fetch_one(&self.pool)
        .await?;

        Ok(Generation(new_gen as u64))
    }

    /// BREAK-GLASS: Force remove a member from the group.
    pub async fn force_remove_member(
        &self,
        group_id: &str,
        topic_id: TopicId,
        reader_id: &str,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        // Release all partitions owned by this reader
        sqlx::query(
            r#"
            UPDATE reader_assignments
            SET reader_id = NULL, lease_expires_at = NULL
            WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .execute(&mut *tx)
        .await?;

        // Remove from members
        sqlx::query(
            r#"
            DELETE FROM reader_members
            WHERE group_id = $1 AND topic_id = $2 AND reader_id = $3
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .bind(reader_id)
        .execute(&mut *tx)
        .await?;

        // Bump generation to trigger rebalance
        sqlx::query(
            r#"
            UPDATE reader_groups
            SET generation = generation + 1
            WHERE group_id = $1 AND topic_id = $2
            "#,
        )
        .bind(group_id)
        .bind(topic_id.0 as i32)
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;

        Ok(())
    }
}
