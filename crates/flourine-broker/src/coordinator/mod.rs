//! Reader group coordinator.
//!
//! Implements lease-based partition assignment using Postgres.
//! All coordination state lives in the database - brokers are stateless.

mod db;

use std::time::Duration;
use flourine_common::ids::{Generation, Offset, PartitionId};

/// Coordinator configuration.
#[derive(Debug, Clone)]
pub struct CoordinatorConfig {
    /// Partition lease duration.
    pub lease_duration: Duration,
    /// Session timeout for detecting dead readers.
    pub session_timeout: Duration,
    /// Broker ID for this coordinator instance.
    pub broker_id: uuid::Uuid,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_secs(45),
            session_timeout: Duration::from_secs(30),
            broker_id: uuid::Uuid::new_v4(),
        }
    }
}

/// Partition assignment with committed offset.
#[derive(Debug, Clone)]
pub struct Assignment {
    pub partition_id: PartitionId,
    pub committed_offset: Offset,
}

/// Result of a join group operation.
#[derive(Debug)]
pub struct JoinResult {
    pub generation: Generation,
    pub assignments: Vec<Assignment>,
}

/// Heartbeat status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatStatus {
    Ok,
    RebalanceNeeded,
    UnknownMember,
}

/// Result of a heartbeat operation.
#[derive(Debug)]
pub struct HeartbeatResult {
    pub generation: Generation,
    pub status: HeartbeatStatus,
}

/// Rejoin status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejoinStatus {
    Ok,
    RebalanceNeeded,
}

/// Result of a rejoin operation.
#[derive(Debug)]
pub struct RejoinResult {
    pub generation: Generation,
    pub status: RejoinStatus,
    pub assignments: Vec<Assignment>,
}

/// Commit status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitStatus {
    Ok,
    NotOwner,
    StaleGeneration,
}

pub use db::Coordinator;

/// Compute partition assignment for a reader.
///
/// Uses deterministic range-based assignment. Given a sorted member list
/// and partition count, any broker computes the same result.
pub fn compute_assignment(reader_id: &str, members: &[String], partition_count: u32) -> Vec<u32> {
    if members.is_empty() || partition_count == 0 {
        return vec![];
    }

    let mut sorted_members = members.to_vec();
    sorted_members.sort();

    let my_index = match sorted_members.iter().position(|m| m == reader_id) {
        Some(idx) => idx,
        None => return vec![],
    };

    let n = sorted_members.len();
    let per_member = partition_count as usize / n;
    let remainder = partition_count as usize % n;

    let (start, count) = if my_index < remainder {
        (my_index * (per_member + 1), per_member + 1)
    } else {
        (
            remainder * (per_member + 1) + (my_index - remainder) * per_member,
            per_member,
        )
    };

    (start..start + count).map(|p| p as u32).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_assignment_single_consumer() {
        let members = vec!["reader-a".to_string()];
        let result = compute_assignment("reader-a", &members, 10);
        assert_eq!(result, vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_compute_assignment_two_consumers_even() {
        let members = vec!["a".to_string(), "b".to_string()];

        let result_a = compute_assignment("a", &members, 10);
        assert_eq!(result_a, vec![0, 1, 2, 3, 4]);

        let result_b = compute_assignment("b", &members, 10);
        assert_eq!(result_b, vec![5, 6, 7, 8, 9]);
    }

    #[test]
    fn test_compute_assignment_three_consumers_uneven() {
        let members = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        // 10 partitions / 3 readers = 3 each + 1 extra for first reader
        let result_a = compute_assignment("a", &members, 10);
        assert_eq!(result_a, vec![0, 1, 2, 3]); // 4 partitions

        let result_b = compute_assignment("b", &members, 10);
        assert_eq!(result_b, vec![4, 5, 6]); // 3 partitions

        let result_c = compute_assignment("c", &members, 10);
        assert_eq!(result_c, vec![7, 8, 9]); // 3 partitions
    }

    #[test]
    fn test_compute_assignment_more_consumers_than_partitions() {
        let members = vec![
            "a".to_string(),
            "b".to_string(),
            "c".to_string(),
            "d".to_string(),
            "e".to_string(),
        ];

        // 3 partitions, 5 readers - some get nothing
        let result_a = compute_assignment("a", &members, 3);
        assert_eq!(result_a, vec![0]);

        let result_b = compute_assignment("b", &members, 3);
        assert_eq!(result_b, vec![1]);

        let result_c = compute_assignment("c", &members, 3);
        assert_eq!(result_c, vec![2]);

        let result_d = compute_assignment("d", &members, 3);
        assert!(result_d.is_empty());

        let result_e = compute_assignment("e", &members, 3);
        assert!(result_e.is_empty());
    }

    #[test]
    fn test_compute_assignment_consumer_not_in_list() {
        let members = vec!["a".to_string(), "b".to_string()];
        let result = compute_assignment("unknown", &members, 10);
        assert!(result.is_empty());
    }

    #[test]
    fn test_compute_assignment_empty_members() {
        let result = compute_assignment("a", &[], 10);
        assert!(result.is_empty());
    }

    #[test]
    fn test_compute_assignment_zero_partitions() {
        let members = vec!["a".to_string()];
        let result = compute_assignment("a", &members, 0);
        assert!(result.is_empty());
    }

    #[test]
    fn test_compute_assignment_order_independent() {
        // Members in different order should append same assignment
        let members1 = vec!["c".to_string(), "a".to_string(), "b".to_string()];
        let members2 = vec!["a".to_string(), "b".to_string(), "c".to_string()];

        let result1 = compute_assignment("b", &members1, 10);
        let result2 = compute_assignment("b", &members2, 10);

        assert_eq!(result1, result2);
    }

    #[test]
    fn test_all_partitions_covered() {
        let members = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let partition_count = 10u32;

        let mut all_partitions: Vec<u32> = Vec::new();
        for member in &members {
            all_partitions.extend(compute_assignment(member, &members, partition_count));
        }

        all_partitions.sort();
        let expected: Vec<u32> = (0..partition_count).collect();
        assert_eq!(all_partitions, expected);
    }

    #[test]
    fn test_no_duplicate_assignments() {
        let members = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        let partition_count = 10u32;

        let mut all_partitions: Vec<u32> = Vec::new();
        for member in &members {
            all_partitions.extend(compute_assignment(member, &members, partition_count));
        }

        let unique: std::collections::HashSet<u32> = all_partitions.iter().copied().collect();
        assert_eq!(unique.len(), all_partitions.len());
    }
}
