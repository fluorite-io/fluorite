//! Producer deduplication cache.
//!
//! LRU cache for tracking producer sequence numbers. Provides fast
//! deduplication checks without hitting the database for every request.

use std::collections::HashMap;

use sqlx::PgPool;
use tokio::sync::RwLock;
use turbine_common::ids::{ProducerId, SeqNum};
use turbine_common::types::SegmentAck;

/// Cached producer state.
#[derive(Debug, Clone)]
pub struct ProducerState {
    /// Last acknowledged sequence number.
    pub last_seq: SeqNum,
    /// Acks for the last sequence (for duplicate requests).
    pub last_acks: Vec<SegmentAck>,
}

/// Result of dedup check.
#[derive(Debug)]
pub enum DedupResult {
    /// New request, should be processed.
    Accept,
    /// Duplicate request, return cached acks.
    Duplicate(Vec<SegmentAck>),
    /// Sequence too old (already processed higher seq).
    Stale,
}

/// Configuration for dedup cache.
#[derive(Debug, Clone)]
pub struct DedupCacheConfig {
    /// Maximum number of producers to cache.
    pub max_entries: usize,
}

impl Default for DedupCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 100_000,
        }
    }
}

/// LRU dedup cache backed by database.
pub struct DedupCache {
    /// In-memory cache.
    cache: RwLock<LruCache>,
    /// Database pool for cache misses.
    pool: PgPool,
    /// Configuration.
    config: DedupCacheConfig,
}

/// Simple LRU cache implementation.
struct LruCache {
    map: HashMap<ProducerId, (ProducerState, u64)>,
    order: u64,
    max_entries: usize,
}

impl LruCache {
    fn new(max_entries: usize) -> Self {
        Self {
            map: HashMap::with_capacity(max_entries),
            order: 0,
            max_entries,
        }
    }

    fn get(&mut self, key: &ProducerId) -> Option<&ProducerState> {
        if let Some((state, order)) = self.map.get_mut(key) {
            self.order += 1;
            *order = self.order;
            Some(state)
        } else {
            None
        }
    }

    fn insert(&mut self, key: ProducerId, state: ProducerState) {
        self.order += 1;

        // Evict oldest if at capacity
        if self.map.len() >= self.max_entries {
            self.evict_oldest();
        }

        self.map.insert(key, (state, self.order));
    }

    fn evict_oldest(&mut self) {
        if let Some(oldest_key) = self
            .map
            .iter()
            .min_by_key(|(_, (_, order))| *order)
            .map(|(k, _)| *k)
        {
            self.map.remove(&oldest_key);
        }
    }
}

impl DedupCache {
    /// Create a new dedup cache.
    pub fn new(pool: PgPool) -> Self {
        Self::with_config(pool, DedupCacheConfig::default())
    }

    /// Create a new dedup cache with custom config.
    pub fn with_config(pool: PgPool, config: DedupCacheConfig) -> Self {
        Self {
            cache: RwLock::new(LruCache::new(config.max_entries)),
            pool,
            config,
        }
    }

    /// Check if a request is a duplicate.
    pub async fn check(
        &self,
        producer_id: ProducerId,
        seq: SeqNum,
    ) -> Result<DedupResult, sqlx::Error> {
        // Fast path: check cache
        {
            let mut cache = self.cache.write().await;
            if let Some(state) = cache.get(&producer_id) {
                if seq.0 == state.last_seq.0 {
                    return Ok(DedupResult::Duplicate(state.last_acks.clone()));
                } else if seq.0 < state.last_seq.0 {
                    return Ok(DedupResult::Stale);
                } else {
                    return Ok(DedupResult::Accept);
                }
            }
        }

        // Slow path: check database
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT last_seq_num FROM producer_state WHERE producer_id = $1",
        )
        .bind(producer_id.0)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some((last_seq,)) => {
                if seq.0 == last_seq as u64 {
                    // Duplicate - need to fetch acks from somewhere
                    // For now, return stale (proper impl would store acks)
                    Ok(DedupResult::Stale)
                } else if seq.0 < last_seq as u64 {
                    Ok(DedupResult::Stale)
                } else {
                    Ok(DedupResult::Accept)
                }
            }
            None => Ok(DedupResult::Accept),
        }
    }

    /// Update cache after successful commit.
    pub async fn update(&self, producer_id: ProducerId, seq: SeqNum, acks: Vec<SegmentAck>) {
        let mut cache = self.cache.write().await;
        cache.insert(
            producer_id,
            ProducerState {
                last_seq: seq,
                last_acks: acks,
            },
        );
    }

    /// Get cache stats.
    pub async fn stats(&self) -> CacheStats {
        let cache = self.cache.read().await;
        CacheStats {
            entries: cache.map.len(),
            max_entries: self.config.max_entries,
        }
    }
}

/// Cache statistics.
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub entries: usize,
    pub max_entries: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use turbine_common::ids::{Offset, PartitionId, SchemaId, TopicId};

    fn make_ack() -> SegmentAck {
        SegmentAck {
            topic_id: TopicId(1),
            partition_id: PartitionId(0),
            schema_id: SchemaId(100),
            start_offset: Offset(0),
            end_offset: Offset(10),
        }
    }

    #[test]
    fn test_lru_cache_insert_get() {
        let mut cache = LruCache::new(10);
        let producer = ProducerId(uuid::Uuid::new_v4());
        let state = ProducerState {
            last_seq: SeqNum(42),
            last_acks: vec![make_ack()],
        };

        cache.insert(producer, state.clone());

        let retrieved = cache.get(&producer).unwrap();
        assert_eq!(retrieved.last_seq.0, 42);
    }

    #[test]
    fn test_lru_cache_eviction() {
        let mut cache = LruCache::new(2);
        let p1 = ProducerId(uuid::Uuid::new_v4());
        let p2 = ProducerId(uuid::Uuid::new_v4());
        let p3 = ProducerId(uuid::Uuid::new_v4());

        cache.insert(p1, ProducerState { last_seq: SeqNum(1), last_acks: vec![] });
        cache.insert(p2, ProducerState { last_seq: SeqNum(2), last_acks: vec![] });

        // Access p1 to make it more recent
        let _ = cache.get(&p1);

        // Insert p3 - should evict p2 (oldest)
        cache.insert(p3, ProducerState { last_seq: SeqNum(3), last_acks: vec![] });

        assert!(cache.get(&p1).is_some());
        assert!(cache.get(&p2).is_none()); // Evicted
        assert!(cache.get(&p3).is_some());
    }

    #[test]
    fn test_lru_cache_update_order() {
        let mut cache = LruCache::new(2);
        let p1 = ProducerId(uuid::Uuid::new_v4());
        let p2 = ProducerId(uuid::Uuid::new_v4());

        cache.insert(p1, ProducerState { last_seq: SeqNum(1), last_acks: vec![] });
        cache.insert(p2, ProducerState { last_seq: SeqNum(2), last_acks: vec![] });

        // Access p1 multiple times
        let _ = cache.get(&p1);
        let _ = cache.get(&p1);

        // p1 should now be most recent, p2 should be evicted on next insert
        let p3 = ProducerId(uuid::Uuid::new_v4());
        cache.insert(p3, ProducerState { last_seq: SeqNum(3), last_acks: vec![] });

        assert!(cache.get(&p1).is_some());
        assert!(cache.get(&p2).is_none());
        assert!(cache.get(&p3).is_some());
    }
}
