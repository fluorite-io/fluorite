//! Property-based tests for wire protocol encoding.

#![cfg(test)]

use bytes::Bytes;
use proptest::prelude::*;
use turbine_common::ids::{Generation, Offset, PartitionId, ProducerId, SchemaId, SeqNum, TopicId};
use turbine_common::types::{Record, Segment, SegmentAck};

use crate::{consumer, producer, record, varint};

// ============ Varint Property Tests ============

proptest! {
    #![proptest_config(ProptestConfig::with_cases(10000))]

    #[test]
    fn prop_varint_i64_roundtrip(value: i64) {
        let mut buf = [0u8; 10];
        let encoded_len = varint::encode_i64(value, &mut buf);
        let (decoded, decoded_len) = varint::decode_i64(&buf[..encoded_len]).unwrap();
        prop_assert_eq!(decoded, value);
        prop_assert_eq!(decoded_len, encoded_len);
    }

    #[test]
    fn prop_varint_u64_roundtrip(value: u64) {
        let mut buf = [0u8; 10];
        let encoded_len = varint::encode_u64(value, &mut buf);
        let (decoded, decoded_len) = varint::decode_u64(&buf[..encoded_len]).unwrap();
        prop_assert_eq!(decoded, value);
        prop_assert_eq!(decoded_len, encoded_len);
    }

    #[test]
    fn prop_varint_i32_roundtrip(value: i32) {
        let mut buf = [0u8; 10];
        let encoded_len = varint::encode_i32(value, &mut buf);
        let (decoded, decoded_len) = varint::decode_i32(&buf[..encoded_len]).unwrap();
        prop_assert_eq!(decoded, value);
        prop_assert_eq!(decoded_len, encoded_len);
    }

    #[test]
    fn prop_varint_encoding_is_compact(value in 0i64..128) {
        // Values 0-127 should encode in 1 byte (zigzag doubles, so 0-63 -> 1 byte)
        let mut buf = [0u8; 10];
        let len = varint::encode_i64(value, &mut buf);
        // Zigzag: 0->0, 1->2, ..., 63->126 (1 byte), 64->128 (2 bytes)
        if value < 64 {
            prop_assert_eq!(len, 1);
        } else {
            prop_assert_eq!(len, 2);
        }
    }
}

// ============ Record Property Tests ============

fn arb_record() -> impl Strategy<Value = Record> {
    (
        prop::option::of(prop::collection::vec(any::<u8>(), 0..100)),
        prop::collection::vec(any::<u8>(), 0..1000),
    )
        .prop_map(|(key, value)| Record {
            key: key.map(Bytes::from),
            value: Bytes::from(value),
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn prop_record_roundtrip(rec in arb_record()) {
        let mut buf = vec![0u8; 2048];
        let encoded_len = record::encode(&rec, &mut buf);
        let (decoded, decoded_len) = record::decode(&buf[..encoded_len]).unwrap();
        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.key, rec.key);
        prop_assert_eq!(decoded.value, rec.value);
    }

    #[test]
    fn prop_record_encoded_size_accurate(rec in arb_record()) {
        let mut buf = vec![0u8; 2048];
        let actual_len = record::encode(&rec, &mut buf);
        let predicted_len = record::encoded_size(&rec);
        prop_assert_eq!(actual_len, predicted_len);
    }
}

// ============ Segment Property Tests ============

fn arb_segment() -> impl Strategy<Value = Segment> {
    (
        any::<u32>(),
        any::<u32>(),
        any::<u32>(),
        prop::collection::vec(arb_record(), 0..20),
    )
        .prop_map(|(topic_id, partition_id, schema_id, records)| Segment {
            topic_id: TopicId(topic_id),
            partition_id: PartitionId(partition_id),
            schema_id: SchemaId(schema_id),
            records,
        })
}

fn arb_segment_ack() -> impl Strategy<Value = SegmentAck> {
    (
        any::<u32>(),
        any::<u32>(),
        any::<u32>(),
        any::<u64>(),
        any::<u64>(),
    )
        .prop_map(
            |(topic_id, partition_id, schema_id, start_offset, end_offset)| SegmentAck {
                topic_id: TopicId(topic_id),
                partition_id: PartitionId(partition_id),
                schema_id: SchemaId(schema_id),
                start_offset: Offset(start_offset),
                end_offset: Offset(end_offset),
            },
        )
}

// ============ ProduceRequest/Response Property Tests ============

fn arb_produce_request() -> impl Strategy<Value = producer::ProduceRequest> {
    (
        prop::collection::vec(any::<u8>(), 16..=16), // UUID bytes
        any::<u64>(),
        prop::collection::vec(arb_segment(), 0..5),
    )
        .prop_map(|(uuid_bytes, seq, segments)| {
            let uuid = uuid::Uuid::from_bytes(uuid_bytes.try_into().unwrap());
            producer::ProduceRequest {
                producer_id: ProducerId(uuid),
                seq: SeqNum(seq),
                segments,
            }
        })
}

fn arb_produce_response() -> impl Strategy<Value = producer::ProduceResponse> {
    (any::<u64>(), prop::collection::vec(arb_segment_ack(), 0..10)).prop_map(|(seq, acks)| {
        producer::ProduceResponse {
            seq: SeqNum(seq),
            acks,
        }
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_produce_request_roundtrip(req in arb_produce_request()) {
        let mut buf = vec![0u8; 64 * 1024]; // 64KB buffer
        let encoded_len = producer::encode_request(&req, &mut buf);
        let (decoded, decoded_len) = producer::decode_request(&buf[..encoded_len]).unwrap();

        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.producer_id.0, req.producer_id.0);
        prop_assert_eq!(decoded.seq.0, req.seq.0);
        prop_assert_eq!(decoded.segments.len(), req.segments.len());
    }

    #[test]
    fn prop_produce_response_roundtrip(resp in arb_produce_response()) {
        let mut buf = vec![0u8; 8 * 1024];
        let encoded_len = producer::encode_response(&resp, &mut buf);
        let (decoded, decoded_len) = producer::decode_response(&buf[..encoded_len]).unwrap();

        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.seq.0, resp.seq.0);
        prop_assert_eq!(decoded.acks.len(), resp.acks.len());
    }
}

// ============ Consumer Message Property Tests ============

fn arb_fetch_request() -> impl Strategy<Value = consumer::FetchRequest> {
    (
        "[a-z]{1,20}",
        "[a-z0-9]{1,30}",
        any::<u64>(),
        prop::collection::vec(
            (any::<u32>(), any::<u32>(), any::<u64>(), any::<u32>()),
            0..10,
        ),
    )
        .prop_map(|(group_id, consumer_id, generation, fetches)| consumer::FetchRequest {
            group_id,
            consumer_id,
            generation: Generation(generation),
            fetches: fetches
                .into_iter()
                .map(|(tid, pid, offset, max_bytes)| consumer::PartitionFetch {
                    topic_id: TopicId(tid),
                    partition_id: PartitionId(pid),
                    offset: Offset(offset),
                    max_bytes,
                })
                .collect(),
        })
}

fn arb_partition_result() -> impl Strategy<Value = consumer::PartitionResult> {
    (
        any::<u32>(),
        any::<u32>(),
        any::<u32>(),
        any::<u64>(),
        prop::collection::vec(arb_record(), 0..10),
    )
        .prop_map(
            |(topic_id, partition_id, schema_id, high_watermark, records)| {
                consumer::PartitionResult {
                    topic_id: TopicId(topic_id),
                    partition_id: PartitionId(partition_id),
                    schema_id: SchemaId(schema_id),
                    high_watermark: Offset(high_watermark),
                    records,
                }
            },
        )
}

fn arb_fetch_response() -> impl Strategy<Value = consumer::FetchResponse> {
    prop::collection::vec(arb_partition_result(), 0..5)
        .prop_map(|results| consumer::FetchResponse { results })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_fetch_request_roundtrip(req in arb_fetch_request()) {
        let mut buf = vec![0u8; 8 * 1024];
        let encoded_len = consumer::encode_fetch_request(&req, &mut buf);
        let (decoded, decoded_len) = consumer::decode_fetch_request(&buf[..encoded_len]).unwrap();

        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.group_id, req.group_id);
        prop_assert_eq!(decoded.consumer_id, req.consumer_id);
        prop_assert_eq!(decoded.generation.0, req.generation.0);
        prop_assert_eq!(decoded.fetches.len(), req.fetches.len());
    }

    #[test]
    fn prop_fetch_response_roundtrip(resp in arb_fetch_response()) {
        let mut buf = vec![0u8; 64 * 1024];
        let encoded_len = consumer::encode_fetch_response(&resp, &mut buf);
        let (decoded, decoded_len) = consumer::decode_fetch_response(&buf[..encoded_len]).unwrap();

        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.results.len(), resp.results.len());
    }
}

// ============ Consumer Group Messages ============

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_join_group_roundtrip(
        group_id in "[a-z]{1,20}",
        consumer_id in "[a-z0-9]{1,30}",
        topic_ids in prop::collection::vec(any::<u32>(), 0..10)
    ) {
        let req = consumer::JoinGroupRequest {
            group_id: group_id.clone(),
            consumer_id: consumer_id.clone(),
            topic_ids: topic_ids.iter().map(|&id| TopicId(id)).collect(),
        };

        let mut buf = vec![0u8; 1024];
        let encoded_len = consumer::encode_join_request(&req, &mut buf);
        let (decoded, decoded_len) = consumer::decode_join_request(&buf[..encoded_len]).unwrap();

        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.group_id, group_id);
        prop_assert_eq!(decoded.consumer_id, consumer_id);
        prop_assert_eq!(decoded.topic_ids.len(), topic_ids.len());
    }

    #[test]
    fn prop_heartbeat_roundtrip(
        group_id in "[a-z]{1,20}",
        topic_id: u32,
        consumer_id in "[a-z0-9]{1,30}",
        generation: u64
    ) {
        let req = consumer::HeartbeatRequest {
            group_id: group_id.clone(),
            topic_id: TopicId(topic_id),
            consumer_id: consumer_id.clone(),
            generation: Generation(generation),
        };

        let mut buf = vec![0u8; 256];
        let encoded_len = consumer::encode_heartbeat_request(&req, &mut buf);
        let (decoded, decoded_len) = consumer::decode_heartbeat_request(&buf[..encoded_len]).unwrap();

        prop_assert_eq!(decoded_len, encoded_len);
        prop_assert_eq!(decoded.group_id, group_id);
        prop_assert_eq!(decoded.topic_id.0, topic_id);
        prop_assert_eq!(decoded.consumer_id, consumer_id);
        prop_assert_eq!(decoded.generation.0, generation);
    }

    #[test]
    fn prop_heartbeat_response_roundtrip(rebalance_needed: bool) {
        let resp = consumer::HeartbeatResponse { rebalance_needed };

        let mut buf = vec![0u8; 8];
        let encoded_len = consumer::encode_heartbeat_response(&resp, &mut buf);
        let (decoded, _) = consumer::decode_heartbeat_response(&buf[..encoded_len]).unwrap();

        prop_assert_eq!(decoded.rebalance_needed, rebalance_needed);
    }
}
