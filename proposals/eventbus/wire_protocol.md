# Wire Protocol

WebSocket + Avro with zigzag varint encoding.

---

## Overview

```
Producer SDK ◄──── WebSocket ────► Agent ◄──── WebSocket ────► Consumer SDK
                 (Avro binary)              (Avro binary)
```

- All messages are Avro-encoded (schemaless, compact binary)
- Zigzag varints for integers (small values = small bytes)
- Schema IDs reference schema registry
- Producer SDK batches internally, sends segments

---

## Message Framing

Each WebSocket message is a single Avro-encoded value. Message type determined by endpoint + direction.

| Endpoint | Client → Agent | Agent → Client |
|----------|----------------|----------------|
| `/v1/producer` | ProduceRequest | ProduceResponse \| Error \| RateLimit |
| `/v1/consumer` | FetchRequest | FetchResponse \| Error |
| both | Ping | Pong |

No envelope needed — context determines schema.

---

## Schemas

### ProduceRequest

```avro
{
  "type": "record",
  "name": "ProduceRequest",
  "fields": [
    {"name": "msg_id", "type": "long"},
    {"name": "producer_id", "type": {"type": "fixed", "size": 16, "name": "UUID"}},
    {"name": "seq_num", "type": "long"},
    {"name": "segments", "type": {"type": "array", "items": {
      "type": "record",
      "name": "Segment",
      "fields": [
        {"name": "topic_id", "type": "int"},
        {"name": "partition_id", "type": "int"},
        {"name": "schema_id", "type": "int"},
        {"name": "records", "type": {"type": "array", "items": {
          "type": "record",
          "name": "Record",
          "fields": [
            {"name": "key", "type": ["null", "bytes"], "default": null},
            {"name": "value", "type": "bytes"}
          ]
        }}}
      ]
    }}}
  ]
}
```

**Wire size (zigzag varints):**
- `msg_id`: 1-10 bytes (typically 1-2)
- `producer_id`: 16 bytes (fixed)
- `seq_num`: 1-10 bytes (typically 1-3)
- `topic_id`, `partition_id`, `schema_id`: 1-5 bytes each (typically 1-2)
- `record_count`: 1-5 bytes (array length prefix)
- `key`: 1 byte (null) or length + bytes
- `value`: length (varint) + bytes

### ProduceResponse

```avro
{
  "type": "record",
  "name": "ProduceResponse",
  "fields": [
    {"name": "msg_id", "type": "long"},
    {"name": "segment_acks", "type": {"type": "array", "items": {
      "type": "record",
      "name": "SegmentAck",
      "fields": [
        {"name": "topic_id", "type": "int"},
        {"name": "partition_id", "type": "int"},
        {"name": "schema_id", "type": "int"},
        {"name": "start_offset", "type": "long"},
        {"name": "end_offset", "type": "long"}
      ]
    }}}
  ]
}
```

### FetchRequest

```avro
{
  "type": "record",
  "name": "FetchRequest",
  "fields": [
    {"name": "msg_id", "type": "long"},
    {"name": "topic_id", "type": "int"},
    {"name": "partition_id", "type": "int"},
    {"name": "start_offset", "type": "long"},
    {"name": "max_bytes", "type": "int", "default": 1048576}
  ]
}
```

### FetchResponse

```avro
{
  "type": "record",
  "name": "FetchResponse",
  "fields": [
    {"name": "msg_id", "type": "long"},
    {"name": "topic_id", "type": "int"},
    {"name": "partition_id", "type": "int"},
    {"name": "next_offset", "type": "long"},
    {"name": "segments", "type": {"type": "array", "items": {
      "type": "record",
      "name": "FetchedSegment",
      "fields": [
        {"name": "schema_id", "type": "int"},
        {"name": "start_offset", "type": "long"},
        {"name": "end_offset", "type": "long"},
        {"name": "records", "type": {"type": "array", "items": "Record"}}
      ]
    }}}
  ]
}
```

### Error

```avro
{
  "type": "record",
  "name": "Error",
  "fields": [
    {"name": "msg_id", "type": "long"},
    {"name": "code", "type": "int"},
    {"name": "message", "type": "string"},
    {"name": "retryable", "type": "boolean"}
  ]
}
```

| Code | Name | Retryable |
|------|------|-----------|
| 1 | INVALID_REQUEST | no |
| 2 | TOPIC_NOT_FOUND | no |
| 3 | SCHEMA_NOT_FOUND | no |
| 4 | INVALID_SEQUENCE | no |
| 5 | RATE_LIMITED | yes |
| 6 | INTERNAL_ERROR | yes |
| 7 | SHUTTING_DOWN | yes |

### RateLimit

```avro
{
  "type": "record",
  "name": "RateLimit",
  "fields": [
    {"name": "msg_id", "type": "long"},
    {"name": "delay_ms", "type": "int"},
    {"name": "reason", "type": "string"}
  ]
}
```

### Ping / Pong

```avro
{"type": "record", "name": "Ping", "fields": [{"name": "msg_id", "type": "long"}]}
{"type": "record", "name": "Pong", "fields": [{"name": "msg_id", "type": "long"}]}
```

---

## Response Discrimination

Agent responses on producer endpoint can be: ProduceResponse, Error, or RateLimit.

**Option A: First-byte tag**

```
┌──────────┬─────────────────────────┐
│ tag (1B) │ Avro payload            │
└──────────┴─────────────────────────┘

tag = 0: ProduceResponse
tag = 1: Error
tag = 2: RateLimit
tag = 3: Pong
```

**Option B: Union type**

```avro
{
  "type": "record",
  "name": "ProducerMessage",
  "fields": [
    {"name": "msg_id", "type": "long"},
    {"name": "body", "type": ["ProduceResponse", "Error", "RateLimit", "Pong"]}
  ]
}
```

Union adds 1-2 bytes for index. Either approach works — Option A is slightly more efficient, Option B is pure Avro.

**Recommendation:** Option B (union) — keeps everything in Avro, easier tooling.

---

## Connection Lifecycle

### Endpoints

```
wss://agent.example.com/v1/producer
wss://agent.example.com/v1/consumer
```

### Handshake

```
GET /v1/producer HTTP/1.1
Host: agent.example.com
Upgrade: websocket
Connection: Upgrade
Authorization: Bearer <token>
```

### Keep-Alive

- Client sends Ping every 30s if idle
- Agent responds with Pong (matching msg_id)
- Close if no Pong within 10s

---

## Producer SDK

### User API

```typescript
const producer = new Producer({
  endpoints: ['wss://agent1', 'wss://agent2'],
  registry: 'https://schema-registry',
});

// Simple send — SDK batches internally
await producer.send('orders', { key: userId, value: orderBytes, schemaId: 100 });
await producer.send('orders', { key: visitorId, value: orderBytes, schemaId: 100 });

// Explicit flush (or auto-flush after lingerMs)
const acks = await producer.flush();
```

### Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `lingerMs` | 100 | Max time to buffer before flush |
| `batchMaxBytes` | 1 MB | Flush when buffer exceeds this |
| `batchMaxRecords` | 10,000 | Flush when records exceed this |
| `requestTimeoutMs` | 30,000 | Timeout waiting for response |
| `maxRetries` | 3 | Retries for retryable errors |

### Internal Flow

```
send(topic, record)
    │
    ▼
┌─────────────────────────────────────┐
│ Buffer: Map<(topic,part,schema), []>│
│                                     │
│ (orders,0,100) → [r1, r2, r3]       │
│ (orders,1,100) → [r4, r5]           │
│ (clicks,0,200) → [r6]               │
└─────────────────────────────────────┘
    │
    │ (after 100ms or size limit)
    ▼
flush()
    │
    ▼
┌─────────────────────────────────────┐
│ ProduceRequest {                    │
│   msg_id: 1,                        │
│   producer_id: <uuid>,              │
│   seq_num: 42,                      │
│   segments: [                       │
│     {topic:1, part:0, schema:100,   │
│      records: [r1,r2,r3]},          │
│     {topic:1, part:1, schema:100,   │
│      records: [r4,r5]},             │
│     {topic:2, part:0, schema:200,   │
│      records: [r6]}                 │
│   ]                                 │
│ }                                   │
└─────────────────────────────────────┘
    │
    │ WebSocket send (Avro binary)
    ▼
   Agent
```

### Exactly-Once

```
Producer                                          Agent
    │                                               │
    │─── ProduceRequest(seq=42, segments) ─────────►│
    │                                               │
    │         (connection drops)                    │
    │                                               │
    │─── reconnect ────────────────────────────────►│
    │                                               │
    │─── ProduceRequest(seq=42, segments) ─────────►│  (same seq!)
    │                                               │
    │◄── ProduceResponse(acks from first attempt) ──│  (deduped)
    │                                               │
```

- SDK persists `seq_num` to disk (or localStorage in browser)
- On reconnect, retry with same `seq_num`
- Agent returns stored acks for duplicate seq

---

## Consumer SDK

### User API

```typescript
const consumer = new Consumer({
  endpoints: ['wss://agent1', 'wss://agent2'],
  topic: 'orders',
  partition: 0,
  startOffset: 'latest', // or 'earliest' or specific number
});

for await (const record of consumer) {
  console.log(record.offset, record.key, record.value);
  await consumer.commit(); // optional: commit offset
}
```

### Internal Flow

```
poll()
    │
    ▼
┌─────────────────────────────────────┐
│ FetchRequest {                      │
│   msg_id: 1,                        │
│   topic_id: 1,                      │
│   partition_id: 0,                  │
│   start_offset: 100,                │
│   max_bytes: 1048576                │
│ }                                   │
└─────────────────────────────────────┘
    │
    │ WebSocket send
    ▼
   Agent
    │
    │ WebSocket recv
    ▼
┌─────────────────────────────────────┐
│ FetchResponse {                     │
│   msg_id: 1,                        │
│   topic_id: 1,                      │
│   partition_id: 0,                  │
│   next_offset: 250,                 │
│   segments: [                       │
│     {schema:100, start:100, end:149,│
│      records: [...]},               │
│     {schema:101, start:150, end:199,│
│      records: [...]},               │
│     {schema:100, start:200, end:249,│
│      records: [...]}                │
│   ]                                 │
│ }                                   │
└─────────────────────────────────────┘
    │
    ▼
yield records one by one
```

---

## Wire Examples

### ProduceRequest (100 records, 1 segment)

```
Bytes breakdown (approximate):
  msg_id:        1 byte  (varint, small number)
  producer_id:  16 bytes (fixed UUID)
  seq_num:       2 bytes (varint)
  segments array length: 1 byte
  segment:
    topic_id:    1 byte
    partition_id: 1 byte
    schema_id:   2 bytes
    records array length: 2 bytes (varint for 100)
    records:
      100 × (1 byte null-key + varint length + value bytes)

Total overhead: ~25 bytes + record data
```

### ProduceResponse (1 segment ack)

```
  msg_id:      1 byte
  acks array:  1 byte (length)
  ack:
    topic_id:    1 byte
    partition_id: 1 byte
    schema_id:   2 bytes
    start_offset: 3 bytes (varint)
    end_offset:  3 bytes (varint)

Total: ~13 bytes
```

---

## Error Handling

### Retryable Errors

```
Producer                                          Agent
    │                                               │
    │─── ProduceRequest ───────────────────────────►│
    │                                               │
    │◄── Error(code=6, INTERNAL_ERROR, retry=true) ─│
    │                                               │
    │    (wait backoff)                             │
    │                                               │
    │─── ProduceRequest (same seq_num) ────────────►│
    │                                               │
    │◄── ProduceResponse ───────────────────────────│
```

### Non-Retryable Errors

```
Producer                                          Agent
    │                                               │
    │─── ProduceRequest(topic=999) ────────────────►│
    │                                               │
    │◄── Error(code=2, TOPIC_NOT_FOUND, retry=false)│
    │                                               │
    │    (throw to user)                            │
```

### Backpressure

```
Producer                                          Agent
    │                                               │
    │─── ProduceRequest ───────────────────────────►│
    │                                               │
    │◄── RateLimit(delay_ms=500, "buffer full") ────│
    │                                               │
    │    (SDK waits 500ms, then retries)            │
    │                                               │
    │─── ProduceRequest (same msg_id) ─────────────►│
    │                                               │
    │◄── ProduceResponse ───────────────────────────│
```

---

## Load Balancing

```
            ┌─────────────────┐
            │  L7 Load Balancer│
            │  (WebSocket)    │
            └────────┬────────┘
    ┌────────────────┼────────────────┐
    ▼                ▼                ▼
┌───────┐       ┌───────┐       ┌───────┐
│Agent 1│       │Agent 2│       │Agent 3│
└───────┘       └───────┘       └───────┘
```

- Least-connections strategy
- No sticky sessions needed
- Any agent handles any partition

---

## Libraries

| Language | WebSocket | Avro |
|----------|-----------|------|
| Java | Netty / java-websocket | Apache Avro |
| Python | websockets | fastavro |
| JavaScript | native WebSocket / ws | avsc |

---

## Summary

| Aspect | Choice |
|--------|--------|
| Transport | WebSocket |
| Encoding | Avro (schemaless, zigzag varints) |
| Message discrimination | Union type (pure Avro) |
| Producer batching | SDK-internal (100ms / 1MB) |
| Agent batching | 400ms / 10MB |
| Exactly-once | seq_num per producer batch |
| Request correlation | msg_id field |
