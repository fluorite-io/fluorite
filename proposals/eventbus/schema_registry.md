# Schema Registry

Centralized schema storage with evolution support.

---

## Overview

```
Producer ──► Registry ◄── Consumer
              │
              ▼
           Postgres
```

**Responsibilities:**
- Store Avro schemas (content-addressed by hash)
- Assign unique schema IDs
- Enforce compatibility on schema evolution per topic
- Serve schemas by ID

---

## Data Model

```sql
-- Schemas: immutable, content-addressed
CREATE TABLE schemas (
    schema_id SERIAL PRIMARY KEY,
    schema_hash BYTEA UNIQUE NOT NULL,       -- SHA-256 of canonical schema
    schema_json JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- Topic-schema association: which schemas are valid for a topic
CREATE TABLE topic_schemas (
    topic_id INT NOT NULL REFERENCES topics(topic_id),
    schema_id INT NOT NULL REFERENCES schemas(schema_id),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (topic_id, schema_id)
);

CREATE INDEX idx_topic_schemas_by_time
ON topic_schemas (topic_id, created_at DESC);
```

**Schema deduplication:** Same schema content → same `schema_id`, regardless of which topic registers it.

---

## API

### Register Schema for Topic

```
POST /topics/{topic}/schemas
Content-Type: application/json

{
  "schema": {
    "type": "record",
    "name": "Order",
    "fields": [
      {"name": "order_id", "type": "string"},
      {"name": "amount", "type": "long"}
    ]
  }
}
```

**Response (success):**
```json
{
  "schema_id": 100
}
```

**Response (incompatible):**
```json
{
  "error": "INCOMPATIBLE_SCHEMA",
  "message": "Field 'amount' type changed from 'long' to 'string'"
}
```

**Behavior:**
1. Canonicalize and hash schema
2. If schema exists → get existing `schema_id`
3. If schema is new → insert, get new `schema_id`
4. Get latest schema for topic (by `created_at`)
5. If latest exists → check compatibility
6. If compatible → insert into `topic_schemas`
7. Return `schema_id`

### Get Schema by ID

```
GET /schemas/{schema_id}
```

**Response:**
```json
{
  "schema_id": 100,
  "schema": {
    "type": "record",
    "name": "Order",
    "fields": [...]
  }
}
```

### List Schemas for Topic

```
GET /topics/{topic}/schemas
```

**Response:**
```json
{
  "topic": "orders",
  "schemas": [
    {"schema_id": 100, "created_at": "2024-01-15T10:00:00Z"},
    {"schema_id": 101, "created_at": "2024-01-16T14:30:00Z"}
  ]
}
```

### Check Compatibility (dry-run)

```
POST /topics/{topic}/schemas/check
Content-Type: application/json

{
  "schema": {...}
}
```

**Response:**
```json
{
  "compatible": true
}
```

Or:
```json
{
  "compatible": false,
  "errors": ["Field 'amount' removed without default"]
}
```

---

## Compatibility Rules

**Mode: BACKWARD** (hardcoded for simplicity)

New schema must be able to read data written with the previous schema.

**Compatible changes:**
| Change | Example | OK? |
|--------|---------|-----|
| Add field with default | `{"name": "currency", "default": "USD"}` | ✅ |
| Remove field | Remove `shipping_address` | ✅ |
| Widen numeric | `int` → `long` | ✅ |
| Add union member | `["null", "string"]` → `["null", "string", "int"]` | ✅ |

**Incompatible changes:**
| Change | Example | OK? |
|--------|---------|-----|
| Add field without default | `{"name": "currency", "type": "string"}` | ❌ |
| Change field type | `"amount": "long"` → `"amount": "string"` | ❌ |
| Rename field | `order_id` → `orderId` | ❌ |
| Narrow numeric | `long` → `int` | ❌ |

---

## Schema Canonicalization

Before hashing, normalize the schema:
1. Sort object keys alphabetically
2. Remove whitespace
3. Remove `doc` fields (documentation doesn't affect compatibility)
4. Remove `default` from hash (same structure = same schema)

```python
def canonicalize(schema: dict) -> str:
    def normalize(obj):
        if isinstance(obj, dict):
            # Remove doc fields, sort keys
            return {k: normalize(v) for k, v in sorted(obj.items()) if k != 'doc'}
        elif isinstance(obj, list):
            return [normalize(item) for item in obj]
        else:
            return obj

    return json.dumps(normalize(schema), separators=(',', ':'))

def schema_hash(schema: dict) -> bytes:
    return hashlib.sha256(canonicalize(schema).encode()).digest()
```

---

## Registration Logic

```python
async def register_schema(topic_id: int, schema: dict) -> int:
    schema_hash = compute_hash(schema)

    async with db.transaction():
        # 1. Get or create schema
        existing = await db.fetchone(
            "SELECT schema_id FROM schemas WHERE schema_hash = $1",
            schema_hash
        )

        if existing:
            schema_id = existing['schema_id']
        else:
            schema_id = await db.fetchval(
                "INSERT INTO schemas (schema_hash, schema_json) VALUES ($1, $2) RETURNING schema_id",
                schema_hash, schema
            )

        # 2. Check if already registered for topic
        already_registered = await db.fetchone(
            "SELECT 1 FROM topic_schemas WHERE topic_id = $1 AND schema_id = $2",
            topic_id, schema_id
        )

        if already_registered:
            return schema_id  # Idempotent

        # 3. Get latest schema for topic
        latest = await db.fetchone(
            "SELECT schema_id, schema_json FROM topic_schemas ts "
            "JOIN schemas s USING (schema_id) "
            "WHERE ts.topic_id = $1 "
            "ORDER BY ts.created_at DESC LIMIT 1",
            topic_id
        )

        # 4. Check compatibility
        if latest:
            if not is_backward_compatible(new_schema=schema, old_schema=latest['schema_json']):
                raise IncompatibleSchemaError()

        # 5. Register
        await db.execute(
            "INSERT INTO topic_schemas (topic_id, schema_id) VALUES ($1, $2)",
            topic_id, schema_id
        )

        return schema_id
```

---

## Avro Compatibility Check

```python
from fastavro.schema import parse_schema
from fastavro._schema_py import match_schemas

def is_backward_compatible(new_schema: dict, old_schema: dict) -> bool:
    """
    Check if new_schema can read data written with old_schema.
    (BACKWARD compatibility)
    """
    try:
        new_parsed = parse_schema(new_schema)
        old_parsed = parse_schema(old_schema)

        # Try to create a resolver from old (writer) to new (reader)
        # This will fail if incompatible
        match_schemas(old_parsed, new_parsed)
        return True
    except Exception:
        return False
```

---

## Producer Integration

```python
class Producer:
    def __init__(self, registry_url: str):
        self.registry = RegistryClient(registry_url)
        self.schema_cache = {}  # hash -> schema_id

    async def send(self, topic: str, value: dict, schema: dict):
        schema_id = await self._get_schema_id(topic, schema)
        encoded = avro_encode(schema, value)
        self._buffer(topic, schema_id, encoded)

    async def _get_schema_id(self, topic: str, schema: dict) -> int:
        h = schema_hash(schema)
        if h in self.schema_cache:
            return self.schema_cache[h]

        result = await self.registry.register(topic, schema)
        self.schema_cache[h] = result.schema_id
        return result.schema_id
```

---

## Consumer Integration

```python
class Consumer:
    def __init__(self, registry_url: str, reader_schema: dict = None):
        self.registry = RegistryClient(registry_url)
        self.schema_cache = {}  # schema_id -> parsed schema
        self.reader_schema = parse_schema(reader_schema) if reader_schema else None

    async def decode(self, schema_id: int, data: bytes) -> dict:
        writer_schema = await self._get_schema(schema_id)

        if self.reader_schema:
            # Schema resolution: project writer schema to reader schema
            return avro_decode(data, writer_schema, self.reader_schema)
        else:
            # Use writer schema directly
            return avro_decode(data, writer_schema)

    async def _get_schema(self, schema_id: int):
        if schema_id in self.schema_cache:
            return self.schema_cache[schema_id]

        result = await self.registry.get_schema(schema_id)
        parsed = parse_schema(result.schema)
        self.schema_cache[schema_id] = parsed
        return parsed
```

---

## Schema Resolution Example

**Writer schema (v1):**
```json
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "long"}
  ]
}
```

**Writer schema (v2):**
```json
{
  "type": "record",
  "name": "Order",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "long"},
    {"name": "currency", "type": "string", "default": "USD"}
  ]
}
```

**Consumer with reader schema v2:**
- Reads v1 data → `currency` defaults to `"USD"`
- Reads v2 data → `currency` from data

**Consumer with reader schema v1:**
- Reads v1 data → works
- Reads v2 data → `currency` field ignored

---

## Reserved Schema IDs

Protocol schemas use reserved IDs (1-99):

| ID | Schema |
|----|--------|
| 1 | ProduceRequest |
| 2 | ProduceResponse |
| 3 | FetchRequest |
| 4 | FetchResponse |
| 5 | Error |
| 6 | Ping |
| 7 | Pong |
| 8 | RateLimit |

User schemas start at 100.

---

## Caching

**Client-side:**
- Cache schema by ID (immutable, never changes)
- Cache schema_id by hash (avoid re-registration)

**Server-side:**
- Cache parsed schemas
- Cache compatibility check results

---

## Example Flow

```
1. Producer starts with Order schema v1
   POST /topics/orders/schemas {"schema": {order_id, amount}}
   → schema_id: 100

2. Producer sends records with schema_id=100

3. Week later, add currency field
   POST /topics/orders/schemas {"schema": {order_id, amount, currency}}
   → Registry checks: can v2 read v1 data? Yes (currency has default)
   → schema_id: 101

4. Producer sends new records with schema_id=101

5. Consumer reads:
   - Old records (schema_id=100): decode with v1, currency="USD" (default)
   - New records (schema_id=101): decode with v2, currency=actual value
```

---

## Summary

| Aspect | Choice |
|--------|--------|
| Storage | Postgres |
| ID assignment | Auto-increment, deduplicated by hash |
| Compatibility | BACKWARD (new reads old) |
| Topic association | `topic_schemas` junction table |
| Ordering | `created_at` timestamp |
| Reserved IDs | 1-99 for protocol, 100+ for users |
