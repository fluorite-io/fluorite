-- Schema registry: stores Avro schemas with content-addressed deduplication

-- Schemas table: immutable, content-addressed by hash
CREATE TABLE schemas (
    schema_id SERIAL PRIMARY KEY,
    schema_hash BYTEA UNIQUE NOT NULL,  -- SHA-256 of canonical schema
    schema_json JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Reserve IDs 1-99 for protocol schemas
ALTER SEQUENCE schemas_schema_id_seq RESTART WITH 100;

-- Topic-schema association: which schemas are valid for a topic
CREATE TABLE topic_schemas (
    topic_id INT NOT NULL REFERENCES topics(topic_id) ON DELETE CASCADE,
    schema_id INT NOT NULL REFERENCES schemas(schema_id) ON DELETE RESTRICT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (topic_id, schema_id)
);

-- Index for looking up schemas by topic in order of registration
CREATE INDEX idx_topic_schemas_by_time
ON topic_schemas (topic_id, created_at DESC);

-- Insert protocol schemas (reserved IDs 1-8)
INSERT INTO schemas (schema_id, schema_hash, schema_json) VALUES
(1, decode('0000000000000000000000000000000000000000000000000000000000000001', 'hex'),
   '{"type":"record","name":"ProduceRequest","fields":[]}'),
(2, decode('0000000000000000000000000000000000000000000000000000000000000002', 'hex'),
   '{"type":"record","name":"ProduceResponse","fields":[]}'),
(3, decode('0000000000000000000000000000000000000000000000000000000000000003', 'hex'),
   '{"type":"record","name":"FetchRequest","fields":[]}'),
(4, decode('0000000000000000000000000000000000000000000000000000000000000004', 'hex'),
   '{"type":"record","name":"FetchResponse","fields":[]}'),
(5, decode('0000000000000000000000000000000000000000000000000000000000000005', 'hex'),
   '{"type":"record","name":"Error","fields":[]}'),
(6, decode('0000000000000000000000000000000000000000000000000000000000000006', 'hex'),
   '{"type":"record","name":"Ping","fields":[]}'),
(7, decode('0000000000000000000000000000000000000000000000000000000000000007', 'hex'),
   '{"type":"record","name":"Pong","fields":[]}'),
(8, decode('0000000000000000000000000000000000000000000000000000000000000008', 'hex'),
   '{"type":"record","name":"RateLimit","fields":[]}');

-- Reset sequence to start user schemas at 100
SELECT setval('schemas_schema_id_seq', 100, false);
