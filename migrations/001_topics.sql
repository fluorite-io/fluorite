-- Topics table
CREATE TABLE topics (
    topic_id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    partition_count INT NOT NULL DEFAULT 1,
    retention_hours INT NOT NULL DEFAULT 168,  -- 7 days default
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for name lookups
CREATE INDEX idx_topics_name ON topics (name);

-- Partition offsets: tracks the next offset for each partition
CREATE TABLE partition_offsets (
    topic_id INT NOT NULL REFERENCES topics(topic_id) ON DELETE CASCADE,
    partition_id INT NOT NULL,
    next_offset BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (topic_id, partition_id)
);

-- Function to auto-create partitions when a topic is created
CREATE OR REPLACE FUNCTION create_topic_partitions()
RETURNS TRIGGER AS $$
BEGIN
    FOR i IN 0..(NEW.partition_count - 1) LOOP
        INSERT INTO partition_offsets (topic_id, partition_id, next_offset)
        VALUES (NEW.topic_id, i, 0);
    END LOOP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_create_topic_partitions
AFTER INSERT ON topics
FOR EACH ROW
EXECUTE FUNCTION create_topic_partitions();
