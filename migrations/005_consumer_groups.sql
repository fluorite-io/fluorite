-- Consumer groups: coordination for parallel consumption

-- Consumer groups table: one row per (group, topic) pair
CREATE TABLE consumer_groups (
    group_id VARCHAR(255) NOT NULL,
    topic_id INT NOT NULL REFERENCES topics(topic_id) ON DELETE CASCADE,
    generation BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, topic_id)
);

-- Consumer members: active consumers in a group
CREATE TABLE consumer_members (
    group_id VARCHAR(255) NOT NULL,
    topic_id INT NOT NULL,
    consumer_id VARCHAR(255) NOT NULL,
    agent_id UUID NOT NULL,  -- Which agent this consumer is connected to
    last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (group_id, topic_id, consumer_id),
    FOREIGN KEY (group_id, topic_id) REFERENCES consumer_groups(group_id, topic_id) ON DELETE CASCADE
);

-- Index for finding expired members
CREATE INDEX idx_consumer_members_heartbeat
ON consumer_members (last_heartbeat);

-- Consumer assignments: which partitions are assigned to which consumers
CREATE TABLE consumer_assignments (
    group_id VARCHAR(255) NOT NULL,
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    consumer_id VARCHAR(255),  -- NULL if unassigned
    generation BIGINT NOT NULL,
    committed_offset BIGINT NOT NULL DEFAULT 0,
    lease_expires_at TIMESTAMPTZ,
    PRIMARY KEY (group_id, topic_id, partition_id),
    FOREIGN KEY (group_id, topic_id) REFERENCES consumer_groups(group_id, topic_id) ON DELETE CASCADE
);

-- Index for finding assignments by consumer
CREATE INDEX idx_consumer_assignments_consumer
ON consumer_assignments (group_id, topic_id, consumer_id);

-- Index for finding expired leases
CREATE INDEX idx_consumer_assignments_lease
ON consumer_assignments (lease_expires_at)
WHERE lease_expires_at IS NOT NULL;

-- Function to initialize consumer group for a topic
CREATE OR REPLACE FUNCTION initialize_consumer_group(
    p_group_id VARCHAR(255),
    p_topic_id INT
) RETURNS VOID AS $$
DECLARE
    partition_count INT;
BEGIN
    -- Get partition count for the topic
    SELECT t.partition_count INTO partition_count
    FROM topics t
    WHERE t.topic_id = p_topic_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Topic % not found', p_topic_id;
    END IF;

    -- Create group row if not exists
    INSERT INTO consumer_groups (group_id, topic_id, generation)
    VALUES (p_group_id, p_topic_id, 1)
    ON CONFLICT (group_id, topic_id) DO NOTHING;

    -- Create assignment rows for each partition
    FOR i IN 0..(partition_count - 1) LOOP
        INSERT INTO consumer_assignments (group_id, topic_id, partition_id, consumer_id, generation, committed_offset)
        VALUES (p_group_id, p_topic_id, i, NULL, 1, 0)
        ON CONFLICT (group_id, topic_id, partition_id) DO NOTHING;
    END LOOP;
END;
$$ LANGUAGE plpgsql;
