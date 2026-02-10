-- Producer state: tracks last committed sequence for deduplication
CREATE TABLE producer_state (
    producer_id UUID PRIMARY KEY,
    last_seq BIGINT NOT NULL,
    last_acks JSONB NOT NULL,  -- Array of SegmentAck for last commit
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for cleanup of stale producers
CREATE INDEX idx_producer_state_updated_at
ON producer_state (updated_at);

-- Function to clean up stale producer state (run via cron)
CREATE OR REPLACE FUNCTION cleanup_stale_producers(retention_hours INT DEFAULT 24)
RETURNS INT AS $$
DECLARE
    deleted_count INT;
BEGIN
    DELETE FROM producer_state
    WHERE updated_at < NOW() - (retention_hours || ' hours')::INTERVAL;

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;
