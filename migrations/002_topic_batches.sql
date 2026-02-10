-- Topic batches: segment index (partitioned by ingest_time for efficient retention)
CREATE TABLE topic_batches (
    batch_id BIGSERIAL,
    topic_id INT NOT NULL,
    partition_id INT NOT NULL,
    schema_id INT NOT NULL,
    start_offset BIGINT NOT NULL,
    end_offset BIGINT NOT NULL,
    record_count INT NOT NULL,
    s3_key VARCHAR(512) NOT NULL,
    byte_offset BIGINT NOT NULL,
    byte_length BIGINT NOT NULL,
    ingest_time TIMESTAMPTZ NOT NULL,
    crc32 BIGINT NOT NULL,
    PRIMARY KEY (batch_id, ingest_time)
) PARTITION BY RANGE (ingest_time);

-- Create initial partition for today and next 7 days
DO $$
DECLARE
    start_date DATE := CURRENT_DATE;
    end_date DATE;
    partition_name TEXT;
BEGIN
    FOR i IN 0..7 LOOP
        end_date := start_date + INTERVAL '1 day';
        partition_name := 'topic_batches_' || TO_CHAR(start_date, 'YYYYMMDD');

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF topic_batches
             FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_date,
            end_date
        );

        start_date := end_date;
    END LOOP;
END $$;

-- Index for fetch queries: find batches by topic/partition/offset
CREATE INDEX idx_topic_batches_fetch
ON topic_batches (topic_id, partition_id, end_offset, ingest_time);

-- Index for S3 key lookups (used by GC)
CREATE INDEX idx_topic_batches_s3_key
ON topic_batches (s3_key);

-- Function to create daily partitions (run via cron/pg_cron)
CREATE OR REPLACE FUNCTION create_topic_batches_partition(target_date DATE)
RETURNS VOID AS $$
DECLARE
    partition_name TEXT;
    start_ts TIMESTAMPTZ;
    end_ts TIMESTAMPTZ;
BEGIN
    partition_name := 'topic_batches_' || TO_CHAR(target_date, 'YYYYMMDD');
    start_ts := target_date::TIMESTAMPTZ;
    end_ts := (target_date + INTERVAL '1 day')::TIMESTAMPTZ;

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF topic_batches
         FOR VALUES FROM (%L) TO (%L)',
        partition_name,
        start_ts,
        end_ts
    );
END;
$$ LANGUAGE plpgsql;
