BEGIN;
-- This migration must be applied while the CEX23.5 Kafka consumer is stopped.
-- Existing CEX23.5 candle revisions cannot be reconstructed as an event history.
-- The replay floor prevents claiming historical candle-update continuity.
CREATE TABLE IF NOT EXISTS market_data.stream_replay_floors (
    topic TEXT NOT NULL,
    source_market_id BIGINT NOT NULL CHECK (source_market_id BETWEEN 0 AND 4294967295),
    first_complete_sequence NUMERIC(20,0) NOT NULL CHECK (first_complete_sequence > 0),
    PRIMARY KEY (topic,source_market_id)
);
INSERT INTO market_data.stream_replay_floors(topic,source_market_id,first_complete_sequence)
SELECT topic, source_market_id, market_sequence + 1
FROM market_data.public_books
ON CONFLICT (topic,source_market_id) DO NOTHING;
CREATE TABLE IF NOT EXISTS market_data.candle_updates (
    topic TEXT NOT NULL,
    source_market_id BIGINT NOT NULL CHECK (source_market_id BETWEEN 0 AND 4294967295),
    market_sequence NUMERIC(20,0) NOT NULL CHECK (market_sequence > 0),
    interval_id INTEGER NOT NULL CHECK (interval_id BETWEEN 1 AND 5),
    start_ns NUMERIC(30,0) NOT NULL,
    payload BYTEA NOT NULL,
    PRIMARY KEY(topic,source_market_id,market_sequence,interval_id,start_ns)
);
CREATE INDEX IF NOT EXISTS candle_updates_replay_idx
    ON market_data.candle_updates(topic,source_market_id,market_sequence);
COMMIT;
