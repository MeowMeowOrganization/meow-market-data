BEGIN;
CREATE SCHEMA IF NOT EXISTS market_data;

-- next_offset is the next Kafka offset that may be applied. The unique
-- partition row is locked in every checkpoint TX, serializing all markets in
-- the same partition. A new group owner steals the fence by incrementing its
-- generation; stale owners cannot write afterwards.
CREATE TABLE IF NOT EXISTS market_data.partition_cursors (
    topic TEXT NOT NULL CHECK (length(topic) > 0),
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    next_offset BIGINT NOT NULL DEFAULT 0 CHECK (next_offset >= 0),
    owner_token TEXT,
    fence_generation BIGINT NOT NULL DEFAULT 0 CHECK (fence_generation >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition_id)
);

-- The SHA-256 checksum detects accidental blob corruption. The checkpoint
-- itself includes the market definition, format version, depth, full resting
-- orders, sequence and last committed envelope digest.
CREATE TABLE IF NOT EXISTS market_data.book_checkpoints (
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL,
    source_market_id BIGINT NOT NULL CHECK (source_market_id >= 0 AND source_market_id <= 4294967295),
    checkpoint BYTEA NOT NULL,
    checkpoint_sha256 BYTEA NOT NULL CHECK (octet_length(checkpoint_sha256) = 32),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition_id, source_market_id),
    FOREIGN KEY (topic, partition_id)
        REFERENCES market_data.partition_cursors(topic, partition_id)
        ON DELETE RESTRICT
);
COMMIT;
