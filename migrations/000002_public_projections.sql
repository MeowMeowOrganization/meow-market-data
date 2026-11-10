BEGIN;
-- Public market-data history only. No account IDs or settlement balances.
-- 20-digit NUMERIC handles the entire uint64 sequence/price domain without
-- signed BIGINT overflow; prices and quantities remain exact integers.
-- Fail closed on in-place upgrade: prior CEX23.4 checkpoints have no public
-- trade/candle history. Use a fresh projection database and replay the FULL
-- source topic from sequence 1 (or implement an audited backfill first).
DO $$ BEGIN
  IF to_regclass('market_data.public_books') IS NULL
     AND EXISTS (SELECT 1 FROM market_data.book_checkpoints) THEN
    RAISE EXCEPTION 'CEX23.5 requires fresh replay: existing CEX23.4 checkpoints lack trade history';
  END IF;
END $$;
CREATE TABLE IF NOT EXISTS market_data.public_books (
    topic TEXT NOT NULL CHECK (length(topic) > 0),
    source_market_id BIGINT CHECK (source_market_id BETWEEN 0 AND 4294967295),
    market_id TEXT NOT NULL CHECK (length(market_id) > 0),
    market_sequence NUMERIC(20,0) NOT NULL CHECK (market_sequence > 0),
    market_definition_version NUMERIC(20,0) NOT NULL CHECK (market_definition_version > 0),
    depth INTEGER NOT NULL CHECK (depth > 0),
    recorded_at_ns NUMERIC(30,0) NOT NULL,
    snapshot BYTEA NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (topic,source_market_id)
);
-- Durable output history for a later resumable streaming publisher. Includes
-- zero-change deltas; never directly depends on Redis Streams retention.
CREATE TABLE IF NOT EXISTS market_data.book_deltas (
    topic TEXT NOT NULL,
    source_market_id BIGINT NOT NULL,
    market_sequence NUMERIC(20,0) NOT NULL CHECK (market_sequence > 0),
    payload BYTEA NOT NULL,
    PRIMARY KEY (topic,source_market_id,market_sequence)
);
CREATE TABLE IF NOT EXISTS market_data.public_trades (
    topic TEXT NOT NULL,
    source_market_id BIGINT NOT NULL CHECK (source_market_id BETWEEN 0 AND 4294967295),
    market_id TEXT NOT NULL,
    market_sequence NUMERIC(20,0) NOT NULL CHECK (market_sequence > 0),
    event_ordinal NUMERIC(20,0) NOT NULL,
    trade_id TEXT NOT NULL CHECK (length(trade_id) > 0),
    recorded_at_ns NUMERIC(30,0) NOT NULL,
    price_units NUMERIC(20,0) NOT NULL CHECK (price_units > 0),
    base_atoms NUMERIC(20,0) NOT NULL CHECK (base_atoms > 0),
    quote_atoms NUMERIC(20,0) NOT NULL CHECK (quote_atoms > 0),
    payload BYTEA NOT NULL,
    PRIMARY KEY (topic,source_market_id, market_sequence, event_ordinal),
    UNIQUE (topic,trade_id)
);
CREATE INDEX IF NOT EXISTS public_trades_time_idx ON market_data.public_trades(topic,source_market_id, recorded_at_ns);
CREATE INDEX IF NOT EXISTS public_trades_latest_idx ON market_data.public_trades(topic,source_market_id, market_sequence DESC, event_ordinal DESC);
CREATE TABLE IF NOT EXISTS market_data.candles (
    topic TEXT NOT NULL,
    source_market_id BIGINT NOT NULL CHECK (source_market_id BETWEEN 0 AND 4294967295),
    market_id TEXT NOT NULL,
    interval_id INTEGER NOT NULL CHECK (interval_id BETWEEN 1 AND 5),
    start_ns NUMERIC(30,0) NOT NULL,
    end_ns NUMERIC(30,0) NOT NULL,
    revision BIGINT NOT NULL CHECK (revision >= 0),
    payload BYTEA NOT NULL,
    PRIMARY KEY (topic,source_market_id,interval_id,start_ns)
);
CREATE INDEX IF NOT EXISTS candles_recent_idx ON market_data.candles(topic,source_market_id,interval_id,start_ns DESC);
COMMIT;
