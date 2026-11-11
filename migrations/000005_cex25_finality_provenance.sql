BEGIN;
-- Historical result provenance is append-only. Never fabricate hashes from
-- book checkpoints: old envelope bytes were not retained by CEX25.1.
CREATE TABLE IF NOT EXISTS market_data.matching_result_provenance (
  topic TEXT NOT NULL,
  source_market_id BIGINT NOT NULL CHECK (source_market_id BETWEEN 0 AND 4294967295),
  market_sequence NUMERIC(20,0) NOT NULL CHECK (market_sequence > 0),
  payload_sha256 BYTEA NOT NULL CHECK (octet_length(payload_sha256) = 32),
  PRIMARY KEY(topic,source_market_id,market_sequence)
);
-- Source-time high-watermark advances only in successful Kafka-offset TXs.
-- 48-hour maximum out-of-order event-time policy; no wall-clock finalization.
CREATE TABLE IF NOT EXISTS market_data.market_event_watermarks (
  topic TEXT NOT NULL,
  source_market_id BIGINT NOT NULL CHECK (source_market_id BETWEEN 0 AND 4294967295),
  max_recorded_at_ns NUMERIC(30,0) NOT NULL,
  PRIMARY KEY(topic,source_market_id)
);
ALTER TABLE market_data.candles ADD COLUMN IF NOT EXISTS is_final BOOLEAN NOT NULL DEFAULT FALSE;
CREATE INDEX IF NOT EXISTS candles_final_candidates_idx
  ON market_data.candles(topic,source_market_id,end_ns)
  WHERE NOT is_final;
CREATE INDEX IF NOT EXISTS public_trades_reference_idx
  ON market_data.public_trades(topic,source_market_id,recorded_at_ns DESC,market_sequence DESC,event_ordinal DESC);
COMMIT;
