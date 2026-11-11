BEGIN;
-- CEX24.3: exact, source-order-aware minute summaries for rolling 24h stats.
-- Apply with the market-data consumer STOPPED. The backfill runs atomically;
-- existing public_trades are not discarded, and recovery replay is unchanged.
-- No floating-point or timestamp rounding participates in the accounting.
CREATE TABLE IF NOT EXISTS market_data.trade_minute_rollups (
    topic TEXT NOT NULL,
    source_market_id BIGINT NOT NULL CHECK (source_market_id BETWEEN 0 AND 4294967295),
    minute_start_ns NUMERIC(30,0) NOT NULL,
    market_id TEXT NOT NULL CHECK (length(market_id) > 0),
    first_sequence NUMERIC(20,0) NOT NULL CHECK (first_sequence > 0),
    first_ordinal NUMERIC(20,0) NOT NULL,
    first_price_units NUMERIC(20,0) NOT NULL CHECK (first_price_units > 0),
    last_sequence NUMERIC(20,0) NOT NULL CHECK (last_sequence > 0),
    last_ordinal NUMERIC(20,0) NOT NULL,
    last_price_units NUMERIC(20,0) NOT NULL CHECK (last_price_units > 0),
    high_price_units NUMERIC(20,0) NOT NULL CHECK (high_price_units > 0),
    low_price_units NUMERIC(20,0) NOT NULL CHECK (low_price_units > 0),
    base_volume_atoms NUMERIC NOT NULL CHECK (base_volume_atoms > 0),
    quote_volume_atoms NUMERIC NOT NULL CHECK (quote_volume_atoms > 0),
    trade_count NUMERIC(20,0) NOT NULL CHECK (trade_count > 0),
    PRIMARY KEY (topic, source_market_id, minute_start_ns),
    CHECK (minute_start_ns = floor(minute_start_ns / 60000000000) * 60000000000),
    CHECK ((first_sequence, first_ordinal) <= (last_sequence, last_ordinal)),
    CHECK (low_price_units <= high_price_units)
);

-- Historical backfill. Window boundaries are based on recorded_at_ns, while
-- open and close are selected by MATCHING sequence and event ordinal (not
-- record timestamp). This is essential for late-recorded executions.
WITH prepared AS (
    SELECT topic,source_market_id,market_id,
           floor(recorded_at_ns / 60000000000) * 60000000000 AS minute_start_ns,
           market_sequence,event_ordinal,price_units,base_atoms,quote_atoms
    FROM market_data.public_trades
), summed AS (
    SELECT topic,source_market_id,minute_start_ns,
           min(market_id) AS market_id,
           max(price_units) AS high_price_units,min(price_units) AS low_price_units,
           sum(base_atoms) AS base_volume_atoms,sum(quote_atoms) AS quote_volume_atoms,
           count(*) AS trade_count
    FROM prepared GROUP BY topic,source_market_id,minute_start_ns
), firsts AS (
    SELECT DISTINCT ON (topic,source_market_id,minute_start_ns)
           topic,source_market_id,minute_start_ns,
           market_sequence AS first_sequence,event_ordinal AS first_ordinal,price_units AS first_price_units
    FROM prepared
    ORDER BY topic,source_market_id,minute_start_ns,market_sequence,event_ordinal
), lasts AS (
    SELECT DISTINCT ON (topic,source_market_id,minute_start_ns)
           topic,source_market_id,minute_start_ns,
           market_sequence AS last_sequence,event_ordinal AS last_ordinal,price_units AS last_price_units
    FROM prepared
    ORDER BY topic,source_market_id,minute_start_ns,market_sequence DESC,event_ordinal DESC
)
INSERT INTO market_data.trade_minute_rollups (
    topic,source_market_id,minute_start_ns,market_id,
    first_sequence,first_ordinal,first_price_units,
    last_sequence,last_ordinal,last_price_units,
    high_price_units,low_price_units,base_volume_atoms,quote_volume_atoms,trade_count
)
SELECT s.topic,s.source_market_id,s.minute_start_ns,s.market_id,
       f.first_sequence,f.first_ordinal,f.first_price_units,
       l.last_sequence,l.last_ordinal,l.last_price_units,
       s.high_price_units,s.low_price_units,s.base_volume_atoms,s.quote_volume_atoms,s.trade_count
FROM summed s
JOIN firsts f USING (topic,source_market_id,minute_start_ns)
JOIN lasts l USING (topic,source_market_id,minute_start_ns)
ON CONFLICT (topic,source_market_id,minute_start_ns) DO NOTHING;
COMMIT;
