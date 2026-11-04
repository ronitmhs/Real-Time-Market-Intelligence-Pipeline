CREATE TABLE IF NOT EXISTS market_trades (
    id BIGSERIAL PRIMARY KEY,

    asset_type VARCHAR(20) NOT NULL,
    symbol VARCHAR(20) NOT NULL,

    price NUMERIC(18, 8) NOT NULL,
    quantity NUMERIC(18, 8) NOT NULL,

    exchange VARCHAR(20),

    event_timestamp TIMESTAMPTZ NOT NULL,
    received_timestamp TIMESTAMPTZ NOT NULL,

    source VARCHAR(50) NOT NULL,

    kafka_topic VARCHAR(100) NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (kafka_topic, kafka_partition, kafka_offset)
);

CREATE INDEX IF NOT EXISTS idx_market_trades_symbol_timestamp
ON market_trades (symbol, event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_market_trades_event_timestamp
ON market_trades (event_timestamp DESC);
