CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.coin_cap (
    id          VARCHAR(50) PRIMARY KEY,
    rank        INTEGER,
    symbol      VARCHAR(10),
    name        VARCHAR(100),
    price_usd   NUMERIC(20, 8),
    ingested_at TIMESTAMP DEFAULT NOW()
);