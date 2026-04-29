
SELECT
    id,
    rank,
    symbol,
    name,
    priceUsd::numeric AS price_usd,
    CURRENT_TIMESTAMP AS ingested_at
FROM {{ source('raw', 'coin_cap_raw') }}