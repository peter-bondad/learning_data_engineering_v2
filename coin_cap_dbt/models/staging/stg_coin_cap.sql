-- This model selects data from the raw coin cap source, converting the price from a string to a numeric type and adding an ingestion timestamp. 
-- The resulting data is used for further transformations and analysis in downstream models.
SELECT
    id,
    rank,
    symbol,
    name,
    priceUsd::numeric AS price_usd,
    CURRENT_TIMESTAMP AS ingested_at
FROM {{ source('raw', 'coin_cap_raw') }}