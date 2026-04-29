SELECT
id,
rank,
symbol,
name,
price_usd,
ingested_at
FROM {{ source('staging', 'coin_cap') }}