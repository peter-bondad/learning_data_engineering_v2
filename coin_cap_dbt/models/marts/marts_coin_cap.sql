SELECT
symbol,
COUNT(*) as total_records,
AVG(price_usd) as avg_price_usd,
MIN(price_usd) as min_price_usd,
MAX(price_usd) as max_price_usd
FROM {{ ref('stg_coin_cap') }}
GROUP BY symbol
ORDER BY avg_price_usd DESC