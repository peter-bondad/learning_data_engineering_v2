from src.infra.storage.client import read_raw_data
from src.infra.db.connection import db_connect
from src.logging.logger import get_logger

logger = get_logger(__name__)
# This function handles null or empty values for integers, which can occur in the CoinCap API data. It ensures that we don't attempt to convert invalid strings to numbers, which would raise exceptions during the load process.
def safe_int(v):
    return int(v) if v not in (None, "", "null") else None

# This function handles null or empty values for floats, similar to safe_int. It ensures that we can safely convert valid numeric strings to floats while gracefully handling invalid or missing data without causing exceptions.
def safe_float(v):
    return float(v) if v not in (None, "", "null") else None

CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS staging.coin_cap (
        id         TEXT PRIMARY KEY,
        rank       INTEGER,
        symbol     TEXT,
        name       TEXT,
        price_usd  NUMERIC,
        loaded_at  TIMESTAMP DEFAULT NOW()
    )
"""

UPSERT_SQL = """
    INSERT INTO staging.coin_cap (id, rank, symbol, name, price_usd, loaded_at)
    VALUES (%s, %s, %s, %s, %s, NOW())
    ON CONFLICT (id) DO UPDATE SET
        rank      = EXCLUDED.rank,
        price_usd = EXCLUDED.price_usd,
        loaded_at = EXCLUDED.loaded_at
"""

def load(key: str):
    logger.info(f"Starting load from MinIO key: {key}")

    raw_data = read_raw_data(key)
    logger.info(f"Read {len(raw_data)} records from MinIO")

    conn = db_connect()

    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
            logger.info("Table ensured: staging.coin_cap")

            failed = []
            for coin in raw_data:
                try:
                    # coin["field name"] is used for required fields, while coin.get("field name") is used for optional fields that may be missing or null. This way, we can handle missing data gracefully without raising KeyError exceptions.
                    cur.execute(UPSERT_SQL, (
                        coin["id"],
                        safe_int(coin.get("rank")),
                        coin.get("name"),
                        coin.get("symbol"),
                        safe_float(coin.get("priceUsd")),
                    ))
                except Exception as e:
                    conn.rollback()  # Rollback on individual record failure to maintain overall transaction integrity
                    failed.append((coin.get("id"), str(e)))
                    logger.warning(f"Failed to upsert coin {coin.get('id')}: {e}")

            conn.commit()
            logger.info(f"Upserted {len(raw_data) - len(failed)} records successfully")

            if failed:
                logger.warning(f"{len(failed)} records failed: {failed}")

    except Exception as e:
        conn.rollback()
        logger.error(f"Load failed — rolled back: {e}")
        raise
    finally:
        conn.close()
        logger.info("Database connection closed")