from src.infra.storage.client import read_raw_data
from src.infra.db.connection import db_connect
from src.logging.logger import get_logger

logger = get_logger(__name__)

CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS coin_cap (
        id         TEXT PRIMARY KEY,
        rank       INTEGER,
        symbol     TEXT,
        name       TEXT,
        price_usd  NUMERIC,
        loaded_at  TIMESTAMP DEFAULT NOW()
    )
"""

UPSERT_SQL = """
    INSERT INTO coin_cap (id, rank, symbol, name, price_usd, loaded_at)
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
                    cur.execute(UPSERT_SQL, (
                        coin["id"],
                        int(coin["rank"]),
                        coin["symbol"],
                        coin["name"],
                        float(coin["priceUsd"]),
                    ))
                except Exception as e:
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