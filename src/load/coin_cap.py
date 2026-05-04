from decimal import Decimal
from pathlib import Path

from src.infra.storage.client import read_raw_data
from src.infra.db.connection import db_connect
from src.logging.logger import get_logger

logger = get_logger(__name__)
# This function handles null or empty values for integers, which can occur in the CoinCap API data. It ensures that we don't attempt to convert invalid strings to numbers, which would raise exceptions during the load process.
def safe_int(v):
    return int(v) if v not in (None, "", "null") else None

# This function handles null or empty values for decimals, similar to safe_int. It ensures that we can safely convert valid numeric strings to decimals while gracefully handling invalid or missing data without causing exceptions.
def safe_decimal(v):
    return Decimal(v) if v not in (None, "", "null") else None

SQL_PATH = Path(__file__).resolve().parents[2] / "infra/sql/raw/coin_cap.sql"

# This function ensures that the raw schema for the coin cap data exists in the database. 
# It reads the SQL schema definition from a file and executes it against the database connection. This is a crucial step to ensure that the target table for loading data is properly set up before we attempt to insert any records.
def ensure_raw_schema(conn):
    with open(SQL_PATH) as f:
        conn.execute(f.read())

INSERT_SQL = """
    INSERT INTO raw.coin_cap_raw (id, rank, symbol, name, price_usd, ingested_at)
        VALUES (%s, %s, %s, %s, %s, NOW())
"""

def load(key: str):
    logger.info(f"Starting load from MinIO key: {key}")

    raw_data = read_raw_data(key)
    logger.info(f"Read {len(raw_data)} records")

    conn = db_connect()

    try:
        ensure_raw_schema(conn)

        # We prepare the records for upsert by converting the raw data into a list of tuples that match the expected format of the database table. 
        # The safe_int and safe_decimal functions are used to handle any potential issues with null or invalid values in the rank and priceUsd fields, ensuring that we can safely insert or update records without encountering exceptions due to data quality issues.
        records = [
            (
                coin["id"],
                safe_int(coin.get("rank")),
                coin.get("symbol"),
                coin.get("name"),
                safe_decimal(coin.get("priceUsd")),
            )
            for coin in raw_data
        ]

        with conn.cursor() as cur:
            cur.executemany(INSERT_SQL, records)

        conn.commit()
        logger.info(f"Inserted {len(records)} records successfully")

    except Exception as e:
        conn.rollback()
        logger.error(f"Load failed: {e}")
        raise

    finally:
        conn.close()