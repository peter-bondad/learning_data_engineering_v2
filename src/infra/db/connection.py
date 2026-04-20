from psycopg2 import connect
import os
from src.logging.logger import get_logger

logger = get_logger(__name__)

def db_connect():
    try:
        logger.info("Connecting to the database...")
        return connect(
            host=os.getenv("POSTGRES_HOST"),
            port=os.getenv("POSTGRES_PORT"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            dbname=os.getenv("POSTGRES_NAME"),
        )
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        raise e