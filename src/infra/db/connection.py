from psycopg2 import connect
from src.logging.logger import get_logger
from src.utils.get_env import get_env

logger = get_logger(__name__)

def db_connect():
    try:
        logger.info("Connecting to the database...")
        return connect(
            host=get_env("POSTGRES_HOST"),
            port=get_env("POSTGRES_PORT"),
            user=get_env("POSTGRES_USER"),
            password=get_env("POSTGRES_PASSWORD"),
            dbname=get_env("POSTGRES_NAME"),
        )
    except Exception as e:
        logger.error(f"Failed to connect to the database: {e}")
        raise e