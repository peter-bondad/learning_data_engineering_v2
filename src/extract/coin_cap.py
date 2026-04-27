import datetime

from pydantic import ValidationError
import requests
from time import sleep

from src.logging.logger import get_logger
from src.schema.coin_cap import CoinCapSchema
logger = get_logger(__name__)

COIN_CAP_FIELDS = ["id", "rank", "symbol", "name", "priceUsd"]
ATTEMPT_THRESHOLD = 3


def fetch_coin_cap(api_key: str) -> list:
    if not api_key:
        raise ValueError("COIN_CAP_API_KEY is missing")

    url = "https://rest.coincap.io/v3/assets"
    logger.info(f"Fetching data from {url}")

    for attempt in range(ATTEMPT_THRESHOLD):
        try:
            res = requests.get(
                url,
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=10,
            )

            if res.status_code in (401, 403):
                raise ValueError("Unauthorized: invalid or missing API key")

            res.raise_for_status()
            logger.info(f"Response status: {res.status_code}")
            break

        except ValueError:
            raise  # don't retry auth errors
        except requests.Timeout:
            logger.warning(f"Timeout on attempt {attempt + 1}")
        except requests.ConnectionError:
            logger.warning(f"Connection error on attempt {attempt + 1}")
        except requests.HTTPError as e:
            if 500 <= res.status_code < 600:
                logger.warning(f"Server error on attempt {attempt + 1}: {e}")
            else:
                raise  # don't retry client errors

        sleep(2)
    else:
        raise RuntimeError(f"Failed after {ATTEMPT_THRESHOLD} retries")

    raw_data = res.json().get("data", [])
    logger.info(f"Raw response contains {len(raw_data)} records")

    valid = []
    invalid = 0

    for coin in raw_data:
        try: 
            validated = CoinCapSchema(**coin, lastIngestedAt=datetime.utcnow()) # validate each record against the schema, adding a timestamp for when it was ingested. This ensures that we only process well-formed data and can track when each record was added to our system.
            valid.append(validated.model_dump())
        except ValidationError as e: # catch validation errors for individual records, log them, and continue processing the rest of the data. This way, we can still load valid records even if some are malformed.
            invalid += 1
            logger.warning(f"Validation failed for coin {coin.get('id', 'unknown')}: {e}")
    logger.info(f"Validation complete: {len(valid)} valid, {invalid} invalid records") 
    
    return valid