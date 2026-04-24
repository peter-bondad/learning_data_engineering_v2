import requests
from time import sleep

from src.logging.logger import get_logger

logger = get_logger(__name__)

COIN_CAP_FIELDS = ["id", "rank", "symbol", "name", "priceUsd"]
ATTEMPT_THRESHOLD = 3


def fetch_coin_cap(api_key: str) -> list:
    if not api_key:
        raise ValueError("COIN_CAP_API_KEY is missing")

    url = "https://rest.coincap.io/v3/assets"

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
                raise

        sleep(2)
    else:
        raise RuntimeError("Failed after retries")

    raw_data = res.json().get("data", [])

    valid = []
    invalid = 0

    for coin in raw_data:
        if any(field not in coin for field in COIN_CAP_FIELDS):
            invalid += 1
            continue
        valid.append(coin)

    logger.info(f"Fetched {len(valid)} valid coins, {invalid} invalid")

    return valid