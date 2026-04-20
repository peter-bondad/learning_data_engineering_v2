import json
import requests
from time import sleep

from src.logging.logger import get_logger

logger = get_logger(__name__)

COIN_CAP_FIELDS = ["id", "rank", "symbol", "name", "priceUsd"]

ATTEMPT_THRESHOLD = 3
def fetch_coin_cap(api_key: str, limit: int = 10):
    url = f"https://rest.coincap.io/v3/assets?limit={limit}"

    for attempt in range(ATTEMPT_THRESHOLD):
        try:
            res = requests.get(
                url,
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=10,
            )
            res.raise_for_status()
            break
        except requests.RequestException as e:
            logger.warning(f"Attempt {attempt+1} failed: {e}")
            sleep(2)
    else:
        raise RuntimeError("Failed to fetch data after retries")

    raw_coin_data = res.json().get("data", [])
    with open("coin_cap_data.json", "w") as file:
         json.dump(raw_coin_data, file, indent=4)

    coins = []
    invalid_count = 0

    for coin in raw_coin_data:
        missing = [f for f in COIN_CAP_FIELDS if f not in coin]
        if missing:
            logger.warning(
                f"Coin {coin.get('id', 'unknown')} missing fields: {missing}"
            )
            invalid_count += 1
            
            # Skip invalid records for now.
            # In production, route failed records to a dead-letter queue (DLQ) or error table
            # for further inspection and reprocessing. Avoid silent data loss by ensuring
            # proper observability (logging, metrics, and alerting) for all failures.
            continue
        coins.append(coin)

    logger.info(f"Fetched {len(coins)} valid coins, {invalid_count} invalid")

    return coins