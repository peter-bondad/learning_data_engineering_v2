import os
from airflow.sdk import Variable

def get_api_key() -> str:
    api_key = (
        os.getenv("COIN_CAP_API_KEY")
        or Variable.get("COIN_CAP_API_KEY")
    )

    if not api_key:
        raise ValueError("Missing COIN_CAP_API_KEY")

    return api_key