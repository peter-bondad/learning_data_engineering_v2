from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow.models import Variable

from src.extract.coin_cap import fetch_coin_cap
from src.infra.storage.client import read_raw_data, upload_raw_data

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="coin_cap_elt_pipeline",
    default_args=default_args,
    description="CoinCap ELT Pipeline",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "coin_cap"],
) as dag:

    @task
    def extract_task():
        api_key = Variable.get("COIN_CAP_API_KEY")
        coins = fetch_coin_cap(api_key)
        key = upload_raw_data(coins)
        return key

    @task
    def read_task(key):
        data = read_raw_data(key)
        print(f"Loaded {len(data)} records from {key}")

    key = extract_task()
    read_task(key)