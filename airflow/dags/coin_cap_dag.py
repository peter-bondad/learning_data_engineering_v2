from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator

from src.logging.logger import get_logger
from src.extract.coin_cap import fetch_coin_cap
from src.infra.storage.client import upload_raw_data
from src.load.coin_cap import load

import logging

logger = get_logger(__name__)

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
    tags=["elt", "coin_cap"],
) as dag:

    # ✅ Operator = already a task
    init_schema = PostgresOperator(
        task_id="init_schema",
        postgres_conn_id="postgres_default",
        sql="/opt/airflow/src/sql/staging/coin_cap/schema.sql",
    )

    init_table = PostgresOperator(
        task_id="init_coin_cap_table",
        postgres_conn_id="postgres_default",
        sql="/opt/airflow/src/sql/staging/coin_cap/coin_cap.sql",
    )

    @task
    def extract_task():
        logging.getLogger("src.extract.coin_cap").setLevel(logging.DEBUG)
        logger.info("Starting extract task")

        api_key = Variable.get("COIN_CAP_API_KEY")
        coins = fetch_coin_cap(api_key)

        key = upload_raw_data(coins)
        logger.info(f"Extract complete — stored at {key}")

        return key

    @task
    def load_task(key: str):
        logging.getLogger("src.load.coin_cap").setLevel(logging.DEBUG)
        logger.info(f"Starting load task for key: {key}")

        load(key)

        logger.info("Load complete")

    key = extract_task()

    # ✅ Proper dependency chain
    init_staging >> key >> load_task(key)