from airflow import DAG
from airflow.decorators  import task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

from src.logging.logger import get_logger
from src.extract.coin_cap import fetch_coin_cap
from src.infra.storage.client import upload_raw_data
from src.load.coin_cap import load
from src.config.settings import get_api_key
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
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["elt", "coin_cap"],
) as dag:

    @task
    def extract_task() -> str:
        logging.getLogger("src.extract.coin_cap").setLevel(logging.DEBUG)
        logger.info("Starting extract task")

        api_key = get_api_key()
        coins = fetch_coin_cap(api_key)

        key = upload_raw_data(coins)
        logger.info(f"Extract complete — stored at {key}")

        return key

    @task
    def load_task(key: str) -> None:
        logging.getLogger("src.load.coin_cap").setLevel(logging.DEBUG)
        logger.info(f"Starting load task for key: {key}")

        load(key)

        logger.info("Load complete")
        
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --project-dir /opt/airflow/coin_cap_dbt",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="dbt test --project-dir /opt/airflow/coin_cap_dbt",
    )

    key = extract_task()
    load_result = load_task(key)
    load_result >> dbt_run >> dbt_test