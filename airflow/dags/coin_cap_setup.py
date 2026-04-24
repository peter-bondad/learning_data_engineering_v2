from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="coin_cap_setup",
    schedule=None,  # manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["setup", "coin_cap"],
) as dag:

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

    init_schema >> init_table