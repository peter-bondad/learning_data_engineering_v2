from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    dag_id="coin_cap_setup",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["setup", "coin_cap"],
    template_searchpath=["/opt/airflow/src/sql"],  # Ensure SQL files are found
) as dag:

    init_schema = PostgresOperator(
    task_id="init_schema",
    postgres_conn_id="analytics_postgres",
    sql="staging/coin_cap/schema.sql", # Relative path to the SQL file in the template_searchpath
    )

    init_table = PostgresOperator(
        task_id="init_coin_cap_table",
        postgres_conn_id="analytics_postgres",
        sql="staging/coin_cap/coin_cap.sql", # Relative path to the SQL file in the template_searchpath
    )

    init_schema >> init_table