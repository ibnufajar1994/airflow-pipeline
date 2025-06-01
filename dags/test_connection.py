from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException
from datetime import datetime
import logging

@dag(
    dag_id="test_connections",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["test", "connection"],
)
def test_connection_dag():

    @task
    def verify_postgres_conn(conn_id: str):
        logging.info(f"⏳ Attempting to connect to `{conn_id}`...")
        try:
            hook = PostgresHook(postgres_conn_id=conn_id)
            connection = hook.get_conn()
            cursor = connection.cursor()
            cursor.execute("SELECT 1;")
            res = cursor.fetchone()
            cursor.close()
            connection.close()
            logging.info(f"✅ Successfully connected to `{conn_id}`. Query result: {res}")
        except Exception as error:
            logging.error(f"❌ Connection to `{conn_id}` failed with error: {error}")
            raise AirflowException(f"Connection test failed for `{conn_id}`: {error}")

    verify_postgres_conn.override(task_id="test_connection_db")("aircraft_db")
    verify_postgres_conn.override(task_id="test_connection_warehouse")("warehouse_db")

test_connection_dag()
