"""DAG demonstrating the umbrella use case with dummy operators."""
import os
import pathlib
from airflow.utils.dates import days_ago
from datetime import datetime
from airflow import DAG
# from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

URL_PREFIX = "https://datahub.io/sports-data/"
EPL_URL_TEMPLATE = URL_PREFIX + "/english-premier-league/r/season-{{ execution_date.strftime(\'%Y-%m\') }}.csv"

# https://datahub.io/sports-data/english-premier-league/r/season-1819.csv


def print_env_var():
    print(os.getenv('GCP_PROJECT_ID'))
    print(os.getenv('GCP_GCS_BUCKET'))
    print(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
    print(os.getenv('AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT'))
    print(os.environ.get("AIRFLOW_HOME", "/opt/airflow/"))
    print(os.environ.get("BIGQUERY_DATASET", "european_football_leagues"))
    print(pathlib.Path(__file__).parent.resolve())
    print(pathlib.Path().resolve())
    print(os.listdir(pathlib.Path().resolve()))
    print(EPL_URL_TEMPLATE)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="01_umbrella",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    print_context = PythonOperator(
        task_id="print_env",
        python_callable=print_env_var
    )

    print_context