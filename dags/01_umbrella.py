"""DAG demonstrating the umbrella use case with dummy operators."""
# import os
# import pathlib
# import airflow.utils.dates
# from airflow import DAG
# # from airflow.operators.dummy import DummyOperator
# from airflow.operators.python import PythonOperator


# def print_env_var():
#     print(os.getenv('GCP_PROJECT_ID'))
#     print(os.getenv('GCP_GCS_BUCKET'))
#     print(os.getenv('GOOGLE_APPLICATION_CREDENTIALS'))
#     print(os.getenv('AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT'))
#     print(os.environ.get("AIRFLOW_HOME", "/opt/airflow/"))
#     print(os.environ.get("BIGQUERY_DATASET", "european_football_leagues"))
#     print(pathlib.Path(__file__).parent.resolve())
#     print(pathlib.Path().resolve())


# print_context = PythonOperator(
#     task_id="print_env",
#     python_callable=print_env_var,
#     dag=dag,
# )


import os
import logging

# Inbuilt airflow operators
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# To interact with GCS storage
from google.cloud import storage

# Google airflow operators to interact with Bigquery to create external table
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

# Import some env variables that we have in docker-compose.yml 
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# Specify dataset and store environmental variables locally
dataset_file = "season-0910.csv"
dataset_url = f"https://datahub.io/sports-data/english-premier-league/r/{dataset_file}"

# The second argument of each `.get` is what it will default to if it's empty.
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "european_football_leagues")

# Note: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    print(client)
    bucket = client.bucket(bucket)
    print(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# Note: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSLf {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    # Todo: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{dataset_file}",
            "local_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    download_dataset_task >> local_to_gcs_task


