import os

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
# dataset_file = "season-0910.csv"
# dataset_url = f"https://datahub.io/sports-data/english-premier-league/r/{dataset_file}"

# The second argument of each `.get` is what it will default to if it's empty.
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "european_football_leagues")

URL_PREFIX = "https://datahub.io/sports-data"
seasons = ["season-0910", "season-1011", "season-1112", "season-1213", "season-1314", "season-1415", "season-1516", "season-1617", "season-1718", "season-1819"]


# Note: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

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

# a function to donwload, parquetize and upload yellow, green and FHV taxi data to google cloud storage
def download_upload_data_dag(
    dag,
    url_template,
    local_csv_path_template,
    gcs_path_template
):
    with dag:
        download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {url_template} > {local_csv_path_template}"
        )

        local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_csv_path_template,
            },
        )

        rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_csv_path_template}"
        )

        download_dataset_task >> local_to_gcs_task >> rm_task


# processing epl


for season in seasons:
  EPL_URL_TEMPLATE = URL_PREFIX + "/english-premier-league/r/" + season + ".csv"
  EPL_CSV_FILE_TEMPLATE = AIRFLOW_HOME + "/epl_" + season + ".csv"
  EPL_GCS_PATH_TEMPLATE = "football_stats/EPL/epl_" + season + ".csv"

  epl_dag = DAG(
    dag_id="epl_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
    )

  download_upload_data_dag(
        dag=epl_dag,
        url_template=EPL_URL_TEMPLATE,
        local_csv_path_template=EPL_CSV_FILE_TEMPLATE,
        gcs_path_template=EPL_GCS_PATH_TEMPLATE
    )


