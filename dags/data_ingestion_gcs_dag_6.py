import os

# Inbuilt airflow operators
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# To interact with GCS storage
from google.cloud import storage

# Google airflow operators to interact with Bigquery to create external table
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

# Import the env variables that we have in the Dockefile
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "european_football_leagues")

# Data source url prefix 
URL_PREFIX = "https://datahub.io/sports-data"


# A function to upload file to GCS
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

# A function to donwload and upload data to GCS
def download_upload_data(url_template, filter, local_csv_path_template, season, gcs_path_template):
    
    # A task to download the data from its source
    download_dataset_task = BashOperator(
            task_id="download_dataset_task",
            bash_command=f"curl -sSLf {url_template} | {filter} | sed '1s/$/,\season/; 2,$s/$/,\{season}/' > {local_csv_path_template}"
    )
    
    # A task to upload data to GCS
    local_to_gcs_task = PythonOperator(
            task_id="local_to_gcs_task",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": gcs_path_template,
                "local_file": local_csv_path_template,
            },
    )
    
    # A task to remove previously downloaded data from Airflow database
    rm_task = BashOperator(
            task_id="rm_task",
            bash_command=f"rm {local_csv_path_template}"
    )
    
    # Define the task dependencies with the following execution order
    download_dataset_task >> local_to_gcs_task >> rm_task



default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="extract_load_gcs_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    leagues = ["english-premier-league", "spanish-la-liga", "german-bundesliga", "italian-serie-a", "french-ligue-1"]
    seasons = ["season-0910", "season-1011", "season-1112", "season-1213", "season-1314", "season-1415", "season-1516", "season-1617", "season-1718", "season-1819"]

    for league in leagues:
        for season in seasons:
            with TaskGroup(group_id=f'group_{league}_{season}') as tg1:
                URL_TEMPLATE = f"{URL_PREFIX}/{league}/r/{season}.csv"
                CSV_FILE_TEMPLATE = f"{AIRFLOW_HOME}/{league}_{season}.csv"
                GCS_PATH_TEMPLATE = f"football_stats/{league}/{league}_{season}.csv"

                if league == "english-premier-league":
                    download_upload_data(url_template=URL_TEMPLATE,
                                        filter="cut -f 1-10,12-23 -d,",
                                        local_csv_path_template=CSV_FILE_TEMPLATE,
                                        season=season,
                                        gcs_path_template=GCS_PATH_TEMPLATE
                                    )
                else:
                    download_upload_data(url_template=URL_TEMPLATE,
                                        filter="cut -f 1-22 -d,",
                                        local_csv_path_template=CSV_FILE_TEMPLATE,
                                        season=season,
                                        gcs_path_template=GCS_PATH_TEMPLATE
                                    )


with DAG(
    dag_id="load_to_bq_dag",
    schedule_interval=None,
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    for league in leagues:
        with TaskGroup(group_id=f'group_{league}') as tg2:
            bigquery_external_table_task = BigQueryCreateExternalTableOperator(
                    task_id="bigquery_external_table_task",
                    destination_project_dataset_table=f"{BIGQUERY_DATASET}.{league}_external_table",
                    bucket=BUCKET,
                    source_objects=[f"football_stats/{league}/*"],
                    skip_leading_rows=1,
                    schema_fields=[
                        {"name": "Div", "type": "STRING"},
                        {"name": "Date", "type": "STRING"},
                        {"name": "HomeTeam", "type": "STRING"},
                        {"name": "AwayTeam", "type": "STRING"},
                        {"name": "FTHG", "type": "INTEGER"},
                        {"name": "FTAG", "type": "INTEGER"},
                        {"name": "FTR", "type": "STRING"},
                        {"name": "HTHG", "type": "INTEGER"},
                        {"name": "HTAG", "type": "INTEGER"},
                        {"name": "HTR", "type": "STRING"},
                        {"name": "HS", "type": "INTEGER"},
                        {"name": "AS", "type": "INTEGER"},
                        {"name": "HST", "type": "INTEGER"},
                        {"name": "AST", "type": "INTEGER"},
                        {"name": "HF", "type": "INTEGER"},
                        {"name": "AF", "type": "INTEGER"},
                        {"name": "HC", "type": "INTEGER"},
                        {"name": "AC", "type": "INTEGER"},
                        {"name": "HY", "type": "INTEGER"},
                        {"name": "AY", "type": "INTEGER"},
                        {"name": "HR", "type": "INTEGER"},
                        {"name": "AR", "type": "INTEGER"},
                        {"name": "season", "type": "STRING"}
                    ]
            )
            
            bigquery_external_table_task
