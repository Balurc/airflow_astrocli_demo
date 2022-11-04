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
# from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
# from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Import some env variables that we have in docker-compose.yml 
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# The second argument of each `.get` is what it will default to if it's empty.
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "european_football_leagues")
# INSERT_QUERY = "SELECT * "

URL_PREFIX = "https://datahub.io/sports-data"


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

# a function to donwload, parquetize and upload yellow, green and FHV taxi data to google cloud storage
def download_upload_data(url_template, local_csv_path_template, gcs_path_template):
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

    # bigquery_table_task = GCSToBigQueryOperator(
    #         task_id='bigquery_table_task',
    #         bucket=bucket,
    #         source_objects=[f'football_stats/{league}/{season}.csv'],
    #         destination_project_dataset_table=f"{bigquery_dataset}.{league}",
    #         skip_leading_rows=1,
    #         field_delimiter=',',
    #         write_disposition='WRITE_APPEND'
    # )

    # download_dataset_task >> local_to_gcs_task >> rm_task >> bigquery_table_task
    download_dataset_task >> local_to_gcs_task >> rm_task


def move_data(project_id, league, bucket, bigquery_dataset):
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
            task_id="bigquery_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": project_id,
                    "datasetId": bigquery_dataset,
                    "tableId": f"{league}_external_table",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": "CSV",
                    "sourceUris": [f"gs://{bucket}/football_stats/{league}/*"],
                },
            },
        )
    # CREATE_BQ_TBL_QUERY = (
    #           f"CREATE OR REPLACE TABLE {bigquery_dataset}.{league} AS SELECT * FROM {bigquery_dataset}.{league}_external_table;"
    #     )

    # bq_create_table_job = BigQueryInsertJobOperator(
    #         task_id="bq_create_table_job",
    #         configuration={
    #             "query": {
    #                 "query": CREATE_BQ_TBL_QUERY,
    #                 "useLegacySql": False,
    #             }
    #         }
    #     )
    
    bigquery_external_table_task

    # bigquery_external_table_task >> bq_create_table_job


    # bigquery_table_task = GCSToBigQueryOperator(
    # task_id='bigquery_table_task',
    # bucket=bucket,
    # source_objects=['football_stats/{league}/*.csv'],
    # destination_project_dataset_table=f"{bigquery_dataset}.{league}",
    # skip_leading_rows=1,
    # field_delimiter=',',
    # write_disposition='WRITE_APPEND',
    # )

    # bigquery_table_task



default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="epl_dag_4",
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
                EPL_URL_TEMPLATE = f"{URL_PREFIX}/{league}/r/{season}.csv"
                EPL_CSV_FILE_TEMPLATE = f"{AIRFLOW_HOME}/{league}_{season}.csv"
                EPL_GCS_PATH_TEMPLATE = f"football_stats/{league}/{league}_{season}.csv"

                download_upload_data(url_template=EPL_URL_TEMPLATE, 
                                    local_csv_path_template=EPL_CSV_FILE_TEMPLATE, 
                                    gcs_path_template=EPL_GCS_PATH_TEMPLATE,
                                )

        with TaskGroup(group_id=f'group_bq_{league}') as tg2:
            move_data(project_id=PROJECT_ID, league=league, bucket=BUCKET, bigquery_dataset=BIGQUERY_DATASET)



# cat season-0910.csv | cut -f 2-5,8 -d ,  > xseason-0910.csv

# bash_command=f"curl -sSLf {url_template} | cut -f 2-5,8 -d , > {local_csv_path_template}"

# cat season-0910.csv | cut -f 2-5,8 -d, | sed '1s/$/,\season/; 2,$s/$/,\season-0910/' > test3.csv