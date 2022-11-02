FROM quay.io/astronomer/astro-runtime:6.0.3

ENV GCP_PROJECT_ID='zinc-proton-363104'
ENV GCP_GCS_BUCKET='dtc_data_lake_zinc-proton-363104'
ENV GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/google_credentials.json
ENV AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT='google-cloud-platform://?extra__google_cloud_platform__key_path=/usr/local/airflow/google_credentials.json'