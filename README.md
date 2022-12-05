# Extract & Load Football Statistics to GCS & BQ

This project aims to extract data from an external data source and load them in Google Cloud Storage & BigQuery.
Please check the accompanying article for this repo. 

<a href="https://medium.com/@baluramachandra90/extract-and-load-football-statistics-to-google-cloud-storage-bigquery-with-airflow-1a217227dbd1" target="_blank">here</a>

![](images/workflow_summary.png)


### Data Source
The data that I am going to extract is the football team statistics from top 5 European football leagues on datahub. There are 5 leagues (English Premier League, Spanish La Liga, German Bundesliga, Italian Serie A and French Ligue 1) and 10 seasons (2009/2010–2018/2019) of data stored in csv format. There are 50 csv files that I am going to extract from the data source.

![](images/data_source.png)

![alt text](images/example_data.png)
