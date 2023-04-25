import os
from datetime import datetime
import requests
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'de_project_dataset')

locations = ['Ankara, Turkey', 'Istanbul, Turkey', 'Antalya, Turkey']

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022,3,1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="weather_data_transform",
    schedule_interval="0 15 1 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['de-project'],
) as dag:

    for loc in locations:
        city = loc.split(', ')[0]

        CREATE_BQ_TBL_QUERY = (
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{city}_data\
            PARTITION BY DATE(date_time) \
            AS \
            SELECT * FROM {BIGQUERY_DATASET}.external_table \
            WHERE address = '{loc}';"
        )

        # Create a partitioned table from external table
        bq_create_partitioned_table_task = BigQueryInsertJobOperator(
            task_id=f"bq_create_{city}_partitioned_table_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

        bq_create_partitioned_table_task


