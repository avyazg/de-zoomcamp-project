import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")
SPARK_CLUSTER = os.environ.get("SPARK_CLUSTER")

locations = ['Ankara, Turkey', 'Istanbul, Turkey', 'Antalya, Turkey']

today = '{{ ds }}'

# this should be done on docker-build stage, but until this moment everything worked fine without this settings
os.system(f'gcloud auth activate-service-account --key-file {os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")}')
os.environ['CLOUDSDK_CORE_PROJECT'] = PROJECT_ID

default_args = {
    "owner": "airflow",
    "start_date": datetime(2022,5,1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="weather_data_transform",
    schedule_interval="0 9 1 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['de-project'],
) as dag:

    # to avoid uploading of current incomplete month due to manual dag triggering
    current_month_1st = datetime.today().strftime(format='%Y-%m-%d')[:-2] + '01'

    bq_create_partitioned_table_task_list = []

    for loc in locations:
        city = loc.split(', ')[0]

        CREATE_BQ_TBL_QUERY = (
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{city}_data\
            PARTITION BY DATE(date_time) \
            AS \
            SELECT * FROM {BIGQUERY_DATASET}.external_table \
            WHERE address = '{loc}' AND DATE_TRUNC(date_time, month) <> '{current_month_1st}';"
        )

        bq_create_partitioned_table_task = BigQueryInsertJobOperator(
            task_id=f"bq_create_{city}_partitioned_table_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

        bq_create_partitioned_table_task_list.append(bq_create_partitioned_table_task)

    command = f"""gcloud dataproc jobs submit pyspark \
                --cluster={SPARK_CLUSTER} \
                --region=europe-west1 \
                --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
                gs://{BUCKET}/code/data_transform_spark.py \
                -- \
                --dataset={BIGQUERY_DATASET}"""

    run_pyspark_script_task = BashOperator(
        task_id="run_pyspark_script",
        bash_command=command
    )

    bq_create_partitioned_table_task_list >> run_pyspark_script_task


