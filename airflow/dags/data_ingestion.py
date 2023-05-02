import os
from datetime import datetime
import requests
from io import BytesIO

from airflow import DAG
# from airflow.utils.dates import days_ago
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow.compute as pc


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

URL = "https://visual-crossing-weather.p.rapidapi.com/history"
API_KEY = os.environ.get("RAPIDAPI_KEY")

last_day_of_month = '{{ macros.ds_add(ds, -1) }}'

locations = ['Ankara, Turkey', 'Istanbul, Turkey', 'Antalya, Turkey']

# dataset_url = url_start + dataset_file
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# parquet_file = dataset_file.replace('.csv.gz', '.parquet')
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")

# def get_last_day_of_month(any_day):
    
#     any_day = datetime.strptime(any_day, '%Y-%M-%d')
#     # The day 28 exists in every month. 4 days later, it's always next month
#     next_month = any_day.replace(day=28) + timedelta(days=4)
#     # subtracting the number of the current day brings us back one month
#     last_day = next_month - timedelta(days=next_month.day)
    
#     return last_day.strftime('%Y-%M-%d')


def get_weather_data(last_day_of_month: str, location: str) -> str:
    # first_day_of_month = year_month + '-01'
    first_day_of_month = last_day_of_month[:-2] + '01'

    querystring = {"startDateTime": first_day_of_month+'T00:00:00',
                   "endDateTime": last_day_of_month+'T23:59:59',
                   "unitGroup": "metric",               
                   "aggregateHours": "2",
                   "location": location,
#                "dayStartTime":"0:00:00",
                   "contentType":"csv",
#                "dayEndTime":"23:00:00",
                   "shortColumnNames":"0"}

    headers = {
        "X-RapidAPI-Key": API_KEY,
        "X-RapidAPI-Host": "visual-crossing-weather.p.rapidapi.com"
    }

    response = requests.request("GET", URL, headers=headers, params=querystring)

    return response.text


def write_parquet(last_day_of_month: str, locations: list) -> None:

    needed_columns = ['Address', 'Date time', 'Temperature', 'Relative Humidity', 'Wind Speed', 'Precipitation', 'Sea Level Pressure', 'Conditions']
    renamed_columns = [col.lower().replace(' ','_') for col in needed_columns]
    # with open('/opt/airflow/logs/ldm.txt', 'w') as f:
    #     f.write(last_day_of_month)
    for loc in locations:
        csv_text = get_weather_data(last_day_of_month, loc)
        csv_io = BytesIO(csv_text.encode())
        df = pv.read_csv(csv_io)
        df = df.select(needed_columns).rename_columns(renamed_columns)
        df = df.set_column(1, 'date_time',
                           pc.strptime(df.column("date_time"), format='%m/%d/%Y %H:%M:%S', unit='s'))
        city = loc.split(', ')[0]
        pq.write_table(df, f'weather_{city}_{last_day_of_month[:-3]}.parquet')
    


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, last_day_of_month: str, locations: list):
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
    bucket = client.bucket(bucket)

    for loc in locations:
        city = loc.split(', ')[0]

        file_name = f'weather_{city}_{last_day_of_month[:-3]}.parquet'

        blob = bucket.blob(os.path.join('raw', file_name))
        blob.upload_from_filename(os.path.join(path_to_local_home, file_name))


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022,6,1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="weather_data_ingestion",
    schedule_interval="0 6 1 * *",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['de-project'],
) as dag:

    write_parquet_task = PythonOperator(
        task_id="write_parquet_task",
        python_callable=write_parquet,
        op_kwargs={
            'last_day_of_month': last_day_of_month, 'locations': locations
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET, 'last_day_of_month': last_day_of_month, 'locations': locations
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/*"],
            },
        },
    )

    write_parquet_task >> local_to_gcs_task  >> bigquery_external_table_task