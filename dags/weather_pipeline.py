from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from extract import extract
from transform import transform
from load import load

default_args = {
    'owner'           : 'bhavya',
    'retries'         : 2,
    'retry_delay'     : timedelta(minutes=5),
    'email_on_failure': False,
}

def run_extract(**context):
    data = extract()
    context['ti'].xcom_push(key='raw_data', value=data)

def run_transform(**context):
    raw_data = context['ti'].xcom_pull(
        key='raw_data',
        task_ids='extract_task'
    )
    df = transform(raw_data)
    context['ti'].xcom_push(
        key='transformed_data',
        value=df.to_dict('records')
    )

def run_load(**context):
    import pandas as pd
    records = context['ti'].xcom_pull(
        key='transformed_data',
        task_ids='transform_task'
    )
    df = pd.DataFrame(records)
    load(df)

with DAG(
    dag_id          = 'weather_pipeline',
    default_args    = default_args,
    description     = 'Weather API → S3 → PostgreSQL pipeline',
    schedule_interval = '@hourly',
    start_date      = datetime(2025, 1, 1),
    catchup         = False,
    tags            = ['weather', 'etl', 'pipeline'],
) as dag:

    extract_task = PythonOperator(
        task_id         = 'extract_task',
        python_callable = run_extract,
    )

    transform_task = PythonOperator(
        task_id         = 'transform_task',
        python_callable = run_transform,
    )

    load_task = PythonOperator(
        task_id         = 'load_task',
        python_callable = run_load,
    )

    extract_task >> transform_task >> load_task