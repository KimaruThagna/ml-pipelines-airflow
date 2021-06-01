from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python import PythonOperator,
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta

from ..etl_utils import *

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 8, 13),
    "email": ["kimaru@thagana.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

with DAG("ECOMMERCE_ETL_DAG", default_args=default_args, schedule_interval=timedelta(days=1)) as dag: 
    with TaskGroup("Extraction_Tasks") as extraction_group: 
        extract_users_task = PythonOperator(
            task_id='extract_users_task',
            python_callable=pull_user_data
        )
        
        extract_products_task = PythonOperator(
            task_id='extract_products_task',
            python_callable=pull_product_data
        )
         
        extract_teansaction_task = PythonOperator(
            task_id='extract_teansaction_task',
            python_callable=pull_transaction_data
        )
        #demonstrate HttpOperator. In practice pulling data with lots of records may require
        #SFTPOperator or PythonOperator that can pull process and store data.
        demo_get_from_api = SimpleHttpOperator(
        task_id="demo_get_from_api",
        http_conn_id="atlassian_marketplace",
        endpoint="/users", 
        method="GET",
        xcom_push=True,
        log_response=True
    )