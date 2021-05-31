from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python PythonOperator
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