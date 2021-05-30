from airflow.dags import DAG
from datetime import datetime, timedelta
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 8, 13),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG("ECOMMERCE_ETL_DAG", default_args=default_args, schedule_interval=timedelta(days=1))