from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from utils.etl_utils import *
from airflow.utils.dates import days_ago

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email": ["thagana44@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}


with DAG(
    "ECOMMERCE_ETL_DAG", default_args=default_args,
    schedule_interval=timedelta(days=1)
) as dag:

    sample_base_filepath = "../faux_data_lake"
    sample_remote_filepath = ""

    with TaskGroup("extraction_group") as extraction_group:
        extract_users_task = PythonOperator(
            task_id="extract_users_task", python_callable=pull_user_data
        )

        extract_products_task = PythonOperator(
            task_id="extract_products_task", python_callable=pull_product_data
        )

        extract_transaction_task = PythonOperator(
            task_id="extract_transaction_task",
            python_callable=pull_transaction_data)

        # demonstrate HttpOperator. In practice pulling data with lots of records may require
        # SFTPOperator or PythonOperator that can pull process and store data.
        demo_get_from_api = SimpleHttpOperator(
            task_id="demo_get_from_api",
            # the connection will be set in Airflow Ui
            http_conn_id="mock-data-server-connection",
            endpoint="/users",
            method="GET",
            do_xcom_push=True,
            headers={"Content-Type": "application/json"},
            log_response=True,
        )
        # sample SFTP for file download

        # download_file = SFTPOperator(
        #         task_id=f"download_file",
        #         ssh_conn_id="file_server",
        #         local_filepath=f"{sample_base_filepath}/filename",
        #         remote_filepath=f"{sample_base_filepath}/filename",
        #         operation=SFTPOperation.GET,
        #         create_intermediate_dirs=True,

        #         )

    # processing/transformation tasks
    create_table_platinum_customer_table = PostgresOperator(
            postgres_conn_id='mock_remote_db',
            task_id="create_table_platinum_customer_table",
            sql=create_table_query # can also be path to file eg sql/create_table_platinum_customer_table.sql
            )

    with TaskGroup("processing_group") as processing_group:
        generate_basket_analysis_csv_task = PythonOperator(
            task_id="generate_basket_analysis_csv_task",
            python_callable=get_basket_analysis_dataset,
        )

        generate_recommendation_engine_csv_task = PythonOperator(
            task_id="generate_recommendation_engine_csv_task",
            python_callable=get_recommendation_engine_dataset,
        )

        get_platinum_customer_task = PythonOperator(
            task_id="get_platinum_customer_task",
            python_callable=get_platinum_customer)

    # we may want to send an email with files after processing. This can be
    # done via an EmailOperator
    '''
        send_sample_dataset_email = EmailOperator(
            task_id='send_sample_dataset_email',
            to='thagana44@gmail.com',
            subject='Ecomm Industries Pipeline Report',
            html_content=""" <h1>Ecomm Industries Daily Summary and error report for {{ ds }}</h1> """,
            files=[f'{sample_base_filepath}/basket_analysis.csv', f'{sample_base_filepath}/recommendation_engine_analysis.csv'],
            )

    '''

extraction_group >> create_table_platinum_customer_table >> processing_group
