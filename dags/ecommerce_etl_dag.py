from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python import PythonOperator, task,
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta

from ..etl_utils import *

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2020, 8, 13),
    "email": ["thagana44@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}


with DAG("ECOMMERCE_ETL_DAG", default_args=default_args, schedule_interval=timedelta(days=1)) as dag: 

    sample_base_filepath = ''
    sample_remote_filepath = ''

    with TaskGroup("extraction_group") as extraction_group: 
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
        http_conn_id="mock-data-server-connection", # the connection will be set in Airflow Ui
        endpoint="/users", 
        method="GET",
        xcom_push=True,
        headers={"Content-Type": "application/json"},
        log_response=True
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
    with TaskGroup("processing_group") as processing_group:
        create_table_platinum_customer_table = PostgresOperator(
                    task_id='create_table_platinum_customer_table',
                    sql=create_table_query,
                        )
        demo_xcom_pull = PythonOperator(
            task_id='demo_xcom_pull',
            python_callable=demonstrate_xcom_pull
        ) 
        
        generate_basket_analysis_csv_task = PythonOperator(
            task_id='generate_basket_analysis_csv_task',
            python_callable=get_basket_analysis_dataset
        )
        
        generate_recommendation_engine_csv_task = PythonOperator(
            task_id='generate_recommendation_engine_csv_task',
            python_callable=get_recommendation_engine_dataset
        )
        
        get_platinum_customer_task = PythonOperator(
            task_id='get_platinum_customer_task',
            python_callable=get_platinum_customer
        )
        
    # we may want to send an email with files after processing. This can be done via an EmailOperator
    '''
        send_sample_dataset_email = EmailOperator(task_id='send_sample_dataset_email',
            to='thagana44@gmail.com',
            subject='Ecomm Industries Pipeline Report',
            html_content=""" <h1>Ecomm Industries Daily Summary and error report for {{ ds }}</h1> """,
            files=[f'{sample_base_filepath}/summary_data.csv', f'{sample_base_filepath}/error_logs.csv'],
            )
        
    '''
    
extraction_group >> processing_group