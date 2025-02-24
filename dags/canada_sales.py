from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.models import DAG
from datetime import timedelta,datetime
from tasks.create_postgres_table import create_postgres_table
from tasks.load import load_file_log, load_data, load_sales
from tasks.transform import transform_naics, transform_provinces, transform_sales, transform_seasonal_adjustment
from tasks.extract import extract_sales
from tasks.utils import *
from tasks.dictionaries import *
import pandas as pd


DATASOURCE = "/usr/local/airflow/datasets"
#csv_input_folder = Variable.get("csv_input_folder")
start_date_canada_sales = Variable.get('PARAMETERS', deserialize_json=True)['start_date_canada_sales']
dataset_canada_sales = Variable.get('PARAMETERS', deserialize_json=True)['dataset_canada_sales']
csv_input_folder = Variable.get('PARAMETERS', deserialize_json=True)['csv_input_folder']

with DAG(
    dag_id='pipeline_canada_sales',
    start_date=datetime.now(),
    schedule_interval=timedelta(seconds=20),
    description="A pipeline to execute whene files exists in folder",
    catchup=False,
    max_active_runs=1
) as dag:
 
    file_sensor = FileSensor(
        task_id='file_sensor',
        filepath= os.path.join(csv_input_folder, '*.csv'),
        fs_conn_id='fs_default',
        poke_interval=20,
    )
  
    with TaskGroup("ETL_sales") as ETL_sales:

        extract_sales = PythonOperator(
            task_id='extract_sales',
            python_callable=extract_sales,
            op_kwargs={'csv_input_folder': csv_input_folder,
                       'columns_to_load': ['REF_DATE','GEO','DGUID','Seasonal adjustment','North American Industry Classification System (NAICS)','VALUE']}
        )

        transform_sales = PythonOperator (
            task_id='transform_sales',
            python_callable=transform_sales,
            op_kwargs={'prior_task': 'ETL_sales.extract_sales',
                       'column_mapping': column_mapping_sales}
        )

        extract_sales >> transform_sales

    with TaskGroup("create_tables") as create_tables:
        create_file_log_table = create_postgres_table(dag,"dim_file_log",dim_file_log_schema)
        create_province_table = create_postgres_table(dag,"dim_province",dim_province_schema)
        create_naics_table = create_postgres_table(dag,"dim_naics",dim_naics_schema)
        create_seasonal_adjustment_table = create_postgres_table(dag,"dim_seasonal_adjustment",dim_seasonal_adjustment_schema)
        create_sales_table = create_postgres_table(dag,"fact_sales",fact_sales_schema)

        [create_seasonal_adjustment_table,create_province_table,create_naics_table] >> create_sales_table

    with TaskGroup("ETL_seasonal_adjustment") as ETL_seasonal_adjustment:
        transform_seasonal_adjustment = PythonOperator(
            task_id='transform_seasonal_adjustment',
            python_callable=transform_seasonal_adjustment,
            op_kwargs={'prior_task': 'ETL_sales.transform_sales',
                       'column_mapping': column_mapping_seasonal_adjustments,}
        )

        load_seasonal_adjustment = PythonOperator(
            task_id='load_seasonal_adjustment',
            python_callable=load_data,
            op_kwargs={'key_df': 'df_seasonal_adjustment',
                        'table_name': 'dim_seasonal_adjustment',
                        'pk_column': 'id',
                        'prior_task': 'ETL_seasonal_adjustment.transform_seasonal_adjustment',
                        'postgres_conn_id': 'postgres_canada_sales'}
        )

        transform_seasonal_adjustment >> load_seasonal_adjustment

    with TaskGroup("ETL_provinces") as ETL_provinces:
        transform_provinces = PythonOperator(
            task_id='transform_provinces',
            python_callable=transform_provinces,
            op_kwargs={'path_file': f'{DATASOURCE}/canada_provinces_lon_lat.csvf',
                       'prior_task': 'ETL_sales.transform_sales',
                        'column_mapping': column_mapping_province}
        )

        load_provinces = PythonOperator(
            task_id='load_provinces',
            python_callable=load_data,
            op_kwargs={'key_df': 'df_province',
                        'table_name': 'dim_province',
                        'pk_column': 'id',
                        'prior_task': 'ETL_provinces.transform_provinces',
                        'postgres_conn_id': 'postgres_canada_sales'}
        )

        transform_provinces >> load_provinces

    with TaskGroup("ETL_naics") as ETL_naics:
        transform_naics = PythonOperator(
            task_id='transform_naics',
            python_callable=transform_naics,
            op_kwargs={'prior_task': 'ETL_sales.transform_sales',
                       'column_mapping': column_mapping_naics}
        )

        load_naics = PythonOperator(
            task_id='load_naics',
            python_callable=load_data,
            op_kwargs={'key_df': 'df_naics',
                        'table_name': 'dim_naics',
                        'pk_column': 'id',
                        'prior_task': 'ETL_naics.transform_naics',
                        'postgres_conn_id': 'postgres_canada_sales'}
        )

        transform_naics >> load_naics

    

    load_sales = PythonOperator(
            task_id='load_sales',
            python_callable=load_sales,
            op_kwargs={'table_name': 'fact_sales',
                    'prior_task': 'ETL_sales.transform_sales',
                    'postgres_conn_id': 'postgres_canada_sales'}
        )
    
    load_file_log = PythonOperator(
        task_id='load_file_log',
        python_callable=load_file_log,
        op_kwargs={ 'table_name': 'dim_file_log',
                    'prior_task': 'ETL_sales.extract_sales',
                    'postgres_conn_id': 'postgres_canada_sales'}
    )
    
    clean_directory = PythonOperator(
            task_id='clean_directory',
            python_callable=clean_directory,
            op_kwargs={'prior_task': 'ETL_sales.extract_sales'}
        )

   
    file_sensor >> ETL_sales >> create_tables >> [ETL_provinces, ETL_naics,ETL_seasonal_adjustment] >> load_sales >> load_file_log >> clean_directory
  
