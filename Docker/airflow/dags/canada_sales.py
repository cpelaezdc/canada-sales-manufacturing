from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime
import pandas as pd
import os
import logging

DATASOURCE = "/opt/airflow/datasets"
OUTPUTPATH = "/opt/airflow/output"

# Define your column mapping
column_mapping_csv = {
    'REF_DATE': 'date',
    'GEO': 'geo',
    'DGUID': 'dguid',
    'Principal statistics' : 'principal',
    'Seasonal adjustment': 'seasonal_adjustment',
    'North American Industry Classification System (NAICS)': 'naics',
    'UOM': 'uom',
    'UOM_ID': 'uom_id',
    'SCALAR_FACTOR': 'scalar_factor',
    'SCALAR_ID': 'scalar_id',
    'VECTOR': 'vector',
    'COORDINATE': 'coordinate',
    'VALUE': 'value',
    'STATUS': 'status',
    'SYMBOL': 'symbol',
    'TERMINATED': 'terminated',
    'DECIMALS': 'decimals'
}

fact_sales_schema = [
        ("date", "VARCHAR(255)"),
        ("geo", "VARCHAR(255)"),
        ("dguid", "VARCHAR(255)"),
        ("principal", "VARCHAR(255)"),
        ("seasonal_adjustment", "VARCHAR(255)"),
        ("naics", "VARCHAR(255)"),
        ("uom", "VARCHAR(255)"),
        ("uom_id", "VARCHAR(255)"),
        ("scalar_factor", "VARCHAR(255)"),
        ("scalar_id", "VARCHAR(255)"),
        ("vector", "VARCHAR(255)"),
        ("coordinate", "VARCHAR(255)"),
        ("value", "VARCHAR(255)"),
        ("status", "VARCHAR(255)"),
        ("symbol", "VARCHAR(255)"),
        ("terminated", "VARCHAR(255)"),
        ("decimals", "VARCHAR(255)")
    ]

def split_csv(file_path, chunk_size=200):
    """Splits a CSV file into smaller chunks, saving them in the same directory.

    Args:
        file_path (str): Path to the CSV file.
        chunk_size (int, optional): Approximate size of each chunk in MB. Defaults to 200.
    """
    
    chunk_size_bytes = chunk_size * 1024 * 1024
    chunk_counter = 0

    df = pd.read_csv(file_path)
    num_rows = len(df)
    rows_per_chunk = num_rows // 20

    for i in range(19):  # Process the first 9 chunks
        start_idx = i * rows_per_chunk
        end_idx = (i+1) * rows_per_chunk
        chunk_df = df.iloc[start_idx:end_idx]
        chunk_filename = os.path.join(OUTPUTPATH, f"chunk_{chunk_counter}.csv")
        chunk_df.to_csv(chunk_filename, index=False)
        chunk_counter += 1

    # Process the last chunk, including any remaining rows
    start_idx = 9 * rows_per_chunk
    end_idx = num_rows
    last_chunk_df = df.iloc[start_idx:end_idx]
    #last_chunk_filename = os.path.join(os.path.dirname(file_path), f"chunk_{chunk_counter}.csv")
    last_chunk_filename = os.path.join(OUTPUTPATH, f"chunk_{chunk_counter}.csv")
    last_chunk_df.to_csv(last_chunk_filename, index=False)


def load_data_to_postgres(file_path,table_name,postgres_conn_id,column_mapping):
    """Loads the extracted data into a PostgreSQL table."""
    df = pd.read_csv(file_path)

    print('before')

     # Rename the columns
    #df.rename(columns=column_mapping, inplace=True)
    df.rename(columns=column_mapping)

    print('after')
    
    # Establish a connection to PostgresSQL
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    # Insert data into the specified table
    df.to_sql(table_name,engine,if_exists='append',index=False)
    print(f"Data from {file_path} inserted into table {table_name}")

def create_postgres_table(dag,table_name,columns):
    """Creates a PostgreSQL table using an Airflow operator
    
    Args:
        dag:  The Airflow DAG object
        table_name: The name of the taable to create
        columns: A list of tuples, where each tuple contains the column name and data type.
    """

    sql_query= f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {", ".join([f"{column_name} {data_type}" for column_name,data_type in columns])}
        
        )
    """

    create_table_task = PostgresOperator(
        task_id=f"create_{table_name}_table",
        postgres_conn_id="postgres_canada_sales", #Replace with corresponding connection
        sql=sql_query,
        dag=dag
    )

    return create_table_task


with DAG(
    'split_csv_dag',
    start_date=days_ago(1),
    schedule_interval=None,
) as dag:

    create_sales_table = create_postgres_table(dag,"fact_sales",fact_sales_schema)

    split_task = PythonOperator(
        task_id='split_csv',
        python_callable=split_csv,
        op_kwargs={'file_path': f'{DATASOURCE}/16100048.csv'}
    )

    load_data_to_postgres = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_postgres,
        op_kwargs={'file_path': f'{OUTPUTPATH}/chunk_0.csv',
                   'table_name': 'fact_sales',
                   'postgres_conn_id': 'postgres_canada_sales',
                   'column_mapping': f'{column_mapping_csv}'},
        dag=dag,
    )

    
    create_sales_table >> split_task >> load_data_to_postgres