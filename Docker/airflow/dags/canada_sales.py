from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
import pandas as pd
import os


DATASOURCE = "/opt/airflow/datasets"
OUTPUTPATH = "/opt/airflow/output"
number_of_chuncks = 19

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

column_mapping_province = {
    'DGUID': 'id',
    'GEO': 'name'
}

dim_province_schema = [
    ("id","INT PRIMARY KEY"),
    ("name","VARCHAR(50)"),
    ("latitude","VARCHAR(20)"),
    ("longitude","VARCHAR(20)")
]

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

    for i in range(number_of_chuncks):  # Process the first 9 chunks
        start_idx = i * rows_per_chunk

        if i == (number_of_chuncks-1):
            end_idx = num_rows
        else:
            end_idx = (i+1) * rows_per_chunk
        
        chunk_df = df.iloc[start_idx:end_idx]
        chunk_filename = os.path.join(OUTPUTPATH, f"chunk_{chunk_counter}.csv")
        chunk_df.to_csv(chunk_filename, index=False)
        chunk_counter += 1


def extract_csv_data(path_files,**kwargs):
    """Extracts data from a CSV file and stores it in XCom."""
    df_unique = pd.DataFrame()

    # Check all files to get all the provinces
    for i in range(number_of_chuncks):  # Process all chuncks
        chunk_filename = os.path.join(path_files,f"chunk_{i}.csv")
        columns_to_load = ['DGUID','GEO']
        #specify columns to load
        df = pd.read_csv(chunk_filename,usecols=columns_to_load)
        # drop duplicates
        df_unique = pd.concat([df_unique,df.drop_duplicates()], ignore_index=True,join='outer')
    
    kwargs['ti'].xcom_push(key='df_unique', value=df_unique.to_dict())


def transform_provinces(path_file,column_mapping,task_name,ti):
    try:
        df_province = ti.xcom_pull(key='df_unique', task_ids=task_name)
        df_province = pd.DataFrame.from_dict(df_province)
    except Exception as e:
        print(f"Error pullin XCom: {e}")

    df_province.rename(columns=column_mapping, inplace=True)

    print(df_province)
    
    df_province = df_province.drop_duplicates()
    # The last to chars in the string is the id province
    df_province['id'] = df_province['id'].str[-2:]
    #drop any nulls
    df_province = df_province.dropna()
    # Merge with another dataframe to get longitude and latitude
    province_lon_lat = path_file
    df_province_lon_lat = pd.read_csv(province_lon_lat)
    df_province = pd.merge(df_province, df_province_lon_lat, on='name')

    print('Number of provinces: ' + str(df_province.shape[0]))
    print(df_province)

    ti.xcom_push(key='df_province', value=df_province.to_dict())

def load_data(table_name,pk_column,postgres_conn_id,key_df,task_name,**kwargs):
    df = kwargs['ti'].xcom_pull(key=key_df, task_ids=task_name)
    df = pd.DataFrame.from_dict(df)

    print(f'key is {key_df} and previous task is {task_name}')
    print(df)

     # Establish a connection to PostgresSQL
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    # Validate that doesn't exist in table
    existing_data = pd.read_sql_query(f"SELECT * FROM {table_name}",engine)
    # Change to numeric to compare with dataframe from table (id is integer in table)
    df[pk_column] = pd.to_numeric(df[pk_column],errors='coerce')
    #  Get only the new rows, filter by id
    df_new_rows = df[~df[pk_column].isin(existing_data[pk_column])].dropna()

    # Insert data into the specified table
    if not df_new_rows.empty:
        df_new_rows.to_sql(table_name,engine,if_exists='append',index=False)
        print(f"Data from {key_df} dataframe inserted into table {table_name}")
    else:
        print("No new rows to insert")


def load_data_to_postgres(path_files,table_name,postgres_conn_id,column_mapping):
    """Loads the extracted data into a PostgreSQL table."""

    for i in range(number_of_chuncks):  # Process all chuncks
        chunk_filename = os.path.join(path_files,f"chunk_{i}.csv")

        print(chunk_filename)

        df = pd.read_csv(chunk_filename)
        
        # Rename the columns
        df.rename(columns=column_mapping_csv, inplace=True)

        print(df.shape[0])

        # Establish a connection to PostgresSQL
        postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        engine = postgres_hook.get_sqlalchemy_engine()

        # Insert data into the specified table
        df.to_sql(table_name,engine,if_exists='append',index=False)
        print(f"Data from {chunk_filename} inserted into table {table_name}")


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

    with TaskGroup("create_tables") as create_tables:
        create_province_table = create_postgres_table(dag,"dim_province",dim_province_schema)
        create_sales_table = create_postgres_table(dag,"fact_sales",fact_sales_schema)

        create_province_table >> create_sales_table

    with TaskGroup("ETL_provinces") as ETL_provinces:
        extract_provinces = PythonOperator(
            task_id='extract_provinces',
            python_callable=extract_csv_data,
            op_kwargs={'path_files': f'{OUTPUTPATH}',
                        'table_name': 'dim_province'}
        )

        transform_provinces = PythonOperator(
            task_id='transform_provinces',
            python_callable=transform_provinces,
            op_kwargs={'path_file': f'{DATASOURCE}/canada_provinces_lon_lat.csvf',
                       'task_name': 'ETL_provinces.extract_provinces',
                        'column_mapping': column_mapping_province}
        )

        load_provinces = PythonOperator(
            task_id='load_provinces',
            python_callable=load_data,
            op_kwargs={'key_df': 'df_province',
                        'table_name': 'dim_province',
                        'pk_column': 'id',
                        'task_name': 'ETL_provinces.transform_provinces',
                        'postgres_conn_id': 'postgres_canada_sales'}
        )

        extract_provinces >> transform_provinces >> load_provinces

    
    """split_task = PythonOperator(
        task_id='split_csv',
        python_callable=split_csv,
        op_kwargs={'file_path': f'{DATASOURCE}/16100048.csv'}
    )"""

    """load_data_to_postgres = PythonOperator(
        task_id='load_data_to_postgres',
        python_callable=load_data_to_postgres,
        op_kwargs={'path_files': f'{OUTPUTPATH}',
                   'table_name': 'fact_sales',
                   'postgres_conn_id': 'postgres_canada_sales',
                   'column_mapping': column_mapping_csv},
        dag=dag,
    )"""
    
    create_tables >> ETL_provinces
    
    #create_tables >> split_task >> ETL_provinces >> load_data_to_postgres
    