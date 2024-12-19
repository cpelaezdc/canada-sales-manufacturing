from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator 
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
import pandas as pd
import os
import re

DATASOURCE = "/opt/airflow/datasets"
OUTPUTPATH = "/opt/airflow/output"
number_of_chuncks = 19

# Define your column mapping
column_mapping_sales = {
    'REF_DATE': 'date_sales',
    'GEO': 'name_province',
    'DGUID': 'id_province',
    'Seasonal adjustment': 'name_seasonal_adjustment',
    'North American Industry Classification System (NAICS)': 'name_naics',
    'VALUE': 'value',
}

column_mapping_seasonal_adjustments = {
    'name_seasonal_adjustment': 'name'
}

column_mapping_province = {
    'id_province': 'id',
    'name_province': 'name'
}

column_mapping_naics = {
    'id_naics': 'id',
    'name_naics': 'name'
}

dim_province_schema = [
    ("id","INT PRIMARY KEY"),
    ("name","VARCHAR(50)"),
    ("latitude","VARCHAR(20)"),
    ("longitude","VARCHAR(20)")
]

dim_naics_schema = [
    ("id","VARCHAR(10) PRIMARY KEY"),
    ("name","VARCHAR(100)")
]

fact_sales_schema = [
    ("date_sales", "DATE"),
    ("id_province","INT REFERENCES dim_province(id)"),
    ("id_seasonal","INT REFERENCES dim_seasonal_adjustment(id)"),
    ("id_naics",   "VARCHAR(10) REFERENCES dim_naics(id)"),
    ("value", "double precision")
]

dim_seasonal_adjustment_schema = [
    ("id","INT PRIMARY KEY"),
    ("name","VARCHAR(50)")
]

def split_file(file_path, chunk_size=200):
    """Splits a CSV file into smaller chunks, saving them in the same directory.

    Args:
        file_path (str): Path to the CSV file.
        chunk_size (int, optional): Approximate size of each chunk in MB. Defaults to 200.
    """
    
    chunk_size_bytes = chunk_size * 1024 * 1024
    chunk_counter = 0

    df = pd.read_csv(file_path)
    # drop not necessary columns
    df = df.drop(['Principal statistics','UOM','UOM_ID','SCALAR_FACTOR','SCALAR_ID','VECTOR','COORDINATE','STATUS','SYMBOL','TERMINATED','DECIMALS'],axis=1)
    # delete rows with nulls
    df = df.dropna()

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


def extract_csv_data(path_files,columns_name,**kwargs):
    """Extracts data from a CSV file and stores it in XCom."""
    df_unique = pd.DataFrame()

    # Check all files to get all the provinces
    for i in range(number_of_chuncks):  # Process all chuncks
        chunk_filename = os.path.join(path_files,f"chunk_{i}.csv")
        columns_to_load = columns_name
        #specify columns to load
        df = pd.read_csv(chunk_filename,usecols=columns_to_load)
        # drop duplicates
        df_unique = pd.concat([df_unique,df.drop_duplicates()], ignore_index=True,join='outer')

        df_unique = df_unique.drop_duplicates()
    
    kwargs['ti'].xcom_push(key='df_unique', value=df_unique.to_dict())


def transform_sales(column_mapping,prior_task,ti):
    try:
        df_sales = ti.xcom_pull(key='df_unique', task_ids=prior_task)
        df_sales = pd.DataFrame.from_dict(df_sales)
    except Exception as e:
        print(f"Error pullin XCom: {e}")

    print(list(df_sales.columns))
    print(f'number of items before transform {str(df_sales.shape[0])}')

    df_sales.rename(columns=column_mapping, inplace=True)

    # transform date to insert as date type, add day as 01
    df_sales['date_sales'] = df_sales['date_sales'] + '-01'
    # The last two chars is the province id
    df_sales['id_province'] = df_sales['id_province'].str[-2:]

    # Extract the id using a regular expression
    df_sales['id_naics'] = df_sales['name_naics'].str.extract( r' \[(.*?)\]')
    # Remove the code from the name column if needed 
    df_sales['name_naics'] = df_sales['name_naics'].str.replace(r'\s* \[.*\]', '', regex=True)

    print(list(df_sales.columns))
    # Get the unique values of the seasonal adjustment
    df_sales_to_seasonal_adjustment = df_sales[['name_seasonal_adjustment']].drop_duplicates().copy()
    # Get the unique values of the naics 
    df_sales_to_naics = df_sales[['id_naics','name_naics']].drop_duplicates().copy()
    # Get the unique values of the province
    df_sales_to_province = df_sales[['id_province','name_province']].drop_duplicates().copy()
    # Drop duplicates from the main dataframe and drop nulls
    df_sales = df_sales[['date_sales','id_province','name_seasonal_adjustment','id_naics','value']].drop_duplicates().copy()   
    df_sales = df_sales.dropna()
    
    print(df_sales.head(5))
    print(f'number of items after transform {str(df_sales.shape[0])}')

    ti.xcom_push(key='df_sales', value=df_sales.to_dict())
    ti.xcom_push(key='df_sales_to_seasonal_adjustment', value=df_sales_to_seasonal_adjustment.to_dict())
    ti.xcom_push(key='df_sales_to_naics', value=df_sales_to_naics.to_dict())
    ti.xcom_push(key='df_sales_to_province', value=df_sales_to_province.to_dict())

def transform_seasonal_adjustment(column_mapping,prior_task,ti):
    try:
        df_seasonal_adjustment = ti.xcom_pull(key='df_sales_to_seasonal_adjustment', task_ids=prior_task)
        df_seasonal_adjustment = pd.DataFrame.from_dict(df_seasonal_adjustment)
    except Exception as e:
        print(f"Error pullin XCom: {e}")

    print(f'number of items before transform {str(df_seasonal_adjustment.shape[0])}')

    df_seasonal_adjustment.rename(columns=column_mapping, inplace=True)

    df_seasonal_adjustment = df_seasonal_adjustment.drop_duplicates()
    df_seasonal_adjustment = df_seasonal_adjustment.dropna()

    # Add an id column
    df_seasonal_adjustment['id'] = range(1, len(df_seasonal_adjustment) + 1)
        
    print(df_seasonal_adjustment.head(5))
    print(f'number of items after transform {str(df_seasonal_adjustment.shape[0])}')

    ti.xcom_push(key='df_seasonal_adjustment', value=df_seasonal_adjustment.to_dict())

def transform_provinces(path_file,column_mapping,prior_task,ti):
    try:
        df_province = ti.xcom_pull(key='df_sales_to_province', task_ids=prior_task)
        df_province = pd.DataFrame.from_dict(df_province)
    except Exception as e:
        print(f"Error pullin XCom: {e}")

    df_province.rename(columns=column_mapping, inplace=True)

    print(df_province.head(5))
    
    df_province = df_province.drop_duplicates()
    #drop any nulls
    df_province = df_province.dropna()
    # Merge with another dataframe to get longitude and latitude
    province_lon_lat = path_file
    df_province_lon_lat = pd.read_csv(province_lon_lat)
    df_province = pd.merge(df_province, df_province_lon_lat, on='name')

    print('Number of provinces: ' + str(df_province.shape[0]))
    print(df_province)

    ti.xcom_push(key='df_province', value=df_province.to_dict())

def transform_naics(column_mapping,prior_task,ti):
    try:
        df_naics = ti.xcom_pull(key='df_sales_to_naics', task_ids=prior_task)
        df_naics = pd.DataFrame.from_dict(df_naics)
    except Exception as e:
        print(f"Error pullin XCom: {e}")

    print(f'print first five: {df_naics.head(5)}')    
    print(f'number of items before transform {str(df_naics.shape[0])}')

    # Delete null values and drop duplicates
    df_naics = df_naics.dropna()
    df_naics = df_naics.drop_duplicates()
    # rename columns
    df_naics.rename(columns=column_mapping, inplace=True)

    # Trim the naics column 
    df_naics['name'] = df_naics['name'].str.strip()
    # print null values
    df_nulls = df_naics[df_naics.isnull().any(axis=1)]
    print(df_nulls)
    
    print(f'print first five: {df_naics.head(5)}')    
    print(f'number of items after transform {str(df_naics.shape[0])}')

    ti.xcom_push(key='df_naics', value=df_naics.to_dict())

def load_data(table_name,pk_column,postgres_conn_id,key_df,prior_task,**kwargs):
    try:
        df = kwargs['ti'].xcom_pull(key=key_df, task_ids=prior_task)
        df = pd.DataFrame.from_dict(df)
    except Exception as e:
        print(f"Error pullin XCom: {e}")

    print(f'key is {key_df} and previous task is {prior_task}')
    
     # Establish a connection to PostgresSQL
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    # Validate that doesn't exist in table
    existing_data = pd.read_sql_query(f"SELECT * FROM {table_name}",engine)
    print(f'types in database: {existing_data.dtypes}')
    print(f'types in dataframe: {df.dtypes}')

    # Change to numeric to compare with dataframe from table (id is integer in table)
    if df[pk_column].dtype != existing_data[pk_column].dtype:
        if existing_data[pk_column].dtype == 'int64':
            df[pk_column] = pd.to_numeric(df[pk_column],errors='coerce')
    
    #  Get only the new rows, filter by id
    df_new_rows = df[~df[pk_column].isin(existing_data[pk_column])].dropna()

    # Insert data into the specified table
    if not df_new_rows.empty:
        df_new_rows.to_sql(table_name,engine,if_exists='append',index=False)
        print(f"Data from {key_df} dataframe inserted into table {table_name}")
    else:
        print("No new rows to insert")


def load_sales(table_name,postgres_conn_id,prior_task,ti):
    """Loads the extracted data into a PostgreSQL table."""
    try:
        df_sales = ti.xcom_pull(key='df_sales', task_ids=prior_task)
        df_sales = pd.DataFrame.from_dict(df_sales)
        df_seasonal_adjustment = ti.xcom_pull(key='df_seasonal_adjustment', task_ids='ETL_seasonal_adjustment.transform_seasonal_adjustment')
        df_seasonal_adjustment = pd.DataFrame.from_dict(df_seasonal_adjustment)
    except Exception as e:
        print(f"Error pullin XCom: {e}")

    print(f'number of items before load {str(df_sales.shape[0])}')

    # Merge the dataframes to get the id of the seasonal adjustment
    df_sales = pd.merge(df_sales, df_seasonal_adjustment, left_on='name_seasonal_adjustment', right_on='name', how='inner') 
    df_sales = df_sales.drop(['name_seasonal_adjustment','name'],axis=1)
    df_sales.rename(columns={'id':'id_seasonal'}, inplace=True) 

    # Establish a connection to PostgresSQL
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    # Insert data into the specified table
    df_sales.to_sql(table_name,engine,if_exists='append',index=False)
    
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

def clean_directory(path_files):
    """Finds all files in a directory that start with 'chunk'.
         Args:
            directory: The directory to search.
        Returns:
            A list of file paths.
    """
    for filename in os.listdir(path_files):
        if filename.startswith("chunk"):
            file_path = os.path.join(path_files, filename)
            os.remove(file_path)
            print(f"Deleted file: {file_path}")

with DAG(
    'pipeline_canada_sales',
    start_date=days_ago(1),
    schedule_interval=None,
) as dag:

    with TaskGroup("create_tables") as create_tables:
        create_province_table = create_postgres_table(dag,"dim_province",dim_province_schema)
        create_naics_table = create_postgres_table(dag,"dim_naics",dim_naics_schema)
        create_seasonal_adjustment_table = create_postgres_table(dag,"dim_seasonal_adjustment",dim_seasonal_adjustment_schema)
        create_sales_table = create_postgres_table(dag,"fact_sales",fact_sales_schema)

        [create_seasonal_adjustment_table,create_province_table,create_naics_table] >> create_sales_table

    split_file = PythonOperator(
        task_id='split_file',
        python_callable=split_file,
        op_kwargs={'file_path': f'{DATASOURCE}/16100048.csv'}
    )

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

    with TaskGroup("ETL_sales") as ETL_sales:

        extract_sales = PythonOperator(
            task_id='extract_sales',
            python_callable=extract_csv_data,
            op_kwargs={'path_files': f'{OUTPUTPATH}',
                       'columns_name': ['REF_DATE','GEO','DGUID','Seasonal adjustment','North American Industry Classification System (NAICS)','VALUE']}
        )

        transform_sales = PythonOperator (
            task_id='transform_sales',
            python_callable=transform_sales,
            op_kwargs={'prior_task': 'ETL_sales.extract_sales',
                       'column_mapping': column_mapping_sales}
        )

        extract_sales >> transform_sales

    load_sales = PythonOperator(
            task_id='load_sales',
            python_callable=load_sales,
            op_kwargs={'path_files': f'{OUTPUTPATH}',
                    'table_name': 'fact_sales',
                    'prior_task': 'ETL_sales.transform_sales',
                    'postgres_conn_id': 'postgres_canada_sales'}
        )
    
    clean_directory = PythonOperator(
            task_id='clean_directory',
            python_callable=clean_directory,
            op_kwargs={'path_files': f'{OUTPUTPATH}'}
        )

   
    [create_tables,split_file] >> ETL_sales >> [ETL_provinces, ETL_naics,ETL_seasonal_adjustment] >> load_sales >> clean_directory
    
  