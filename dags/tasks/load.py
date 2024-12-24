from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import os

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


def load_file_log(table_name,postgres_conn_id,prior_task,ti):
    """Loads the extracted data into a PostgreSQL table."""
    try:
        file_name = ti.xcom_pull(key='file_name', task_ids=prior_task)
    except Exception as e:
        print(f"Error pullin XCom: {e}")

    # Establish a connection to PostgresSQL
    postgres_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()

    # extract for file_name variable name only the file without the path
    file_name = os.path.basename(file_name)

    # Insert data into the specified table
    df = pd.DataFrame({'date_sales': [file_name[:7]], 'file_name': [file_name], 'file_date': [datetime.now()], 'status': ['Processed']})
    df.to_sql(table_name,engine,if_exists='append',index=False)
