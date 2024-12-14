from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
import os

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
    rows_per_chunk = num_rows // 10

    for i in range(9):  # Process the first 9 chunks
        start_idx = i * rows_per_chunk
        end_idx = (i+1) * rows_per_chunk
        chunk_df = df.iloc[start_idx:end_idx]
        chunk_filename = os.path.join(os.path.dirname(file_path), f"chunk_{chunk_counter}.csv")
        chunk_df.to_csv(chunk_filename, index=False)
        chunk_counter += 1

    # Process the last chunk, including any remaining rows
    start_idx = 9 * rows_per_chunk
    end_idx = num_rows
    last_chunk_df = df.iloc[start_idx:end_idx]
    last_chunk_filename = os.path.join(os.path.dirname(file_path), f"chunk_{chunk_counter}.csv")
    last_chunk_df.to_csv(last_chunk_filename, index=False)

def read_csv_file():
    #df = pd.read_csv('/Users/loonycorn/airflow/datasets/insurance.csv')
    df = pd.read_csv('/opt/airflow/datasets/chunk_0.csv')

    print(df)

    return df.to_json()


with DAG(
    'split_csv_dag',
    start_date=days_ago(2),
    schedule_interval=None,
) as dag:

    split_task = PythonOperator(
        task_id='split_csv',
        python_callable=split_csv,
        op_kwargs={'file_path': '/opt/airflow/datasets/16100048.csv'}
    )

    read_csv_file = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_file
    )

    split_task >> read_csv_file