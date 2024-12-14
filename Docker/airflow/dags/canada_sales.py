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
    rows_per_chunk = num_rows // 10  # Adjust the divisor to control the number of chunks

    for i in range(10):
        start_idx = i * rows_per_chunk
        end_idx = min((i+1) * rows_per_chunk, num_rows)
        chunk_df = df.iloc[start_idx:end_idx]
        chunk_filename = os.path.join(os.path.dirname(file_path), f"chunk_{chunk_counter}.csv")
        chunk_df.to_csv(chunk_filename, index=False)
        chunk_counter += 1

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

    split_task