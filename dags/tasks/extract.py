import pandas as pd
from tasks.utils import find_oldest_file


def extract_sales(columns_to_load,csv_input_folder,**kwargs):
    """Extracts data from a CSV file and stores it in XCom."""
    # find in airflow variable the directory where the files are stored
    path_files = csv_input_folder
    print(f"Path files: {path_files}")

    # Get the oldest file in the folder bassed in the file name YYY-MM
    try:
        file_to_process = find_oldest_file(path_files)
        print(f"File to process: {file_to_process}")
            # load the file into a dataframe
        df = pd.read_csv(file_to_process,usecols=columns_to_load)
        # Get the number of rows
        print(f'number of items before transform {str(df.shape[0])}')
        print(f'columns: {list(df.columns)}')
        print(f'the 5 first rows are: {df.head(5)}')

        # Drop rows with null values
        df = df.dropna()
        # Drop duplicates
        df = df.drop_duplicates()
        # Get the number of rows
        print(f'number of items after transform {str(df.shape[0])}')
        print(f'the 5 first rows are: {df.head(5)}')

        # Send the dataframe to XCom
        kwargs['ti'].xcom_push(key='df_extract_sales', value=df.to_dict())
        # send the file name to XCom
        kwargs['ti'].xcom_push(key='file_name', value=file_to_process)  

        return True

    except Exception as e:
        print(f"There are not files")
        print(f"An error occurred: {e}")
        return False