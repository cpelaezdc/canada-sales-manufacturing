from airflow.models import Variable
import pandas as pd
from tasks.utils import find_oldest_file

def load_file_into_dataframe(directory_dataset,start_date_canada_sales,csv_input_folder,**kwargs):
    """Loads a CSV file into a Pandas DataFrame.

    Args:
        file_path (str): The path to the CSV file.
    """
    df = pd.read_csv(f'{directory_dataset}')
    print(df.head(2))
    
    # Validate if there is a date in airflow variable last_date_canada_sales
    last_date = Variable.get("last_date_canada_sales")

    if last_date == '0000-00':
        print("there are not date")
        last_date = start_date_canada_sales
        print(f"last date will be {last_date}")
    else:
        # Take the last date with format yyyy-mm and add 1 month
        last_date = pd.to_datetime(last_date)
        last_date = last_date + pd.DateOffset(months=1)
        last_date = last_date.strftime('%Y-%m')
        print(f"last date will be {last_date}")
    
    # Send to a new dataframe the rows where column REF_DATE is equal to last_date
    df_last_date = df[df['REF_DATE'] == last_date]
    # print number of rows
    print(f'number of rows with date {last_date} is {df_last_date.shape[0]}')
    # print first five rows
    print(df_last_date.head(5))

    # save to csv file this dataframe with the last date and replace file if exists
    # only if number of rows is greater than 0
    if df_last_date.shape[0] > 0:
        df_last_date.to_csv(f'{csv_input_folder}/{last_date}.csv', index=False)
        # validate that the file was created
        if f'{csv_input_folder}/{last_date}.csv':
            print(f"File {last_date}.csv created")
            # update airflow variable last_date_canada_sales
            Variable.set("last_date_canada_sales",last_date) 
    else:
        print(f"No rows found with date {last_date}")
 


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