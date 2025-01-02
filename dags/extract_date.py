from airflow.decorators import dag, task
from datetime import timedelta,datetime
from airflow.models import Variable
import pandas as pd

DATASOURCE = "/usr/local/airflow/datasets"

@dag(
    start_date=datetime.now(),
    schedule_interval=timedelta(seconds=10),
    catchup=False,
    max_active_runs=1
)

def extract_date_load_csv():
    
    @task
    def load_date_to_csv(find_date, source_file, dest_folder):
        # Your existing load_file_into_dataframe function implementation
        """Extract data from a specifique date and save it to a CSV file.
        Args:
            find_date (str): The start date to extract data.
            source_file (str): The path to the CSV file.
            dest_folder (str): The path to the folder where the CSV file will be saved.
        """
        df = pd.read_csv(f'{source_file}')
        print(df.head(2))
        
        # Validate if there is a date in airflow variable last_date_canada_sales
        last_date = Variable.get("last_date_canada_sales")

        if last_date == '0000-00':
            print("there are not date")
            last_date = find_date
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
            df_last_date.to_csv(f'{dest_folder}/{last_date}.csv', index=False)
            # validate that the file was created
            if f'{dest_folder}/{last_date}.csv':
                print(f"File {last_date}.csv created")
                # update airflow variable last_date_canada_sales
                Variable.set("last_date_canada_sales",last_date) 
        else:
            print(f"No rows found with date {last_date}")

    load_date_to_csv(
        find_date=   Variable.get('PARAMETERS', deserialize_json=True)['start_date_canada_sales'],
        source_file= f'{DATASOURCE}/{Variable.get('PARAMETERS', deserialize_json=True)['dataset_canada_sales']}',
        dest_folder= Variable.get('PARAMETERS', deserialize_json=True)['csv_input_folder']
    )

extract_date_load_csv()

    


