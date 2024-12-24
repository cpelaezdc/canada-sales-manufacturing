import pandas as pd

def transform_sales(column_mapping,prior_task,ti):
    try:
        df_sales = ti.xcom_pull(key='df_extract_sales', task_ids=prior_task)
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
