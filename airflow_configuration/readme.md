## CONFIGURATION IN AIRFLOW UI

*  Import in Airflow/Admin/Variables - [variables.json](variables.json)
*  Import not declared in Admin/Variables are declared in .env variable.
```
AIRFLOW_VAR_PARAMETERS='{"csv_input_folder":"/usr/local/airflow/datasets/extraction","dataset_canada_sales":"16100048.csv","start_date_canada_sales":"1992-01"}'
```




*  Add Postgress connection in Airflow/Admin/Connections [posgress-connection](postgres.png)