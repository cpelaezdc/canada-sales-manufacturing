from airflow.providers.postgres.operators.postgres import PostgresOperator

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