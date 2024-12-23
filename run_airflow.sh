export AIRFLOW_HOME="/workspaces/canada-sales-data-engineering-project/Docker/airflow"
echo "AIRFLOW_HOME is set to: $AIRFLOW_HOME"

airflow webserver -D
airflow scheduler -D