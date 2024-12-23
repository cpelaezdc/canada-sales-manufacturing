export AIRFLOW_HOME="/workspaces/canada-sales-data-engineering-project/Docker/airflow"
echo "AIRFLOW_HOME is set to: $AIRFLOW_HOME"

# Get Actively Running PID of webserver
WEBSERVER_PID=$(ps -ef | grep -i webserver | grep master | grep -v grep | awk '{print $2}')
echo "Killing webserver running with pid $WEBSERVER_PID"
kill $WEBSERVER_PID

# Get Actively Running PID of scheduler
SCHEDULER_PID=$(ps -ef | grep -i scheduler | grep -v grep | grep -v DagFileProcessorManager | awk '{print $2}')
echo "Killing scheduler running with pid $SCHEDULER_PID"
kill $SCHEDULER_PID

echo "Airflow stopped."
