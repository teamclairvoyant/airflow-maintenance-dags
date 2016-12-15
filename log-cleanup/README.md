# Airflow Log Cleanup

A maintenance workflow that you can deploy into Airflow to periodically clean out the task logs to avoid those getting too big.

## Deploy

1. Login to the machine running Airflow

2. Navigate to the dags directory

3. Copy the airflow-log-cleanup.py file to this dags directory

       a. Here's a fast way:

                $ wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/master/airflow-maintenance-dags/log-cleanup/airflow-log-cleanup.py?token=ABEar_oJJ2aOhlFDqBnBGmxYS8EvO4aFks5YUxCBwA%3D%3D -O airflow-log-cleanup.py
        
4. Update the global variables in the DAG with the desired values 

5. Enable the DAG in the Airflow Webserver
