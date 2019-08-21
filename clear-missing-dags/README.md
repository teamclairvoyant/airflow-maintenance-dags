# Airflow Clear Missing DAGs

A maintenance workflow that you can deploy into Airflow to periodically clean out entries in the DAG table of which there is no longer a corresponding Python File for it. This ensures that the DAG table doesn't have needless items in it and that the Airflow Web Server displays only those available DAGs.  

## Deploy

1. Login to the machine running Airflow

2. Navigate to the dags directory

3. Copy the airflow-clear-missing-dags.py file to this dags directory

       a. Here's a fast way:

                $ wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/master/clear-missing-dags/airflow-clear-missing-dags.py
        
4. Update the global variables in the DAG with the desired values 

5. Enable the DAG in the Airflow Webserver
