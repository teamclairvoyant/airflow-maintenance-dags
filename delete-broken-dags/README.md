# Airflow Delete Broken DAGs

A maintenance workflow that you can deploy into Airflow to periodically delete DAG files and clean out entries in the
ImportError table for DAGs which Airflow cannot parse or import properly. This ensures that the ImportError table is cleaned every day.

## Deploy

1. Login to the machine running Airflow

2. Navigate to the dags directory

3. Copy the airflow-delete-broken-dags.py file to this dags directory

       a. Here's a fast way:

                $ wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/master/delete-broken-dags/airflow-delete-broken-dags.py
        
4. Update the global variables (SCHEDULE_INTERVAL, DAG_OWNER_NAME, ALERT_EMAIL_ADDRESSES and ENABLE_DELETE) in the DAG with the desired values

5. Enable the DAG in the Airflow Webserver
