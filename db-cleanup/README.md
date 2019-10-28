# Airflow DB Cleanup

A maintenance workflow that you can deploy into Airflow to periodically clean out the DagRun, TaskInstance, Log, XCom, Job DB and SlaMiss entries to avoid having too much data in your Airflow MetaStore.

## Deploy

1. Login to the machine running Airflow

2. Navigate to the dags directory

3. Copy the airflow-db-cleanup.py file to this dags directory

       a. Here's a fast way:

                $ wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/master/db-cleanup/airflow-db-cleanup.py
        
4. Update the global variables (SCHEDULE_INTERVAL, DAG_OWNER_NAME, ALERT_EMAIL_ADDRESSES and ENABLE_DELETE) in the DAG with the desired values

5. Create and Set the following Variables in the Airflow Web Server (Admin -> Variables)

    - airflow_db_cleanup__max_db_entry_age_in_days - integer - Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or older.

5. Enable the DAG in the Airflow Webserver

