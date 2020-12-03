# Airflow Log Cleanup

A maintenance workflow that you can deploy into Airflow to periodically clean out the task logs to avoid those getting too big.

## Deploy

1. Login to the machine running Airflow

2. Navigate to the dags directory

3. ### airflow-log-cleanup.py ###
   1. Copy the airflow-log-cleanup.py file to this dags directory

          a. Here's a fast way:

                   $ wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/master/log-cleanup/airflow-log-cleanup.py
           
   2. Update the global variables (SCHEDULE_INTERVAL, DAG_OWNER_NAME, ALERT_EMAIL_ADDRESSES, ENABLE_DELETE and NUMBER_OF_WORKERS) in the DAG with the desired values

   3. Create and Set the following Variables in the Airflow Web Server (Admin -> Variables)

       - airflow_log_cleanup__max_log_age_in_days - integer - Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or older.
       - airflow_log_cleanup__enable_delete_child_log - boolean (True/False) - Whether to delete files from the Child Log directory defined under [scheduler] in the airflow.cfg file

   4. Enable the DAG in the Airflow Webserver
4. ### airflow-log-cleanup-pwdless-ssh.py ###
   1. Copy the airflow-log-cleanup.py file to this dags directory

          a. Here's a fast way:

                   $ wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/master/log-cleanup/airflow-log-cleanup-pwdless-ssh.py
           
   2. Update the global variables (SCHEDULE_INTERVAL, DAG_OWNER_NAME, ALERT_EMAIL_ADDRESSES, ENABLE_DELETE and AIRFLOW_HOSTS) in the DAG with the desired values

   3. Create and Set the following Variables in the Airflow Web Server (Admin -> Variables)

       - airflow_log_cleanup__max_log_age_in_days - integer - Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or older.
       - airflow_log_cleanup__enable_delete_child_log - boolean (True/False) - Whether to delete files from the Child Log directory defined under [scheduler] in the airflow.cfg file

   4. Ensure the `airflow` user can passwordless SSH on the hosts listed in `AIRFLOW_HOSTS`
   5. Enable the DAG in the Airflow Webserver
