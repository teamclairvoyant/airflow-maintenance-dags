# Airflow Log Cleanup

A maintenance workflow that you can deploy into Airflow to periodically clean out the task logs to avoid those getting too big.

- airflow-log-cleanup.py: Allows to delete logs by specifying the **number** of worker nodes. Does not guarantee log deletion of all nodes. 
- airflow-log-cleanup-pwdless-ssh.py: Allows to delete logs by specifying the list of worker nodes by their hostname. Requires the `airflow` user to have passwordless ssh to access all nodes.

## Deploy

1. Login to the machine running Airflow

2. Navigate to the dags directory

### airflow-log-cleanup.py

1. Copy the airflow-log-cleanup.py file to this dags directory

       a. Here's a fast way:

                $ wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/master/log-cleanup/airflow-log-cleanup.py

2. Update the global variables (SCHEDULE_INTERVAL, DAG_OWNER_NAME, ALERT_EMAIL_ADDRESSES, ENABLE_DELETE and NUMBER_OF_WORKERS) in the DAG with the desired values

3. Create and Set the following Variables in the Airflow Web Server (Admin -> Variables)

    - airflow_log_cleanup__max_log_age_in_days - integer - Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or older.
    - airflow_log_cleanup__enable_delete_child_log - boolean (True/False) - Whether to delete files from the Child Log directory defined under [scheduler] in the airflow.cfg file

   1. Enable the DAG in the Airflow Webserver

### airflow-log-cleanup-pwdless-ssh.py ###

1. Copy the airflow-log-cleanup-pwdless-ssh.py file to this dags directory

       a. Here's a fast way:

                $ wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/master/log-cleanup/airflow-log-cleanup-pwdless-ssh.py

2. Update the global variables (SCHEDULE_INTERVAL, DAG_OWNER_NAME, ALERT_EMAIL_ADDRESSES, ENABLE_DELETE and AIRFLOW_HOSTS) in the DAG with the desired values

3. Create and Set the following Variables in the Airflow Web Server (Admin -> Variables)

    - airflow_log_cleanup__max_log_age_in_days - integer - Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or older.
    - airflow_log_cleanup__enable_delete_child_log - boolean (True/False) - Whether to delete files from the Child Log directory defined under [scheduler] in the airflow.cfg file

4. Ensure the `airflow` user can passwordless SSH on the hosts listed in `AIRFLOW_HOSTS`
   1. Create a public and private key SSH key on all the worker nodes. You can follow these instructions: https://www.digitalocean.com/community/tutorials/how-to-set-up-ssh-keys--2
   2. Add the public key content to the ~/.ssh/authorized_keys file on all the other machines

5.  Enable the DAG in the Airflow Webserver
