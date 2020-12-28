# Airflow Backup Configs

A maintenance workflow that you can deploy into Airflow to periodically take backups of various Airflow configurations and files.

## Deploy

1. Login to the machine running Airflow

2. Navigate to the dags directory

3. Copy the airflow-backup-configs.py file to this dags directory

       a. Here's a fast way:

                $ wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/master/backup-configs/airflow-backup-configs.py
        
4. Update the global variables (SCHEDULE_INTERVAL, DAG_OWNER_NAME, ALERT_EMAIL_ADDRESSES, BACKUP_FOLDER_DATE_FORMAT, BACKUP_HOME_DIRECTORY, BACKUPS_ENABLED, and BACKUP_RETENTION_COUNT) in the DAG with the desired values

6. Enable the DAG in the Airflow Webserver

