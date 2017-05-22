from airflow.models import DAG, DagRun, TaskInstance, Log, XCom, settings
from airflow.operators import PythonOperator
from datetime import datetime, timedelta
import os
import logging

"""
A maintenance workflow that you can deploy into Airflow to periodically clean out the DagRun, TaskInstance, Log and XCom DB entries to avoid having too much data in your Airflow MetaStore.

airflow trigger_dag --conf '{"maxDBEntryAgeInDays":30}' airflow-db-cleanup

--conf options:
    maxDBEntryAgeInDays:<INT> - Optional

"""

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")  # airflow-db-cleanup
START_DATE = datetime.now() - timedelta(minutes=1)
SCHEDULE_INTERVAL = None            # How often to Run. @daily - Once a day at Midnight
DAG_OWNER_NAME = "operations"           # Who is listed as the owner of this DAG in the Airflow Web Server
ALERT_EMAIL_ADDRESSES = []              # List of email address to send email alerts to if this job fails
DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = 30   # Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or older.
ENABLE_DELETE = True                    # Whether the job should delete the db entries or not. Included if you want to temporarily avoid deleting the db entries.

default_args = {
    'owner': DAG_OWNER_NAME,
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(DAG_ID, default_args=default_args, schedule_interval=SCHEDULE_INTERVAL, start_date=START_DATE)


def db_cleanup_function(**context):
    logging.info("Getting Configurations...")
    dag_run_conf = context.get("dag_run").conf
    logging.info("dag_run.conf: " + str(dag_run_conf))
    if dag_run_conf:
        max_db_entry_age_in_days = dag_run_conf.get("maxDBEntryAgeInDays", None)
    else:
        max_db_entry_age_in_days = None
    logging.info("maxDBEntryAgeInDays from dag_run.conf: " + str(dag_run_conf))
    if max_db_entry_age_in_days is None:
        logging.info("maxDBEntryAgeInDays conf variable isn't included. Using Default '" + str(DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS) + "'")
        max_db_entry_age_in_days = DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS
    max_execution_date = datetime.now() + timedelta(-max_db_entry_age_in_days)
    session = settings.Session()
    logging.info("Finished Getting Configurations\n")

    logging.info("Configurations:")
    logging.info("max_db_entry_age_in_days: " + str(max_db_entry_age_in_days))
    logging.info("max_execution_date:       " + str(max_execution_date))
    logging.info("enable_delete:            " + str(ENABLE_DELETE))
    logging.info("session:                  " + str(session))
    logging.info("")

    logging.info("Running Cleanup Process...")

    dag_runs_to_delete = session.query(DagRun).filter(
        DagRun.execution_date <= max_execution_date,
    ).all()
    logging.info("Process will be Deleting the following DagRun(s):")
    for dag_run in dag_runs_to_delete:
        logging.info("\t" + str(dag_run))
    logging.info("Process will be Deleting " + str(len(dag_runs_to_delete)) + " DagRun(s)")

    task_instances_to_delete = session.query(TaskInstance).filter(
        TaskInstance.execution_date <= max_execution_date,
    ).all()
    logging.info("Process will be Deleting the following TaskInstance(s):")
    for task_instance in task_instances_to_delete:
        logging.info("\t" + str(task_instance))
    logging.info("Process will be Deleting " + str(len(task_instances_to_delete)) + " TaskInstance(s)")

    logs_to_delete = session.query(Log).filter(
        Log.dttm <= max_execution_date,
        ).all()
    logging.info("Process will be Deleting the following Log(s):")
    for log in logs_to_delete:
        logging.info("\t" + str(log))
    logging.info("Process will be Deleting " + str(len(logs_to_delete)) + " Log(s)")

    xcom_entries_to_delete = session.query(XCom).filter(
        XCom.execution_date <= max_execution_date,
        ).all()
    logging.info("Process will be Deleting the following XCom Entries(s):")
    for xcom_entry in xcom_entries_to_delete:
        logging.info("\t" + str(xcom_entry))
    logging.info("Process will be Deleting " + str(len(xcom_entries_to_delete)) + " XCom Entries(s)")

    if ENABLE_DELETE:
        logging.info("Performing Delete...")
        for dag_run in dag_runs_to_delete:
            session.delete(dag_run)
        for task_instance in task_instances_to_delete:
            session.delete(task_instance)
        for log in logs_to_delete:
            session.delete(log)
        for xcom_entry in xcom_entries_to_delete:
            session.delete(xcom_entry)
        logging.info("Finished Performing Delete")
    else:
        logging.warn("You're opted to skip deleting the db entries!!!")

    logging.info("Finished Running Cleanup Process")

db_cleanup = PythonOperator(
    task_id='db_cleanup',
    python_callable=db_cleanup_function,
    provide_context=True,
    dag=dag)
