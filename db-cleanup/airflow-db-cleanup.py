from airflow.models import DAG, DagRun, TaskInstance, Log, XCom, SlaMiss, DagModel, Variable
from airflow.jobs import BaseJob
from airflow.models import settings
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import logging

"""
A maintenance workflow that you can deploy into Airflow to periodically clean out the DagRun, TaskInstance, Log, XCom, Job DB and SlaMiss entries to avoid having too much data in your Airflow MetaStore.

airflow trigger_dag --conf '{"maxDBEntryAgeInDays":30}' airflow-db-cleanup

--conf options:
    maxDBEntryAgeInDays:<INT> - Optional

"""

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")  # airflow-db-cleanup
START_DATE = datetime.now() - timedelta(minutes=1)
SCHEDULE_INTERVAL = "@daily"            # How often to Run. @daily - Once a day at Midnight (UTC)
DAG_OWNER_NAME = "operations"           # Who is listed as the owner of this DAG in the Airflow Web Server
ALERT_EMAIL_ADDRESSES = []              # List of email address to send email alerts to if this job fails
DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = Variable.get("max_db_entry_age_in_days", 30) # Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or older.
ENABLE_DELETE = True                    # Whether the job should delete the db entries or not. Included if you want to temporarily avoid deleting the db entries.
DATABASE_OBJECTS = [                    # List of all the objects that will be deleted. Comment out the DB objects you want to skip.
    {"airflow_db_model": DagRun, "age_check_column": DagRun.execution_date},
    {"airflow_db_model": TaskInstance, "age_check_column": TaskInstance.execution_date},
    {"airflow_db_model": Log, "age_check_column": Log.dttm},
    {"airflow_db_model": XCom, "age_check_column": XCom.execution_date},
    {"airflow_db_model": BaseJob, "age_check_column": BaseJob.latest_heartbeat},
    {"airflow_db_model": SlaMiss, "age_check_column": SlaMiss.execution_date},
    {"airflow_db_model": DagModel, "age_check_column": DagModel.last_scheduler_run},
]

session = settings.Session()

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


def print_configuration_function(**context):
    logging.info("Loading Configurations...")
    dag_run_conf = context.get("dag_run").conf
    logging.info("dag_run.conf: " + str(dag_run_conf))
    max_db_entry_age_in_days = None
    if dag_run_conf:
        max_db_entry_age_in_days = dag_run_conf.get("maxDBEntryAgeInDays", None)
    logging.info("maxDBEntryAgeInDays from dag_run.conf: " + str(dag_run_conf))
    if max_db_entry_age_in_days is None:
        logging.info("maxDBEntryAgeInDays conf variable isn't included. Using Default '" + str(DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS) + "'")
        max_db_entry_age_in_days = DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS
    max_date = datetime.now() + timedelta(-max_db_entry_age_in_days)
    logging.info("Finished Loading Configurations")
    logging.info("")

    logging.info("Configurations:")
    logging.info("max_db_entry_age_in_days: " + str(max_db_entry_age_in_days))
    logging.info("max_date:                 " + str(max_date))
    logging.info("enable_delete:            " + str(ENABLE_DELETE))
    logging.info("session:                  " + str(session))
    logging.info("")

    logging.info("Setting max_execution_date to XCom for Downstream Processes")
    context["ti"].xcom_push(key="max_date", value=max_date)

print_configuration = PythonOperator(
    task_id='print_configuration',
    python_callable=print_configuration_function,
    provide_context=True,
    dag=dag)


def cleanup_function(**context):

    logging.info("Retrieving max_execution_date from XCom")
    max_date = context["ti"].xcom_pull(task_ids=print_configuration.task_id, key="max_date")

    airflow_db_model = context["params"].get("airflow_db_model")
    age_check_column = context["params"].get("age_check_column")

    logging.info("Configurations:")
    logging.info("max_date:                 " + str(max_date))
    logging.info("enable_delete:            " + str(ENABLE_DELETE))
    logging.info("session:                  " + str(session))
    logging.info("airflow_db_model:         " + str(airflow_db_model))
    logging.info("age_check_column:         " + str(age_check_column))
    logging.info("")

    logging.info("Running Cleanup Process...")

    entries_to_delete = session.query(airflow_db_model).filter(
        age_check_column <= max_date,
        ).all()
    logging.info("Process will be Deleting the following " + str(airflow_db_model.__name__) + "(s):")
    for entry in entries_to_delete:
        logging.info("\tEntry: " + str(entry) + ", Date: " + str(entry.__dict__[str(age_check_column).split(".")[1]]))
    logging.info("Process will be Deleting " + str(len(entries_to_delete)) + " " + str(airflow_db_model.__name__) + "(s)")

    if ENABLE_DELETE:
        logging.info("Performing Delete...")
        for entry in entries_to_delete:
            session.delete(entry)
        logging.info("Finished Performing Delete")
    else:
        logging.warn("You're opted to skip deleting the db entries!!!")

    logging.info("Finished Running Cleanup Process")

for db_object in DATABASE_OBJECTS:

    cleanup = PythonOperator(
        task_id='cleanup_' + str(db_object["airflow_db_model"].__name__),
        python_callable=cleanup_function,
        params=db_object,
        provide_context=True,
        dag=dag
    )

    print_configuration.set_downstream(cleanup)
