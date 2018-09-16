import logging
import os
from datetime import datetime, timedelta

from airflow.jobs import BaseJob
from airflow.models import DAG, DagRun, DagModel, Variable, TaskInstance, XCom, Log, SlaMiss
from airflow.models import settings
from airflow.operators.python_operator import PythonOperator
from sqlalchemy import not_

"""
A maintenance workflow that you can deploy into Airflow to periodically clean out the DagRun, TaskInstance, 
Log, XCom, Job DB and SlaMiss entries to avoid having too much data in your Airflow MetaStore.

airflow trigger_dag --conf '{"maxDBEntryAgeInDays":30}' airflow-db-cleanup

--conf options:
    maxDBEntryAgeInDays:<INT> - Optional

"""

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")  # airflow-db-cleanup
START_DATE = datetime.now() - timedelta(minutes=1)
SCHEDULE_INTERVAL = "@daily"  # How often to Run. @daily - Once a day at Midnight (UTC)
DAG_OWNER_NAME = "operations"  # Who is listed as the owner of this DAG in the Airflow Web Server
ALERT_EMAIL_ADDRESSES = []  # List of email address to send email alerts to if this job fails
DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = Variable.get("max_db_entry_age_in_days",
                                                30)  # Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or older.
ENABLE_DELETE = False  # Whether the job should delete the db entries or not. Included if you want to temporarily avoid deleting the db entries.


# need some special loves to DagRun objects
def _find_dag_run_entries_to_delete():
    def _query(max_date):
        inactive_runs_to_delete = session.query(DagRun) \
            .join(DagModel, DagModel.dag_id == DagRun.dag_id) \
            .filter(not_(DagModel.is_active)) \
            .all()
        for run in inactive_runs_to_delete:
            logging.info(
                "Inactive runs to delete: " + str(run))

        manual_runs_to_delete = session.query(DagRun) \
            .join(DagModel, DagModel.dag_id == DagRun.dag_id) \
            .filter(DagRun.execution_date <= max_date) \
            .filter(DagRun.external_trigger) \
            .all()
        for run in manual_runs_to_delete:
            logging.info(
                "Manual runs to delete: " + str(run))

        scheduled_active_runs_to_delete = session.query(DagRun) \
            .join(DagModel, DagModel.dag_id == DagRun.dag_id) \
            .filter(DagRun.execution_date <= max_date) \
            .filter(not_(DagRun.external_trigger), DagModel.is_active) \
            .order_by(DagRun.execution_date) \
            .all()
        for run in scheduled_active_runs_to_delete:
            logging.info(
                "Scheduled runs to delete: " + str(run))

        scheduled_active_dags_to_keep = session.query(DagModel) \
            .filter(DagModel.is_active) \
            .filter(not_(session.query(DagRun)
                         .filter(DagRun.dag_id == DagModel.dag_id)
                         .filter(DagRun.execution_date > max_date)
                         .filter(not_(DagRun.external_trigger))
                         .exists())) \
            .all()
        for scheduled_active_dag in scheduled_active_dags_to_keep:
            logging.info(
                "Scheduled DAGs to keep: " + str(scheduled_active_dag))
            for scheduled_active_run_to_delete in scheduled_active_runs_to_delete:
                if scheduled_active_run_to_delete.dag_id == scheduled_active_dag.dag_id:
                    logging.info(
                        "Scheduled DAG run to keep: " + str(scheduled_active_run_to_delete))
                    scheduled_active_runs_to_delete.remove(scheduled_active_run_to_delete)
                    break

        return inactive_runs_to_delete + manual_runs_to_delete + scheduled_active_runs_to_delete

    return _query


def _build_db_entries_to_delete_query_func(airflow_db_model, age_check_column):
    def _query(max_date):
        return session.query(airflow_db_model) \
            .filter(age_check_column <= max_date) \
            .all()

    return _query


DATABASE_OBJECTS = [
    # List of all the objects that will be deleted. Comment out the DB objects you want to skip.
    {
        "airflow_db_model": DagRun,
        "find_entries_func": _find_dag_run_entries_to_delete()
    },
    {
        "airflow_db_model": TaskInstance,
        "find_entries_func": _build_db_entries_to_delete_query_func(TaskInstance, TaskInstance.execution_date)
    },
    {
        "airflow_db_model": Log,
        "find_entries_func": _build_db_entries_to_delete_query_func(Log, Log.dttm)
    },
    {
        "airflow_db_model": XCom,
        "find_entries_func": _build_db_entries_to_delete_query_func(XCom, XCom.execution_date)
    },
    {
        "airflow_db_model": BaseJob,
        "find_entries_func": _build_db_entries_to_delete_query_func(BaseJob, BaseJob.latest_heartbeat)
    },
    {
        "airflow_db_model": SlaMiss,
        "find_entries_func": _build_db_entries_to_delete_query_func(SlaMiss, SlaMiss.execution_date)
    },
    {
        "airflow_db_model": DagModel,
        "find_entries_func": _build_db_entries_to_delete_query_func(DagModel, DagModel.last_scheduler_run)
    },
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
        logging.info("maxDBEntryAgeInDays conf variable isn't included. Using Default '" + str(
            DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS) + "'")
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
    find_entries_to_delete_func = context["params"].get("find_entries_func")

    logging.info("Configurations:")
    logging.info("max_date:                 " + str(max_date))
    logging.info("enable_delete:            " + str(ENABLE_DELETE))
    logging.info("session:                  " + str(session))
    logging.info("airflow_db_model:         " + str(airflow_db_model))
    logging.info("")

    logging.info("Running Cleanup Process...")

    entries_to_delete = find_entries_to_delete_func(max_date)
    logging.info("Process will be Deleting the following " + str(airflow_db_model.__name__) + "(s):")
    logging.info(
        "Process will be Deleting " + str(len(entries_to_delete)) + " " + str(airflow_db_model.__name__) + "(s)")
    for entry_to_delete in entries_to_delete:
        logging.info(
            "Process will be Deleting " + str(entry_to_delete))

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
