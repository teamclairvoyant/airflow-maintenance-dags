"""
A maintenance workflow that you can deploy into Airflow to periodically clean out the DagRun, TaskInstance, Log, XCom, Job DB and SlaMiss entries to avoid having too much data in your Airflow MetaStore.

airflow trigger_dag --conf '{"maxDBEntryAgeInDays":30}' airflow-db-cleanup

--conf options:
    maxDBEntryAgeInDays:<INT> - Optional

"""
from airflow.models import DAG, DagRun, TaskInstance, Log, XCom, SlaMiss, DagModel, Variable
from airflow.jobs import BaseJob
from airflow import settings
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from sqlalchemy import func, and_
from sqlalchemy.orm import load_only
import os
import logging
import dateutil.parser
import airflow

try:
    from airflow.utils import timezone  # airflow.utils.timezone is available from v1.10 onwards
    now = timezone.utcnow
except ImportError:
    now = datetime.utcnow

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")  # airflow-db-cleanup
START_DATE = airflow.utils.dates.days_ago(1)
SCHEDULE_INTERVAL = "@daily"            # How often to Run. @daily - Once a day at Midnight (UTC)
DAG_OWNER_NAME = "operations"           # Who is listed as the owner of this DAG in the Airflow Web Server
ALERT_EMAIL_ADDRESSES = []              # List of email address to send email alerts to if this job fails
DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = int(Variable.get("airflow_db_cleanup__max_db_entry_age_in_days", 30)) # Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or older.
ENABLE_DELETE = True                    # Whether the job should delete the db entries or not. Included if you want to temporarily avoid deleting the db entries.
DATABASE_OBJECTS = [                    # List of all the objects that will be deleted. Comment out the DB objects you want to skip.
    {"airflow_db_model": DagRun, "age_check_column": DagRun.execution_date, "keep_last": True, "keep_last_filters": [DagRun.external_trigger==False], "keep_last_group_by": DagRun.dag_id},
    {"airflow_db_model": TaskInstance, "age_check_column": TaskInstance.execution_date, "keep_last": False, "keep_last_filters": None, "keep_last_group_by": None},
    {"airflow_db_model": Log, "age_check_column": Log.dttm, "keep_last": False, "keep_last_filters": None, "keep_last_group_by": None},
    {"airflow_db_model": XCom, "age_check_column": XCom.execution_date, "keep_last": False, "keep_last_filters": None, "keep_last_group_by": None},
    {"airflow_db_model": BaseJob, "age_check_column": BaseJob.latest_heartbeat, "keep_last": False, "keep_last_filters": None, "keep_last_group_by": None},
    {"airflow_db_model": SlaMiss, "age_check_column": SlaMiss.execution_date, "keep_last": False, "keep_last_filters": None, "keep_last_group_by": None},
    {"airflow_db_model": DagModel, "age_check_column": DagModel.last_scheduler_run, "keep_last": False, "keep_last_filters": None, "keep_last_group_by": None},
]

session = settings.Session()

default_args = {
    'owner': DAG_OWNER_NAME,
    'depends_on_past': False,
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(DAG_ID, default_args=default_args, schedule_interval=SCHEDULE_INTERVAL, start_date=START_DATE)
if hasattr(dag, 'doc_md'):
    dag.doc_md = __doc__
if hasattr(dag, 'catchup'):
    dag.catchup = False


def print_configuration_function(**context):
    logging.info("Loading Configurations...")
    dag_run_conf = context.get("dag_run").conf
    logging.info("dag_run.conf: " + str(dag_run_conf))
    max_db_entry_age_in_days = None
    if dag_run_conf:
        max_db_entry_age_in_days = dag_run_conf.get("maxDBEntryAgeInDays", None)
    logging.info("maxDBEntryAgeInDays from dag_run.conf: " + str(dag_run_conf))
    if (max_db_entry_age_in_days is None or max_db_entry_age_in_days < 1):
        logging.info("maxDBEntryAgeInDays conf variable isn't included or Variable value is less than 1. Using Default '" + str(DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS) + "'")
        max_db_entry_age_in_days = DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS
    max_date = now() + timedelta(-max_db_entry_age_in_days)
    logging.info("Finished Loading Configurations")
    logging.info("")

    logging.info("Configurations:")
    logging.info("max_db_entry_age_in_days: " + str(max_db_entry_age_in_days))
    logging.info("max_date:                 " + str(max_date))
    logging.info("enable_delete:            " + str(ENABLE_DELETE))
    logging.info("session:                  " + str(session))
    logging.info("")

    logging.info("Setting max_execution_date to XCom for Downstream Processes")
    context["ti"].xcom_push(key="max_date", value=max_date.isoformat())

print_configuration = PythonOperator(
    task_id='print_configuration',
    python_callable=print_configuration_function,
    provide_context=True,
    dag=dag)


def cleanup_function(**context):

    logging.info("Retrieving max_execution_date from XCom")
    max_date = context["ti"].xcom_pull(task_ids=print_configuration.task_id, key="max_date")
    max_date = dateutil.parser.parse(max_date) # stored as iso8601 str in xcom

    airflow_db_model = context["params"].get("airflow_db_model")
    state = context["params"].get("state")
    age_check_column = context["params"].get("age_check_column")
    keep_last = context["params"].get("keep_last")
    keep_last_filters = context["params"].get("keep_last_filters")
    keep_last_group_by = context["params"].get("keep_last_group_by")

    logging.info("Configurations:")
    logging.info("max_date:                 " + str(max_date))
    logging.info("enable_delete:            " + str(ENABLE_DELETE))
    logging.info("session:                  " + str(session))
    logging.info("airflow_db_model:         " + str(airflow_db_model))
    logging.info("state:                    " + str(state))
    logging.info("age_check_column:         " + str(age_check_column))
    logging.info("keep_last:                " + str(keep_last))
    logging.info("keep_last_filters:        " + str(keep_last_filters))
    logging.info("keep_last_group_by:          " + str(keep_last_group_by))

    logging.info("")

    logging.info("Running Cleanup Process...")

    query = session.query(airflow_db_model).options(load_only(age_check_column))
    
    logging.info("INITIAL QUERY : " +  str(query))

    if keep_last:

        subquery = session.query(func.max(DagRun.execution_date))
        # workaround for MySQL "table specified twice" issue
        # https://github.com/teamclairvoyant/airflow-maintenance-dags/issues/41
        if keep_last_filters is not None:
            for entry in keep_last_filters:
                subquery = subquery.filter(entry)
                
            logging.info("SUB QUERY [keep_last_filters]: " +  str(subquery))

        if  keep_last_group_by is not None:
            subquery = subquery.group_by(keep_last_group_by)
            logging.info("SUB QUERY [keep_last_group_by]: " +  str(subquery))
            

        subquery = subquery.from_self()

        query = query.filter(
            and_(age_check_column.notin_(subquery)),
            and_(age_check_column <= max_date)
        )
        
    else:
        query = query.filter(age_check_column <= max_date,)

    entries_to_delete = query.all()

    logging.info("Query : " +  str(query))
    logging.info("Process will be Deleting the following " + str(airflow_db_model.__name__) + "(s):")
    for entry in entries_to_delete:
        logging.info("\tEntry: " + str(entry) + ", Date: " + str(entry.__dict__[str(age_check_column).split(".")[1]]))

    logging.info("Process will be Deleting " + str(len(entries_to_delete)) + " " + str(airflow_db_model.__name__) + "(s)")

    if ENABLE_DELETE:
        logging.info("Performing Delete...")
        #using bulk delete
        query.delete(synchronize_session=False)
        session.commit()
        logging.info("Finished Performing Delete")
    else:
        logging.warn("You're opted to skip deleting the db entries!!!")

    logging.info("Finished Running Cleanup Process")

for db_object in DATABASE_OBJECTS:

    cleanup_op = PythonOperator(
        task_id='cleanup_' + str(db_object["airflow_db_model"].__name__),
        python_callable=cleanup_function,
        params=db_object,
        provide_context=True,
        dag=dag
    )

    print_configuration.set_downstream(cleanup_op)
