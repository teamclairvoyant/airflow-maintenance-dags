"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the DagRun, TaskInstance, Log, XCom, Job DB and SlaMiss entries to avoid
having too much data in your Airflow MetaStore.

airflow trigger_dag --conf '[curly-braces]"maxDBEntryAgeInDays":30[curly-braces]' airflow-db-cleanup

--conf options:
    maxDBEntryAgeInDays:<INT> - Optional

"""
import airflow
from airflow import settings
from airflow.configuration import conf
from airflow.models import DAG, DagTag, DagModel, DagRun, Log, XCom, SlaMiss, TaskInstance, Variable
try:
    from airflow.jobs import BaseJob
except Exception as e:
    from airflow.jobs.base_job import BaseJob
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import dateutil.parser
import logging
import os
from sqlalchemy import func, and_
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import load_only

try:
    # airflow.utils.timezone is available from v1.10 onwards
    from airflow.utils import timezone
    now = timezone.utcnow
except ImportError:
    now = datetime.utcnow

# airflow-db-cleanup
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
DOC_MD = f"""
### [README.md](https://github.com/teamclairvoyant/airflow-maintenance-dags/tree/master/{DAG_ID})
"""

START_DATE = airflow.utils.dates.days_ago(1)
# How often to Run. @daily - Once a day at Midnight (UTC)
SCHEDULE_INTERVAL = "@daily"
# Who is listed as the owner of this DAG in the Airflow Web Server
DAG_OWNER_NAME = "operations"
# List of email address to send email alerts to if this job fails
ALERT_EMAIL_ADDRESSES = []
# Length to retain the log files if not already provided in the conf. If this
# is set to 30, the job will remove those files that arE 30 days old or older.

DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS = int(
    Variable.get("airflow_db_cleanup__max_db_entry_age_in_days", 30)
)
# Prints the database entries which will be getting deleted; set to False to avoid printing large lists and slowdown process
PRINT_DELETES = True
# Whether the job should delete the db entries or not. Included if you want to
# temporarily avoid deleting the db entries.
ENABLE_DELETE = True

# get dag model last schedule run
try:
    dag_model_last_scheduler_run = DagModel.last_scheduler_run
except AttributeError:
    dag_model_last_scheduler_run = DagModel.last_parsed_time

# List of all the objects that will be deleted. Comment out the DB objects you
# want to skip.
DATABASE_OBJECTS = [
    {
        "airflow_db_model": BaseJob,
        "age_check_column": BaseJob.latest_heartbeat,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": DagRun,
        "age_check_column": DagRun.execution_date,
        "keep_last": True,
        "keep_last_filters": [DagRun.external_trigger.is_(False)],
        "keep_last_group_by": DagRun.dag_id
    },
    {
        "airflow_db_model": TaskInstance,
        "age_check_column": TaskInstance.execution_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": Log,
        "age_check_column": Log.dttm,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": XCom,
        "age_check_column": XCom.execution_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": SlaMiss,
        "age_check_column": SlaMiss.execution_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    },
    {
        "airflow_db_model": DagModel,
        "age_check_column": dag_model_last_scheduler_run,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    }]

# Check for TaskReschedule model
try:
    from airflow.models import TaskReschedule
    DATABASE_OBJECTS.append({
        "airflow_db_model": TaskReschedule,
        "age_check_column": TaskReschedule.execution_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    })

except Exception as e:
    logging.error(e)

# Check for TaskFail model
try:
    from airflow.models import TaskFail
    DATABASE_OBJECTS.append({
        "airflow_db_model": TaskFail,
        "age_check_column": TaskFail.execution_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    })

except Exception as e:
    logging.error(e)

# Check for RenderedTaskInstanceFields model
try:
    from airflow.models import RenderedTaskInstanceFields
    DATABASE_OBJECTS.append({
        "airflow_db_model": RenderedTaskInstanceFields,
        "age_check_column": RenderedTaskInstanceFields.execution_date,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    })

except Exception as e:
    logging.error(e)

# Check for ImportError model
try:
    from airflow.models import ImportError
    DATABASE_OBJECTS.append({
        "airflow_db_model": ImportError,
        "age_check_column": ImportError.timestamp,
        "keep_last": False,
        "keep_last_filters": None,
        "keep_last_group_by": None
    })

except Exception as e:
    logging.error(e)

# Check for celery executor
airflow_executor = str(conf.get("core", "executor"))
logging.info("Airflow Executor: " + str(airflow_executor))
if(airflow_executor == "CeleryExecutor"):
    logging.info("Including Celery Modules")
    try:
        from celery.backends.database.models import Task, TaskSet
        DATABASE_OBJECTS.extend((
            {
                "airflow_db_model": Task,
                "age_check_column": Task.date_done,
                "keep_last": False,
                "keep_last_filters": None,
                "keep_last_group_by": None
            },
            {
                "airflow_db_model": TaskSet,
                "age_check_column": TaskSet.date_done,
                "keep_last": False,
                "keep_last_filters": None,
                "keep_last_group_by": None
            }))

    except Exception as e:
        logging.error(e)

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

dag = DAG(
    DAG_ID,
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    tags=['teamclairvoyant', 'airflow-maintenance-dags']
)
if hasattr(dag, 'doc_md'):
    dag.doc_md = DOC_MD
if hasattr(dag, 'catchup'):
    dag.catchup = False


def print_configuration_function(**context):
    logging.info("Loading Configurations...")
    dag_run_conf = context.get("dag_run").conf
    logging.info("dag_run.conf: " + str(dag_run_conf))
    max_db_entry_age_in_days = None
    if dag_run_conf:
        max_db_entry_age_in_days = dag_run_conf.get(
            "maxDBEntryAgeInDays", None
        )
    logging.info("maxDBEntryAgeInDays from dag_run.conf: " + str(dag_run_conf))
    if (max_db_entry_age_in_days is None or max_db_entry_age_in_days < 1):
        logging.info(
            "maxDBEntryAgeInDays conf variable isn't included or Variable " +
            "value is less than 1. Using Default '" +
            str(DEFAULT_MAX_DB_ENTRY_AGE_IN_DAYS) + "'"
        )
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
    max_date = context["ti"].xcom_pull(
        task_ids=print_configuration.task_id, key="max_date"
    )
    max_date = dateutil.parser.parse(max_date)  # stored as iso8601 str in xcom

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
    logging.info("keep_last_group_by:       " + str(keep_last_group_by))

    logging.info("")

    logging.info("Running Cleanup Process...")

    try:
        query = session.query(airflow_db_model).options(
            load_only(age_check_column)
        )

        logging.info("INITIAL QUERY : " + str(query))

        if keep_last:

            subquery = session.query(func.max(DagRun.execution_date))
            # workaround for MySQL "table specified twice" issue
            # https://github.com/teamclairvoyant/airflow-maintenance-dags/issues/41
            if keep_last_filters is not None:
                for entry in keep_last_filters:
                    subquery = subquery.filter(entry)

                logging.info("SUB QUERY [keep_last_filters]: " + str(subquery))

            if keep_last_group_by is not None:
                subquery = subquery.group_by(keep_last_group_by)
                logging.info(
                    "SUB QUERY [keep_last_group_by]: " + str(subquery))

            subquery = subquery.from_self()

            query = query.filter(
                and_(age_check_column.notin_(subquery)),
                and_(age_check_column <= max_date)
            )

        else:
            query = query.filter(age_check_column <= max_date,)

        if PRINT_DELETES:
            entries_to_delete = query.all()

            logging.info("Query: " + str(query))
            logging.info(
                "Process will be Deleting the following " +
                str(airflow_db_model.__name__) + "(s):"
            )
            for entry in entries_to_delete:
                logging.info(
                    "\tEntry: " + str(entry) + ", Date: " +
                    str(entry.__dict__[str(age_check_column).split(".")[1]])
                )

            logging.info(
                "Process will be Deleting " + str(len(entries_to_delete)) + " " +
                str(airflow_db_model.__name__) + "(s)"
            )
        else:
            logging.warn(
                "You've opted to skip printing the db entries to be deleted. Set PRINT_DELETES to True to show entries!!!")

        if ENABLE_DELETE:
            logging.info('Performing Delete...')
            if airflow_db_model.__name__ == 'DagModel':
                logging.info('Deleting tags...')
                ids_query = query.from_self().with_entities(DagModel.dag_id)
                tags_query = session.query(DagTag).filter(DagTag.dag_id.in_(ids_query))
                logging.info('Tags delete Query: ' + str(tags_query))
                tags_query.delete(synchronize_session=False)
            # using bulk delete
            query.delete(synchronize_session=False)
            session.commit()
            logging.info('Finished Performing Delete')
        else:
            logging.warn(
                "You've opted to skip deleting the db entries. Set ENABLE_DELETE to True to delete entries!!!")

        logging.info("Finished Running Cleanup Process")

    except ProgrammingError as e:
        logging.error(e)
        logging.error(str(airflow_db_model) +
                      " is not present in the metadata. Skipping...")


for db_object in DATABASE_OBJECTS:

    cleanup_op = PythonOperator(
        task_id='cleanup_' + str(db_object["airflow_db_model"].__name__),
        python_callable=cleanup_function,
        params=db_object,
        provide_context=True,
        dag=dag
    )

    print_configuration.set_downstream(cleanup_op)
