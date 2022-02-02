"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the DagRun, TaskInstance, Log, XCom, Job DB and SlaMiss entries to avoid
having too much data in your Airflow MetaStore.

airflow trigger_dag --conf '[curly-braces]"maxDBEntryAgeInDays":30[curly-braces]' airflow-db-cleanup

--conf options:
    maxDBEntryAgeInDays:<INT> - Optional

"""
import os
import logging
from datetime import timedelta

from airflow import settings
from airflow.configuration import conf
from airflow.jobs.base_job import BaseJob
from airflow.models import (DAG, DagTag, DagRun, DagModel, Log, SlaMiss, TaskInstance, Variable, XCom)
from airflow.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.utils.dates import days_ago
from sqlalchemy import and_, func
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.orm import load_only

from airflow_maintenance.util import get_bool
from utils.monitoring import failure_callback, success_callback

# get dag model last schedule run
try:
    dag_model_last_scheduler_run = DagModel.last_scheduler_run
except AttributeError:
    dag_model_last_scheduler_run = DagModel.last_parsed_time

# List of all the objects that will be deleted.
# Comment out the DB objects you want to skip.
DATABASE_OBJECTS = ['BaseJob',
                    'Log',
                    'XCom',
                    'SlaMiss',
                    'DagModel',
                    'TaskFail',
                    'RenderedTaskInstanceFields',
                    'ImportError',
                    'TaskReschedule',
                    'TaskInstance_end_date',
                    'TaskInstance_start_date',
                    'DagRun',
                    # celery executor specific
                    'Task',
                    'TaskSet'
                    ]


def cleanup_function(**context):
    database_objects_dict = {
        'BaseJob': {
            "airflow_db_model": BaseJob,
            "age_check_column": BaseJob.latest_heartbeat,
            "keep_last": False,
            "keep_last_filters": None,
            "keep_last_group_by": None
        },
        'DagRun': {
            "airflow_db_model": DagRun,
            "age_check_column": DagRun.execution_date,
            "keep_last": True,
            "keep_last_filters": [DagRun.external_trigger.is_(False)],
            "keep_last_group_by": DagRun.dag_id
        },
        'TaskInstance_end_date': {
            "airflow_db_model": TaskInstance,
            "age_check_column": TaskInstance.end_date,
            "keep_last": False,
            "keep_last_filters": None,
            "keep_last_group_by": None
        },
        'TaskInstance_start_date': {
            "airflow_db_model": TaskInstance,
            "age_check_column": TaskInstance.start_date,
            "keep_last": False,
            "keep_last_filters": None,
            "keep_last_group_by": None
        },
        'Log': {
            "airflow_db_model": Log,
            "age_check_column": Log.dttm,
            "keep_last": False,
            "keep_last_filters": None,
            "keep_last_group_by": None
        },
        'XCom': {
            "airflow_db_model": XCom,
            "age_check_column": XCom.execution_date,
            "keep_last": False,
            "keep_last_filters": None,
            "keep_last_group_by": None
        },
        'SlaMiss': {
            "airflow_db_model": SlaMiss,
            "age_check_column": SlaMiss.execution_date,
            "keep_last": False,
            "keep_last_filters": None,
            "keep_last_group_by": None
        },
        'DagModel': {
            "airflow_db_model": DagModel,
            "age_check_column": dag_model_last_scheduler_run,
            "keep_last": False,
            "keep_last_filters": None,
            "keep_last_group_by": None
        }
    }

    # Check for TaskReschedule model
    try:
        from airflow.models import TaskReschedule
        database_objects_dict['TaskReschedule'] = {
            "airflow_db_model": TaskReschedule,
            "age_check_column": TaskReschedule.reschedule_date,
            "keep_last": False,
            "keep_last_filters": None,
            "keep_last_group_by": None
        }

    except Exception as e:
        logging.error(e)

    # Check for TaskFail model
    try:
        from airflow.models import TaskFail
        database_objects_dict['TaskFail'] = {
            "airflow_db_model": TaskFail,
            "age_check_column": TaskFail.execution_date,
            "keep_last": False,
            "keep_last_filters": None,
            "keep_last_group_by": None
        }

    except Exception as e:
        logging.error(e)

    # Check for RenderedTaskInstanceFields model
    try:
        from airflow.models import RenderedTaskInstanceFields
        database_objects_dict['RenderedTaskInstanceFields'] = {
            "airflow_db_model": RenderedTaskInstanceFields,
            "age_check_column": RenderedTaskInstanceFields.execution_date,
            "keep_last": False,
            "keep_last_filters": None,
            "keep_last_group_by": None
        }

    except Exception as e:
        logging.error(e)

    # Check for ImportError model
    try:
        from airflow.models import ImportError
        database_objects_dict['ImportError'] = {
            "airflow_db_model": ImportError,
            "age_check_column": ImportError.timestamp,
            "keep_last": False,
            "keep_last_filters": None,
            "keep_last_group_by": None
        }

    except Exception as e:
        logging.error(e)

    # Check for celery executor
    airflow_executor = str(conf.get("core", "executor"))
    logging.info("Airflow Executor: " + str(airflow_executor))
    if airflow_executor == "CeleryExecutor":
        logging.info("Including Celery Modules")
        try:
            from celery.backends.database.models import Task, TaskSet
            database_objects_dict['Task'] = {
                "airflow_db_model": Task,
                "age_check_column": Task.date_done,
                "keep_last": False,
                "keep_last_filters": None,
                "keep_last_group_by": None
            }
            database_objects_dict['TaskSet'] = {
                "airflow_db_model": TaskSet,
                "age_check_column": TaskSet.date_done,
                "keep_last": False,
                "keep_last_filters": None,
                "keep_last_group_by": None
            }

        except Exception as e:
            logging.error(e)

    object_name = str(context["params"].get("object_name"))

    airflow_db_model = database_objects_dict[object_name].get("airflow_db_model")
    age_check_column = database_objects_dict[object_name].get("age_check_column")
    keep_last = database_objects_dict[object_name].get("keep_last")
    keep_last_filters = database_objects_dict[object_name].get("keep_last_filters")
    keep_last_group_by = database_objects_dict[object_name].get("keep_last_group_by")

    session = settings.Session()

    enable_delete = get_bool("airflow_db_cleanup__enable")
    print_deletes = get_bool("airflow_db_cleanup__print_deletes")
    max_db_entry_age_in_days = int(
        Variable.get("airflow_db_cleanup__max_db_entry_age_in_days", 90)
    )

    max_date = timezone.utcnow() + timedelta(-max_db_entry_age_in_days)

    logging.info("Configurations:")
    logging.info("max_date:                 " + str(max_date))
    logging.info("enable_delete:            " + str(enable_delete))
    logging.info("airflow_db_model:         " + str(airflow_db_model))
    logging.info("age_check_column:         " + str(age_check_column))
    logging.info("keep_last:                " + str(keep_last))
    logging.info("keep_last_filters:        " + str(keep_last_filters))
    logging.info("keep_last_group_by:       " + str(keep_last_group_by))

    logging.info("")

    logging.info("Running Cleanup Process...")

    try:
        query = session.query(airflow_db_model).options(load_only(age_check_column))

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
                logging.info("SUB QUERY [keep_last_group_by]: " + str(subquery))

            subquery = subquery.from_self()

            query = query.filter(
                and_(age_check_column.notin_(subquery)),
                and_(age_check_column <= max_date),
            )

        else:
            query = query.filter(age_check_column <= max_date,)

        logging.info("Query to retrieve data for deletion : " + str(query))

        entries_to_delete = query.all()

        logging.info(
            "Process will be Deleting "
            + str(len(entries_to_delete))
            + " "
            + str(airflow_db_model.__name__)
            + "(s)"
        )

        if print_deletes:
            logging.info("Query: " + str(query))
            logging.info(
                "Process will be Deleting the following "
                + str(airflow_db_model.__name__)
                + "(s):"
            )
            for entry in entries_to_delete:
                logging.info(
                    "\tEntry: "
                    + str(entry)
                    + ", Date: "
                    + str(entry.__dict__[str(age_check_column).split(".")[1]])
                )
        else:
            logging.warning(
                "You've opted to skip printing the db entries to be deleted. Set PRINT_DELETES to True to show entries!!!"
            )

        if enable_delete:
            logging.info("Performing Delete...")
            if airflow_db_model.__name__ == 'DagModel':
                 logging.info('Deleting tags...')
                 ids_query = query.from_self().with_entities(DagModel.dag_id)
                 tags_query = session.query(DagTag).filter(DagTag.dag_id.in_(ids_query))
                 logging.info('Tags delete Query: ' + str(tags_query))
                 tags_query.delete(synchronize_session=False)
            # using bulk delete
            query.delete(synchronize_session=False)
            session.commit()
            logging.info("Finished Performing Delete")
        else:
            logging.warning(
                "You've opted to skip deleting the db entries. Set ENABLE_DELETE to True to delete entries!!!"
            )

        logging.info("Finished Running Cleanup Process")

    except ProgrammingError as e:
        logging.error(e)
        logging.error(
            str(airflow_db_model) + " is not present in the metadata. Skipping..."
        )


default_args = {
    "owner": "airflow_maintenance",
    "depends_on_past": False,
    "email": ["d9k4z1c8u2u8g2f1@propellerads.slack.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 0,
    # db_cleanup process should not block the airflow db for a long time
    "sla": timedelta(minutes=3),
    "execution_timeout": timedelta(minutes=5),
    "on_success_callback": success_callback,
    "on_failure_callback": failure_callback,
}

with open(os.path.join(os.path.dirname(__file__), 'README_db_cleanup.md'), 'r') as f:
    doc = f.read()

with DAG(
    dag_id="airflow_db_cleanup",
    default_args=default_args,
    start_date=days_ago(1, hour=1, minute=2),
    schedule_interval='2 1-23/2 * * *',
    max_active_runs=1,
    concurrency=1,
    doc_md=doc,
    tags=['maintenance'],
) as dag:
    if hasattr(dag, "catchup"):
        dag.catchup = False

    prev_task = None

    for db_object in DATABASE_OBJECTS:

        cleanup_op = PythonOperator(
            task_id='cleanup_' + str(db_object),
            python_callable=cleanup_function,
            params={'object_name': db_object},
        )

        if prev_task is not None:
            prev_task >> cleanup_op

        prev_task = cleanup_op
