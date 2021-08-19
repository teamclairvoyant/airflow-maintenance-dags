"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the task logs to avoid those getting too big.
airflow trigger_dag --conf '[curly-braces]"maxLogAgeInDays":30[curly-braces]' airflow-log-cleanup
--conf options:
    maxLogAgeInDays:<INT> - Optional
"""
import logging
import os
import time
from datetime import timedelta

import airflow
from airflow.configuration import conf
from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

# airflow-log-cleanup
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
START_DATE = airflow.utils.dates.days_ago(1)
try:
    BASE_LOG_FOLDER = conf.get("core", "BASE_LOG_FOLDER").rstrip("/")
except Exception as e:
    BASE_LOG_FOLDER = conf.get("logging", "BASE_LOG_FOLDER").rstrip("/")
# How often to Run. @daily - Once a day at Midnight
SCHEDULE_INTERVAL = "@daily"
# Who is listed as the owner of this DAG in the Airflow Web Server
DAG_OWNER_NAME = "operations"
# List of email address to send email alerts to if this job fails
ALERT_EMAIL_ADDRESSES = []
# Length to retain the log files if not already provided in the conf. If this
# is set to 30, the job will remove those files that are 30 days old or older
DEFAULT_MAX_LOG_AGE_IN_DAYS = Variable.get(
    "airflow_log_cleanup__max_log_age_in_days", 30
)
# Whether the job should delete the logs or not. Included if you want to
# temporarily avoid deleting the logs
ENABLE_DELETE = False

AIRFLOW_HOSTS = "localhost"  # comma separated list of host(s)

TEMP_LOG_CLEANUP_SCRIPT_PATH = "/tmp/airflow_log_cleanup.sh"
DIRECTORIES_TO_DELETE = [BASE_LOG_FOLDER]
ENABLE_DELETE_CHILD_LOG = Variable.get(
    "airflow_log_cleanup__enable_delete_child_log", "False"
)

logging.info("ENABLE_DELETE_CHILD_LOG  " + ENABLE_DELETE_CHILD_LOG)

if not BASE_LOG_FOLDER or BASE_LOG_FOLDER.strip() == "":
    raise ValueError(
        "BASE_LOG_FOLDER variable is empty in airflow.cfg. It can be found "
        "under the [core] (<2.0.0) section or [logging] (>=2.0.0) in the cfg file. "
        "Kindly provide an appropriate directory path."
    )

if ENABLE_DELETE_CHILD_LOG.lower() == "true":
    try:
        CHILD_PROCESS_LOG_DIRECTORY = conf.get(
            "scheduler", "CHILD_PROCESS_LOG_DIRECTORY"
        )
        if CHILD_PROCESS_LOG_DIRECTORY != ' ':
            DIRECTORIES_TO_DELETE.append(CHILD_PROCESS_LOG_DIRECTORY)
    except Exception as e:
        logging.exception(
            "Could not obtain CHILD_PROCESS_LOG_DIRECTORY from " +
            "Airflow Configurations: " + str(e)
        )

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
    dag.doc_md = __doc__
if hasattr(dag, 'catchup'):
    dag.catchup = False

log_cleanup = """
echo "Getting Configurations..."

BASE_LOG_FOLDER=$1
MAX_LOG_AGE_IN_DAYS=$2
ENABLE_DELETE=$3

echo "Finished Getting Configurations"
echo ""

echo "Configurations:"
echo "BASE_LOG_FOLDER:      \'${BASE_LOG_FOLDER}\'"
echo "MAX_LOG_AGE_IN_DAYS:  \'${MAX_LOG_AGE_IN_DAYS}\'"
echo "ENABLE_DELETE:        \'${ENABLE_DELETE}\'"

cleanup() {
    echo "Executing Find Statement: $1"
    FILES_MARKED_FOR_DELETE=$(eval $1)
    echo "Process will be Deleting the following files or directories:"
    echo "${FILES_MARKED_FOR_DELETE}"
    echo "Process will be Deleting $(echo "${FILES_MARKED_FOR_DELETE}" |
        grep -v \'^$\' | wc -l) files or directories"

    echo ""
    if [ "${ENABLE_DELETE}" == "true" ]; then
        if [ "${FILES_MARKED_FOR_DELETE}" != "" ]; then
            echo "Executing Delete Statement: $2"
            eval $2
            DELETE_STMT_EXIT_CODE=$?
            if [ "${DELETE_STMT_EXIT_CODE}" != "0" ]; then
                echo "Delete process failed with exit code  \'${DELETE_STMT_EXIT_CODE}\'"

                exit ${DELETE_STMT_EXIT_CODE}
            fi
        else
            echo "WARN: No files or directories to Delete"
        fi
    else
        echo "WARN: You have opted to skip deleting the files or directories"
    fi
}

echo ""
echo "Running Cleanup Process..."

FIND_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type f -mtime +${MAX_LOG_AGE_IN_DAYS}"
DELETE_STMT="${FIND_STATEMENT} -exec rm -f {} \;"

cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
CLEANUP_EXIT_CODE=$?

FIND_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type d -empty"
DELETE_STMT="${FIND_STATEMENT} -prune -exec rm -rf {} \;"

cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
CLEANUP_EXIT_CODE=$?

FIND_STATEMENT="find ${BASE_LOG_FOLDER}/* -type d -empty"
DELETE_STMT="${FIND_STATEMENT} -prune -exec rm -rf {} \;"

cleanup "${FIND_STATEMENT}" "${DELETE_STMT}"
CLEANUP_EXIT_CODE=$?

echo "Finished Running Cleanup Process"
"""

create_log_cleanup_script = BashOperator(
    task_id=f'create_log_cleanup_script',
    bash_command=f"""
    echo '{log_cleanup}' > {TEMP_LOG_CLEANUP_SCRIPT_PATH}
    chmod +x {TEMP_LOG_CLEANUP_SCRIPT_PATH}
    current_host=$(echo $HOSTNAME)
    echo "Current Host: $current_host"
    hosts_string={AIRFLOW_HOSTS}
    echo "All Scheduler Hosts: $hosts_string"
    IFS=',' read -ra host_array <<< "$hosts_string"
    for host in "${{host_array[@]}}"
    do
        if [ "$host" != "$current_host" ]; then
            echo "Copying log_cleanup script to $host..."
            scp {TEMP_LOG_CLEANUP_SCRIPT_PATH} $host:{TEMP_LOG_CLEANUP_SCRIPT_PATH}
            echo "Making the script executable..."
            ssh $host "chmod +x {TEMP_LOG_CLEANUP_SCRIPT_PATH}"
        fi
    done
    """,
    dag=dag)

for host in AIRFLOW_HOSTS.split(","):
    for DIR_ID, DIRECTORY in enumerate(DIRECTORIES_TO_DELETE):
        LOG_CLEANUP_COMMAND = f'{TEMP_LOG_CLEANUP_SCRIPT_PATH} {DIRECTORY} {DEFAULT_MAX_LOG_AGE_IN_DAYS} {str(ENABLE_DELETE).lower()}'
        cleanup_task = BashOperator(
            task_id=f'airflow_log_cleanup_{host}_dir_{DIR_ID}',
            bash_command=f"""
            echo "Executing cleanup script..."
            ssh {host} "{LOG_CLEANUP_COMMAND}"
            echo "Removing cleanup script..."
            ssh {host} "rm {TEMP_LOG_CLEANUP_SCRIPT_PATH}"
            """,
            dag=dag)

        cleanup_task.set_upstream(create_log_cleanup_script)
