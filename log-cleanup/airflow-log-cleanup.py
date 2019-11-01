"""
A maintenance workflow that you can deploy into Airflow to periodically clean out the task logs to avoid those getting too big.
airflow trigger_dag --conf '{"maxLogAgeInDays":30}' airflow-log-cleanup
--conf options:
    maxLogAgeInDays:<INT> - Optional
"""
from airflow.models import DAG, Variable
from airflow.configuration import conf
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
import os
import logging
import airflow


DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")  # airflow-log-cleanup
START_DATE = airflow.utils.dates.days_ago(1)
BASE_LOG_FOLDER = conf.get("core", "BASE_LOG_FOLDER")
SCHEDULE_INTERVAL = "@daily"        # How often to Run. @daily - Once a day at Midnight
DAG_OWNER_NAME = "operations"       # Who is listed as the owner of this DAG in the Airflow Web Server
ALERT_EMAIL_ADDRESSES = []          # List of email address to send email alerts to if this job fails
DEFAULT_MAX_LOG_AGE_IN_DAYS = Variable.get("airflow_log_cleanup__max_log_age_in_days", 30)  # Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or older
ENABLE_DELETE = True                # Whether the job should delete the logs or not. Included if you want to temporarily avoid deleting the logs
NUMBER_OF_WORKERS = 1               # The number of worker nodes you have in Airflow. Will attempt to run this process for however many workers there are so that each worker gets its logs cleared.
DIRECTORIES_TO_DELETE = [BASE_LOG_FOLDER]
ENABLE_DELETE_CHILD_LOG = Variable.get("airflow_log_cleanup__enable_delete_child_log", "False")
logging.info("ENABLE_DELETE_CHILD_LOG  " + ENABLE_DELETE_CHILD_LOG)

if ENABLE_DELETE_CHILD_LOG.lower() == "true":
    try:
        CHILD_PROCESS_LOG_DIRECTORY = conf.get("scheduler", "CHILD_PROCESS_LOG_DIRECTORY")
        if CHILD_PROCESS_LOG_DIRECTORY is not ' ':
            DIRECTORIES_TO_DELETE.append(CHILD_PROCESS_LOG_DIRECTORY)
    except Exception as e:
        logging.exception("Cloud not obtain CHILD_PROCESS_LOG_DIRECTORY from Airflow Configurations: " + str(e))

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


log_cleanup = """
echo "Getting Configurations..."
BASE_LOG_FOLDER="{{params.directory}}"
TYPE="{{params.type}}"
MAX_LOG_AGE_IN_DAYS="{{dag_run.conf.maxLogAgeInDays}}"
if [ "${MAX_LOG_AGE_IN_DAYS}" == "" ]; then
    echo "maxLogAgeInDays conf variable isn't included. Using Default '""" + str(DEFAULT_MAX_LOG_AGE_IN_DAYS) + """'."
    MAX_LOG_AGE_IN_DAYS='""" + str(DEFAULT_MAX_LOG_AGE_IN_DAYS) + """'
fi
ENABLE_DELETE=""" + str("true" if ENABLE_DELETE else "false") + """
echo "Finished Getting Configurations"
echo ""

echo "Configurations:"
echo "BASE_LOG_FOLDER:      '${BASE_LOG_FOLDER}'"
echo "MAX_LOG_AGE_IN_DAYS:  '${MAX_LOG_AGE_IN_DAYS}'"
echo "ENABLE_DELETE:        '${ENABLE_DELETE}'"
echo "TYPE:                 '${TYPE}'"

echo ""
echo "Running Cleanup Process..."
if [ $TYPE == "file" ]; then
    FIND_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type f -mtime +${MAX_LOG_AGE_IN_DAYS}"
    DELETE_STMT="${FIND_STATEMENT} -exec rm -f {} \;"
elif [ $TYPE == "task_directory" ]; then
    FIND_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type d -empty"
    DELETE_STMT="${FIND_STATEMENT} -prune -exec rm -rf {} \;"
elif [ $TYPE == "dag_directory" ]; then
    FIND_STATEMENT="find ${BASE_LOG_FOLDER}/* -type d -empty"
    DELETE_STMT="${FIND_STATEMENT} -prune -exec rm -rf {} \;"
else
    exit 1
fi
echo "Executing Find Statement: ${FIND_STATEMENT}"
FILES_MARKED_FOR_DELETE=`eval ${FIND_STATEMENT}`
echo "Process will be Deleting the following File(s)/Directory(s):"
echo "${FILES_MARKED_FOR_DELETE}"
echo "Process will be Deleting `echo "${FILES_MARKED_FOR_DELETE}" | grep -v '^$' | wc -l` File(s)/Directory(s)"     # "grep -v '^$'" - removes empty lines. "wc -l" - Counts the number of lines
echo ""
if [ "${ENABLE_DELETE}" == "true" ];
then
    if [ "${FILES_MARKED_FOR_DELETE}" != "" ];
    then
        echo "Executing Delete Statement: ${DELETE_STMT}"
        eval ${DELETE_STMT}
        DELETE_STMT_EXIT_CODE=$?
        if [ "${DELETE_STMT_EXIT_CODE}" != "0" ]; then
            echo "Delete process failed with exit code '${DELETE_STMT_EXIT_CODE}'"
            exit ${DELETE_STMT_EXIT_CODE}
        fi
    else
        echo "WARN: No File(s)/Directory(s) to Delete"
    fi
else
    echo "WARN: You're opted to skip deleting the File(s)/Directory(s)!!!"
fi
echo "Finished Running Cleanup Process"
"""
i = 0
for log_cleanup_id in range(1, NUMBER_OF_WORKERS + 1):

    for directory in DIRECTORIES_TO_DELETE:
        log_cleanup_file_op = BashOperator(
            task_id='log_cleanup_file_' + str(i),
            bash_command=log_cleanup,
            params={"directory": str(directory), "type": "file"},
            dag=dag)

        log_cleanup_task_dir_op = BashOperator(
            task_id='log_cleanup_task_directory_' + str(i),
            bash_command=log_cleanup,
            params={"directory": str(directory), "type": "task_directory"},
            dag=dag)

        log_cleanup_dag_dir_op = BashOperator(
            task_id='log_cleanup_dag_directory_' + str(i),
            bash_command=log_cleanup,
            params={"directory": str(directory), "type": "dag_directory"},
            dag=dag)

        i += 1

        log_cleanup_file_op.set_downstream(log_cleanup_task_dir_op)
        log_cleanup_task_dir_op.set_downstream(log_cleanup_dag_dir_op)
