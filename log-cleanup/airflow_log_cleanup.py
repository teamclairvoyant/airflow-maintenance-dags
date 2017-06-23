"""
A maintenance workflow that you can deploy into Airflow to periodically clean
out the task logs to avoid those getting too big.

airflow trigger_dag --conf '{"maxLogAgeInDays":30}' airflow-log-cleanup

--conf options:
    maxLogAgeInDays:<INT> - Optional
"""
import os

from datetime import datetime, timedelta

# pylint: disable=import-error
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.configuration import conf

# airflow-log-cleanup
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")

START_DATE = datetime(2017, 6, 20)

BASE_LOG_FOLDER = conf.get("core", "BASE_LOG_FOLDER")

# How often to Run. @daily - Once a day at Midnight
SCHEDULE_INTERVAL = "@daily"

# Who is listed as the owner of this DAG in the Airflow Web Server
DAG_OWNER_NAME = "operations"

# List of email address to send email alerts to if this job fails
ALERT_EMAIL_ADDRESSES = []

# Length to retain the log files if not already provided in the conf. If this is
# set to 30, the job will remove those files that are 30 days old or odler
DEFAULT_MAX_LOG_AGE_IN_DAYS = 30

# Whether the job should delete the logs or not. Included if you want to
# temporarily avoid deleting the logs
ENABLE_DELETE = True

# The number of worker nodes you have in Airflow. Will attempt to run this
# process for however many workers there are so that each worker gets its logs
# cleared.
NUMBER_OF_WORKERS = 1

DEFAULT_ARGS = {
    'owner': DAG_OWNER_NAME,
    'email': ALERT_EMAIL_ADDRESSES,
    'email_on_failure': True,
    'email_on_retry': False,
    'start_date': START_DATE,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

DAG_OBJ = DAG(DAG_ID,
              default_args=DEFAULT_ARGS,
              schedule_interval=SCHEDULE_INTERVAL,
              start_date=START_DATE)

# pylint: disable=line-too-long
LOG_CLEANUP_BASH = """
echo "Getting Configurations..."
BASE_LOG_FOLDER='""" + BASE_LOG_FOLDER + """'
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
echo ""
echo "Running Cleanup Process..."
FIND_STATEMENT="find ${BASE_LOG_FOLDER}/*/* -type f -mtime +${MAX_LOG_AGE_IN_DAYS}"
echo "Executing Find Statement: ${FIND_STATEMENT}"
FILES_MARKED_FOR_DELETE=`eval ${FIND_STATEMENT}`
echo "Process will be Deleting the following directories:"
echo "${FILES_MARKED_FOR_DELETE}"
echo "Process will be Deleting `echo "${FILES_MARKED_FOR_DELETE}" | grep -v '^$' | wc -l ` file(s)"     # "grep -v '^$'" - removes empty lines. "wc -l" - Counts the number of lines
echo ""
if [ "${ENABLE_DELETE}" == "true" ];
then
    DELETE_STMT="${FIND_STATEMENT} -delete"
    echo "Executing Delete Statement: ${DELETE_STMT}"
    eval ${DELETE_STMT}
    DELETE_STMT_EXIT_CODE=$?
    if [ "${DELETE_STMT_EXIT_CODE}" != "0" ]; then
        echo "Delete process failed with exit code '${DELETE_STMT_EXIT_CODE}'"
        exit ${DELETE_STMT_EXIT_CODE}
    fi
else
    echo "WARN: You're opted to skip deleting the files!!!"
fi
echo "Finished Running Cleanup Process"
"""

for log_cleanup_id in range(1, NUMBER_OF_WORKERS + 1):

    log_cleanup_opr = BashOperator(
        task_id='log_cleanup_' + str(log_cleanup_id),
        bash_command=LOG_CLEANUP_BASH,
        provide_context=True,
        dag=DAG_OBJ)
