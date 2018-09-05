from airflow.models import DAG, Variable
from airflow.operators.bash_operator import BashOperator
from airflow.configuration import conf
from datetime import datetime, timedelta
import os

"""
A maintenance workflow that you can deploy into Airflow to periodically clean out the task logs to avoid those getting too big.

airflow trigger_dag --conf '{"maxLogAgeInDays":30}' airflow-log-cleanup

--conf options:
    maxLogAgeInDays:<INT> - Optional

"""

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")  # airflow-log-cleanup
START_DATE = datetime.now() - timedelta(minutes=1)
BASE_LOG_FOLDER = conf.get("core", "BASE_LOG_FOLDER")
SCHEDULE_INTERVAL = "@daily"        # How often to Run. @daily - Once a day at Midnight
DAG_OWNER_NAME = "operations"       # Who is listed as the owner of this DAG in the Airflow Web Server
ALERT_EMAIL_ADDRESSES = []          # List of email address to send email alerts to if this job fails
DEFAULT_MAX_LOG_AGE_IN_DAYS = Variable.get("max_log_age_in_days", 30)    # Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or odler
ENABLE_DELETE = True                # Whether the job should delete the logs or not. Included if you want to temporarily avoid deleting the logs
NUMBER_OF_WORKERS = 1               # The number of worker nodes you have in Airflow. Will attempt to run this process for however many workers there are so that each worker gets its logs cleared.

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

log_cleanup = """
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

    log_cleanup = BashOperator(
        task_id='log_cleanup_' + str(log_cleanup_id),
        bash_command=log_cleanup,
        dag=dag)
