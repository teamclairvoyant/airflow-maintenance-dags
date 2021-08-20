"""
A maintenance workflow that you can deploy into Airflow to periodically take
backups of various Airflow configurations and files.

airflow trigger_dag airflow-backup-configs

"""
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.configuration import conf
from datetime import datetime, timedelta
import os
import airflow
import logging
import subprocess
# airflow-backup-configs
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
# How often to Run. @daily - Once a day at Midnight
START_DATE = airflow.utils.dates.days_ago(1)
# Who is listed as the owner of this DAG in the Airflow Web Server
SCHEDULE_INTERVAL = "@daily"
# List of email address to send email alerts to if this job fails
DAG_OWNER_NAME = "operations"
ALERT_EMAIL_ADDRESSES = []
# Format options: https://www.tutorialspoint.com/python/time_strftime.htm
BACKUP_FOLDER_DATE_FORMAT = "%Y%m%d%H%M%S"
BACKUP_HOME_DIRECTORY = Variable.get("airflow_backup_config__backup_home_directory", "/tmp/airflow_backups")
BACKUPS_ENABLED = {
    "dag_directory": True,
    "log_directory": True,
    "airflow_cfg": True,
    "pip_packages": True
}
# How many backups to retain (not including the one that was just taken)
BACKUP_RETENTION_COUNT = 7

default_args = {
    'owner': DAG_OWNER_NAME,
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


def print_configuration_fn(**context):
    logging.info("Executing print_configuration_fn")

    logging.info("Loading Configurations...")
    BACKUP_FOLDER_DATE = datetime.now().strftime(BACKUP_FOLDER_DATE_FORMAT)
    BACKUP_DIRECTORY = BACKUP_HOME_DIRECTORY + "/" + BACKUP_FOLDER_DATE + "/"

    logging.info("Configurations:")
    logging.info(
        "BACKUP_FOLDER_DATE_FORMAT:    " + str(BACKUP_FOLDER_DATE_FORMAT)
    )
    logging.info("BACKUP_FOLDER_DATE:           " + str(BACKUP_FOLDER_DATE))
    logging.info("BACKUP_HOME_DIRECTORY:        " + str(BACKUP_HOME_DIRECTORY))
    logging.info("BACKUP_DIRECTORY:             " + str(BACKUP_DIRECTORY))
    logging.info(
        "BACKUP_RETENTION_COUNT:       " + str(BACKUP_RETENTION_COUNT)
    )
    logging.info("")

    logging.info("Pushing to XCom for Downstream Processes")
    context["ti"].xcom_push(
        key="backup_configs.backup_home_directory",
        value=BACKUP_HOME_DIRECTORY
    )
    context["ti"].xcom_push(
        key="backup_configs.backup_directory",
        value=BACKUP_DIRECTORY
    )
    context["ti"].xcom_push(
        key="backup_configs.backup_retention_count",
        value=BACKUP_RETENTION_COUNT
    )


print_configuration_op = PythonOperator(
    task_id='print_configuration',
    python_callable=print_configuration_fn,
    provide_context=True,
    dag=dag)


def execute_shell_cmd(cmd):
    logging.info("Executing Command: `" + cmd + "`")
    proc = subprocess.Popen(cmd, shell=True, universal_newlines=True)
    proc.communicate()
    exit_code = proc.returncode
    if exit_code != 0:
        exit(exit_code)


def delete_old_backups_fn(**context):
    logging.info("Executing delete_old_backups_fn")

    logging.info("Loading Configurations...")
    BACKUP_HOME_DIRECTORY = context["ti"].xcom_pull(
        task_ids=print_configuration_op.task_id,
        key='backup_configs.backup_home_directory'
    )
    BACKUP_RETENTION_COUNT = context["ti"].xcom_pull(
        task_ids=print_configuration_op.task_id,
        key='backup_configs.backup_retention_count'
    )

    logging.info("Configurations:")
    logging.info("BACKUP_HOME_DIRECTORY:    " + str(BACKUP_HOME_DIRECTORY))
    logging.info("BACKUP_RETENTION_COUNT:   " + str(BACKUP_RETENTION_COUNT))
    logging.info("")

    if BACKUP_RETENTION_COUNT < 0:
        logging.info(
            "Retention is less then 0. Assuming to allow infinite backups. "
            "Skipping..."
        )
        return

    backup_folders = [
        os.path.join(BACKUP_HOME_DIRECTORY, f)
        for f in os.listdir(BACKUP_HOME_DIRECTORY)
        if os.path.isdir(os.path.join(BACKUP_HOME_DIRECTORY, f))
    ]
    backup_folders.reverse()
    logging.info("backup_folders: " + str(backup_folders))
    logging.info("")

    cnt = 0
    for backup_folder in backup_folders:
        logging.info(
            "cnt = " + str(cnt) + ", backup_folder = " + str(backup_folder)
        )
        if cnt > BACKUP_RETENTION_COUNT:
            logging.info("Deleting Backup Folder: " + str(backup_folder))
            execute_shell_cmd("rm -rf " + str(backup_folder))
        cnt += 1


delete_old_backups_op = PythonOperator(
    task_id='delete_old_backups',
    python_callable=delete_old_backups_fn,
    provide_context=True,
    dag=dag)


def general_backup_fn(**context):
    logging.info("Executing general_backup_fn")

    logging.info("Loading Configurations...")
    PATH_TO_BACKUP = context["params"].get("path_to_backup")
    TARGET_DIRECTORY_NAME = context["params"].get("target_directory_name")
    BACKUP_DIRECTORY = context["ti"].xcom_pull(
        task_ids=print_configuration_op.task_id,
        key='backup_configs.backup_directory'
    )

    logging.info("Configurations:")
    logging.info("PATH_TO_BACKUP:           " + str(PATH_TO_BACKUP))
    logging.info("TARGET_DIRECTORY_NAME:    " + str(TARGET_DIRECTORY_NAME))
    logging.info("BACKUP_DIRECTORY:         " + str(BACKUP_DIRECTORY))
    logging.info("")

    execute_shell_cmd("mkdir -p " + str(BACKUP_DIRECTORY))

    execute_shell_cmd(
        "cp -r -n " + str(PATH_TO_BACKUP) + " " + str(BACKUP_DIRECTORY) +
        (TARGET_DIRECTORY_NAME if TARGET_DIRECTORY_NAME is not None else "")
    )


def pip_packages_backup_fn(**context):
    logging.info("Executing pip_packages_backup_fn")

    logging.info("Loading Configurations...")

    BACKUP_DIRECTORY = context["ti"].xcom_pull(
        task_ids=print_configuration_op.task_id,
        key='backup_configs.backup_directory'
    )

    logging.info("Configurations:")
    logging.info("BACKUP_DIRECTORY:     " + str(BACKUP_DIRECTORY))
    logging.info("")
    if not os.path.exists(BACKUP_DIRECTORY):
        os.makedirs(BACKUP_DIRECTORY)
    execute_shell_cmd("pip freeze > " + BACKUP_DIRECTORY + "pip_freeze.out")


if BACKUPS_ENABLED.get("dag_directory"):
    backup_op = PythonOperator(
        task_id='backup_dag_directory',
        python_callable=general_backup_fn,
        params={"path_to_backup": conf.get("core", "DAGS_FOLDER")},
        provide_context=True,
        dag=dag)
    print_configuration_op.set_downstream(backup_op)
    backup_op.set_downstream(delete_old_backups_op)

if BACKUPS_ENABLED.get("log_directory"):
    try:
        BASE_LOG_FOLDER = conf.get("core", "BASE_LOG_FOLDER")
    except Exception as e:
        BASE_LOG_FOLDER = conf.get("logging", "BASE_LOG_FOLDER")

    backup_op = PythonOperator(
        task_id='backup_log_directory',
        python_callable=general_backup_fn,
        params={
            "path_to_backup": BASE_LOG_FOLDER,
            "target_directory_name": "logs"
        },
        provide_context=True,
        dag=dag)
    print_configuration_op.set_downstream(backup_op)
    backup_op.set_downstream(delete_old_backups_op)

if BACKUPS_ENABLED.get("airflow_cfg"):
    backup_op = PythonOperator(
        task_id='backup_airflow_cfg',
        python_callable=general_backup_fn,
        params={
            "path_to_backup": (os.environ.get('AIRFLOW_HOME') if os.environ.get('AIRFLOW_HOME') is not None else "~/airflow/") + "/airflow.cfg"
        },
        provide_context=True,
        dag=dag)
    print_configuration_op.set_downstream(backup_op)
    backup_op.set_downstream(delete_old_backups_op)

if BACKUPS_ENABLED.get("pip_packages"):
    backup_op = PythonOperator(
        task_id='backup_pip_packages',
        python_callable=pip_packages_backup_fn,
        provide_context=True,
        dag=dag)
    print_configuration_op.set_downstream(backup_op)
    backup_op.set_downstream(delete_old_backups_op)
