"""
A maintenance workflow that you can deploy into Airflow to periodically kill
off tasks that are running in the background that don't correspond to a running
task in the DB.

This is useful because when you kill off a DAG Run or Task through the Airflow
Web Server, the task still runs in the background on one of the executors until
the task is complete.

airflow trigger_dag airflow-kill-halted-tasks

"""
from airflow.models import DAG, DagModel, DagRun, TaskInstance
from airflow import settings
from airflow.operators.python_operator \
    import PythonOperator, ShortCircuitOperator
from airflow.operators.email_operator import EmailOperator
from sqlalchemy import and_
from datetime import datetime, timedelta
import os
import re
import logging
import pytz
import airflow


# airflow-kill-halted-tasks
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
DOC_MD = f"""
### [README.md](https://github.com/teamclairvoyant/airflow-maintenance-dags/tree/master/{DAG_ID})
"""

START_DATE = airflow.utils.dates.days_ago(1)
# How often to Run. @daily - Once a day at Midnight. @hourly - Once an Hour.
SCHEDULE_INTERVAL = "@hourly"
# Who is listed as the owner of this DAG in the Airflow Web Server
DAG_OWNER_NAME = "operations"
# List of email address to send email alerts to if this job fails
ALERT_EMAIL_ADDRESSES = []
# Whether to send out an email whenever a process was killed during a DAG Run
# or not
SEND_PROCESS_KILLED_EMAIL = True
# Subject of the email that is sent out when a task is killed by the DAG
PROCESS_KILLED_EMAIL_SUBJECT = DAG_ID + " - Tasks were Killed"
# List of email address to send emails to when a task is killed by the DAG
PROCESS_KILLED_EMAIL_ADDRESSES = []
# Whether the job should delete the db entries or not. Included if you want to
# temporarily avoid deleting the db entries.
ENABLE_KILL = True
# Whether to print out certain statements meant for debugging
DEBUG = False

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
    dag.doc_md = DOC_MD
if hasattr(dag, 'catchup'):
    dag.catchup = False


uid_regex = "(\w+)"
pid_regex = "(\w+)"
ppid_regex = "(\w+)"
processor_scheduling_regex = "(\w+)"
start_time_regex = "([\w:.]+)"
tty_regex = "([\w?/]+)"
cpu_time_regex = "([\w:.]+)"
command_regex = "(.+)"

# When Search Command is:  ps -o pid -o cmd -u `whoami` | grep 'airflow run'
full_regex = '\s*' + pid_regex + '\s+' + command_regex

airflow_run_regex = '.*run\s([\w._-]*)\s([\w._-]*)\s([\w:.-]*).*'


def parse_process_linux_string(line):
    if DEBUG:
        logging.info("DEBUG: Processing Line: " + str(line))
    full_regex_match = re.search(full_regex, line)
    if DEBUG:
        for index in range(0, (len(full_regex_match.groups()) + 1)):
            group = full_regex_match.group(index)
            logging.info(
                "DEBUG: index: " + str(index) + ", group: " + str(group)
            )
    pid = full_regex_match.group(1)
    command = full_regex_match.group(2).strip()
    process = {"pid": pid, "command": command}

    if DEBUG:
        logging.info("DEBUG: Processing Command: " + str(command))
    airflow_run_regex_match = re.search(airflow_run_regex, command)
    if DEBUG:
        for index in range(0, (len(airflow_run_regex_match.groups()) + 1)):
            group = airflow_run_regex_match.group(index)
            logging.info(
                "DEBUG: index: " + str(index) + ", group: " + str(group)
            )
    process["airflow_dag_id"] = airflow_run_regex_match.group(1)
    process["airflow_task_id"] = airflow_run_regex_match.group(2)
    process["airflow_execution_date"] = airflow_run_regex_match.group(3)
    return process


def kill_halted_tasks_function(**context):
    logging.info("Getting Configurations...")
    airflow_version = airflow.__version__
    session = settings.Session()

    logging.info("Finished Getting Configurations\n")

    logging.info("Configurations:")
    logging.info(
        "send_process_killed_email:      " + str(SEND_PROCESS_KILLED_EMAIL)
    )
    logging.info(
        "process_killed_email_subject:   " + str(PROCESS_KILLED_EMAIL_SUBJECT)
    )
    logging.info(
        "process_killed_email_addresses: " +
        str(PROCESS_KILLED_EMAIL_ADDRESSES)
    )
    logging.info("enable_kill:                    " + str(ENABLE_KILL))
    logging.info("debug:                          " + str(DEBUG))
    logging.info("session:                        " + str(session))
    logging.info("airflow_version:                " + str(airflow_version))
    logging.info("")

    logging.info("Running Cleanup Process...")
    logging.info("")

    process_search_command = (
        "ps -o pid -o cmd -u `whoami` | grep 'airflow run'"
    )
    logging.info("Running Search Process: " + process_search_command)
    search_output = os.popen(process_search_command).read()
    logging.info("Search Process Output: ")
    logging.info(search_output)

    logging.info(
        "Filtering out: Empty Lines, Grep processes, and this DAGs Run."
    )
    search_output_filtered = [
        line for line in search_output.split("\n") if line is not None
        and line.strip() != "" and ' grep ' not in line
        and DAG_ID not in line
    ]
    logging.info("Search Process Output (with Filter): ")
    for line in search_output_filtered:
        logging.info(line)
    logging.info("")

    logging.info("Searching through running processes...")
    airflow_timezone_not_required_versions = ['1.7', '1.8', '1.9']
    processes_to_kill = []
    for line in search_output_filtered:
        logging.info("")
        process = parse_process_linux_string(line=line)

        logging.info("Checking: " + str(process))
        exec_date_str = (process["airflow_execution_date"]).replace("T", " ")
        if '.' not in exec_date_str:
            # Add milliseconds if they are missing.
            exec_date_str = exec_date_str + '.0'
        execution_date_to_search_for = datetime.strptime(
            exec_date_str, '%Y-%m-%d %H:%M:%S.%f'
        )
        # apache-airflow version >= 1.10 requires datetime field values with
        # timezone
        if airflow_version[:3] not in airflow_timezone_not_required_versions:
            execution_date_to_search_for = pytz.utc.localize(
                execution_date_to_search_for
            )

        logging.info(
            "Execution Date to Search For: " +
            str(execution_date_to_search_for)
        )

        # Checking to make sure the DAG is available and active
        if DEBUG:
            logging.info("DEBUG: Listing All DagModels: ")
            for dag in session.query(DagModel).all():
                logging.info(
                    "DEBUG: dag: " + str(dag) + ", dag.is_active: " +
                    str(dag.is_active)
                )
            logging.info("")
        logging.info(
            "Getting dag where DagModel.dag_id == '" +
            str(process["airflow_dag_id"]) + "'"
        )
        dag = session.query(DagModel).filter(
            DagModel.dag_id == process["airflow_dag_id"]
        ).first()
        logging.info("dag: " + str(dag))
        if dag is None:
            kill_reason = "DAG was not found in metastore."
            process["kill_reason"] = kill_reason
            processes_to_kill.append(process)
            logging.warn(kill_reason)
            logging.warn("Marking process to be killed.")
            continue
        logging.info("dag.is_active: " + str(dag.is_active))
        if not dag.is_active:  # is the dag active?
            kill_reason = "DAG was found to be Disabled."
            process["kill_reason"] = kill_reason
            processes_to_kill.append(process)
            logging.warn(kill_reason)
            logging.warn("Marking process to be killed.")
            continue

        # Checking to make sure the DagRun is available and in a running state
        if DEBUG:
            dag_run_relevant_states = ["queued", "running", "up_for_retry"]
            logging.info(
                "DEBUG: Listing All Relevant DAG Runs (With State: " +
                str(dag_run_relevant_states) + "): "
            )
            for dag_run in session.query(DagRun).filter(
                DagRun.state.in_(dag_run_relevant_states)
            ).all():
                logging.info(
                    "DEBUG: dag_run: " + str(dag_run) + ", dag_run.state: " +
                    str(dag_run.state)
                )
            logging.info("")
        logging.info(
            "Getting dag_run where DagRun.dag_id == '" +
            str(process["airflow_dag_id"]) +
            "' AND DagRun.execution_date == '" +
            str(execution_date_to_search_for) + "'"
        )

        dag_run = session.query(DagRun).filter(
            and_(
                DagRun.dag_id == process["airflow_dag_id"],
                DagRun.execution_date == execution_date_to_search_for,
            )
        ).first()

        logging.info("dag_run: " + str(dag_run))
        if dag_run is None:
            kill_reason = "DAG RUN was not found in metastore."
            process["kill_reason"] = kill_reason
            processes_to_kill.append(process)
            logging.warn(kill_reason)
            logging.warn("Marking process to be killed.")
            continue
        logging.info("dag_run.state: " + str(dag_run.state))
        dag_run_states_required = ["running"]
        # is the dag_run in a running state?
        if dag_run.state not in dag_run_states_required:
            kill_reason = (
                "DAG RUN was found to not be in the states '" +
                str(dag_run_states_required) +
                "', but rather was in the state '" + str(dag_run.state) + "'."
            )
            process["kill_reason"] = kill_reason
            processes_to_kill.append(process)
            logging.warn(kill_reason)
            logging.warn("Marking process to be killed.")
            continue

        # Checking to ensure TaskInstance is available and in a running state
        if DEBUG:
            task_instance_relevant_states = [
                "queued", "running", "up_for_retry"
            ]
            logging.info(
                "DEBUG: Listing All Relevant TaskInstances (With State: " +
                str(task_instance_relevant_states) + "): "
            )
            for task_instance in session.query(TaskInstance).filter(
                TaskInstance.state.in_(task_instance_relevant_states)
            ).all():
                logging.info(
                    "DEBUG: task_instance: " + str(task_instance) +
                    ", task_instance.state: " + str(task_instance.state)
                )
            logging.info("")
        logging.info(
            "Getting task_instance where TaskInstance.dag_id == '" +
            str(process["airflow_dag_id"]) +
            "' AND TaskInstance.task_id == '" +
            str(process["airflow_task_id"]) +
            "' AND TaskInstance.execution_date == '" +
            str(execution_date_to_search_for) + "'"
        )

        task_instance = session.query(TaskInstance).filter(
            and_(
                TaskInstance.dag_id == process["airflow_dag_id"],
                TaskInstance.task_id == process["airflow_task_id"],
                TaskInstance.execution_date == execution_date_to_search_for,
            )
        ).first()

        logging.info("task_instance: " + str(task_instance))
        if task_instance is None:
            kill_reason = (
                "Task Instance was not found in metastore. Marking process "
                "to be killed."
            )
            process["kill_reason"] = kill_reason
            processes_to_kill.append(process)
            logging.warn(kill_reason)
            logging.warn("Marking process to be killed.")
            continue
        logging.info("task_instance.state: " + str(task_instance.state))
        task_instance_states_required = ["queued", "running", "up_for_retry"]
        # is task_instance queued, running or up for retry?
        if task_instance.state not in task_instance_states_required:
            kill_reason = (
                "The TaskInstance was found to not be in the states '" +
                str(task_instance_states_required) +
                "', but rather was in the state '" +
                str(task_instance.state) + "'."
            )
            process["kill_reason"] = kill_reason
            processes_to_kill.append(process)
            logging.warn(kill_reason)
            logging.warn("Marking process to be killed.")
            continue

    # Listing processes that will be killed
    logging.info("")
    logging.info("Processes Marked to Kill: ")
    if len(processes_to_kill) > 0:
        for process in processes_to_kill:
            logging.info(str(process))
    else:
        logging.info("No Processes Marked to Kill Found")

    # Killing the processes
    logging.info("")
    if ENABLE_KILL:
        logging.info("Performing Kill...")
        if len(processes_to_kill) > 0:
            for process in processes_to_kill:
                logging.info("Killing Process: " + str(process))
                kill_command = "kill -9 " + str(process["pid"])
                logging.info("Running Command: " + str(kill_command))
                output = os.popen(kill_command).read()
                logging.info("kill output: " + str(output))
            context['ti'].xcom_push(
                key='kill_halted_tasks.processes_to_kill',
                value=processes_to_kill
            )
            logging.info("Finished Performing Kill")
        else:
            logging.info("No Processes Marked to Kill Found")
    else:
        logging.warn("You're opted to skip killing the processes!!!")

    logging.info("")
    logging.info("Finished Running Cleanup Process")


kill_halted_tasks_op = PythonOperator(
    task_id='kill_halted_tasks',
    python_callable=kill_halted_tasks_function,
    provide_context=True,
    dag=dag)


def branch_function(**context):
    logging.info(
        "Deciding whether to send an email about tasks that were killed by " +
        "this DAG..."
    )
    logging.info(
        "SEND_PROCESS_KILLED_EMAIL: '" +
        str(SEND_PROCESS_KILLED_EMAIL) + "'"
    )
    logging.info(
        "PROCESS_KILLED_EMAIL_ADDRESSES: " +
        str(PROCESS_KILLED_EMAIL_ADDRESSES)
    )
    logging.info("ENABLE_KILL: " + str(ENABLE_KILL))

    if not SEND_PROCESS_KILLED_EMAIL:
        logging.info(
            "Skipping sending an email since SEND_PROCESS_KILLED_EMAIL is " +
            "set to false"
        )
        # False = short circuit the dag and don't execute downstream tasks
        return False
    if len(PROCESS_KILLED_EMAIL_ADDRESSES) == 0:
        logging.info(
            "Skipping sending an email since PROCESS_KILLED_EMAIL_ADDRESSES " +
            "is empty"
        )
        # False = short circuit the dag and don't execute downstream tasks
        return False

    processes_to_kill = context['ti'].xcom_pull(
        task_ids=kill_halted_tasks_op.task_id,
        key='kill_halted_tasks.processes_to_kill'
    )
    logging.info("processes_to_kill from xcom_pull: " + str(processes_to_kill))
    if processes_to_kill is not None and len(processes_to_kill) > 0:
        logging.info("There were processes to kill")
        if ENABLE_KILL:
            logging.info("enable_kill is set to true")
            logging.info(
                "Opting to send an email to alert the users that processes " +
                "were killed"
            )
            # True = don't short circuit the dag and execute downstream tasks
            return True
        else:
            logging.info("enable_kill is set to False")
    else:
        logging.info("Processes to kill list was either None or Empty")

    logging.info(
        "Opting to skip sending an email since no processes were killed"
    )
    # False = short circuit the dag and don't execute downstream tasks
    return False


email_or_not_branch = ShortCircuitOperator(
    task_id="email_or_not_branch",
    python_callable=branch_function,
    provide_context=True,
    dag=dag)


send_processes_killed_email = EmailOperator(
    task_id="send_processes_killed_email",
    to=PROCESS_KILLED_EMAIL_ADDRESSES,
    subject=PROCESS_KILLED_EMAIL_SUBJECT,
    html_content="""
<html>
    <body>
        <h6>This is not a failure alert!</h6>
        <h2>Dag Run Information</h2>
        <table>
            <tr><td><b> ID: </b></td><td>{{ dag_run.id }}</td></tr>
            <tr><td><b> DAG ID: </b></td><td>{{ dag_run.dag_id }}</td></tr>
            <tr><td><b> Execution Date: </b></td><td>
                {{ dag_run.execution_date }}
            </td></tr>
            <tr><td><b> Start Date: </b></td><td>
                {{ dag_run.start_date }}
            </td></tr>
            <tr><td><b> End Date: </b></td><td>{{ dag_run.end_date }}</td></tr>
            <tr><td><b> Run ID: </b></td><td>{{ dag_run.run_id }}</td></tr>
            <tr><td><b> External Trigger: </b></td><td>
                {{ dag_run.external_trigger }}
            </td></tr>
        </table>
        <h2>Task Instance Information</h2>
        <table>
            <tr><td><b> Task ID: </b></td><td>
                {{ task_instance.task_id }}
            </td></tr>
            <tr><td><b> Execution Date: </b></td><td>
                {{ task_instance.execution_date }}
            </td></tr>
            <tr><td><b> Start Date: </b></td><td>
                {{ task_instance.start_date }}
            </td></tr>
            <tr><td><b> End Date: </b></td><td>
                {{ task_instance.end_date }}
            </td></tr>
            <tr><td><b> Host Name: </b></td><td>
                {{ task_instance.hostname }}
            </td></tr>
            <tr><td><b> Unix Name: </b></td><td>
                {{ task_instance.unixname }}
            </td></tr>
            <tr><td><b> Job ID: </b></td><td>
                {{ task_instance.job_id }}
            </td></tr>
            <tr><td><b> Queued Date Time: </b></td><td>
                {{ task_instance.queued_dttm }}
            </td></tr>
            <tr><td><b> Log URL: </b></td><td>
                <a href="{{ task_instance.log_url }}">
                    {{ task_instance.log_url }}
                </a>
            </td></tr>
        </table>
        <h2>Processes Killed</h2>
        <ul>
        {% for process_killed in task_instance.xcom_pull(
            task_ids='kill_halted_tasks',
            key='kill_halted_tasks.processes_to_kill'
        ) %}
            <li>Process {{loop.index}}</li>
            <ul>
            {% for key, value in process_killed.iteritems() %}
                <li>{{ key }}: {{ value }}</li>
            {% endfor %}
            </ul>
        {% endfor %}
        </ul>
    </body>
</html>
    """,
    dag=dag)


kill_halted_tasks_op.set_downstream(email_or_not_branch)
email_or_not_branch.set_downstream(send_processes_killed_email)
