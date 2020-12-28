# Airflow Kill Halted Tasks

A maintenance workflow that you can deploy into Airflow to periodically kill off tasks that are running in the background that don't correspond to a running task in the DB. 

This is useful because when you kill off a DAG Run or Task through the Airflow Web Server, the task still runs in the background on one of the executors until the task is complete.

## Deploy

1. Login to the machine running Airflow

2. Navigate to the dags directory

3. Copy the airflow-kill-halted-tasks.py file to this dags directory

       a. Here's a fast way:

                $ wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/master/kill-halted-tasks/airflow-kill-halted-tasks.py
        
4. Update the global variables in the DAG with the desired values 

5. Enable the DAG in the Airflow Webserver

