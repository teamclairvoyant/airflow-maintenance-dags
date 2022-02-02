# Airflow DB Cleanup

A maintenance workflow that you can deploy into Airflow to periodically clean out the DagRun, TaskInstance, Log, XCom, Job DB and SlaMiss entries to avoid having too much data in your Airflow MetaStore.

## Deploy

1. Login to the machine running Airflow

2. Navigate to the dags directory

3. Copy the airflow-db-cleanup.py file to this dags directory

       a. Here's a fast way:

                $ wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/master/db-cleanup/airflow-db-cleanup.py
        
4. Update the global variables (SCHEDULE_INTERVAL, DAG_OWNER_NAME, ALERT_EMAIL_ADDRESSES and ENABLE_DELETE) in the DAG with the desired values

5. Modify the DATABASE_OBJECTS list to add/remove objects as needed. Each dictionary in the list features the following parameters:
    - airflow_db_model: Model imported from airflow.models corresponding to a table in the airflow metadata database
    - age_check_column: Column in the model/table to use for calculating max date of data deletion
    - keep_last: Boolean to specify whether to preserve last run instance
        - keep_last_filters: List of filters to preserve data from deleting during clean-up, such as DAG runs where the external trigger is set to 0. 
        - keep_last_group_by: Option to specify column by which to group the database entries and perform aggregate functions.

6. Create and Set the following Variables in the Airflow Web Server (Admin -> Variables)

    - airflow_db_cleanup__max_db_entry_age_in_days - integer - Length to retain the log files if not already provided in the conf. If this is set to 30, the job will remove those files that are 30 days old or older.

7. Enable the DAG in the Airflow Webserver

## Covering indexes

Create some index for improving query performance:

```sql
CREATE INDEX idx_job_heartbeat_type ON airflow.job(latest_heartbeat, job_type);
```

```sql
CREATE INDEX idx_log_dttm ON airflow.log(dttm);
```

```sql
CREATE INDEX idx_task_reschedule_reschedule_date ON airflow.task_reschedule(reschedule_date);
```

```sql
CREATE INDEX idx_task_instance_end_date ON airflow.task_instance(end_date);
```

```sql
CREATE INDEX idx_task_instance_start_date ON airflow.task_instance(start_date);
```

```sql
CREATE INDEX idx_dag_run_dag_id_external_trigger_execution_date ON airflow.dag_run(dag_id, external_trigger, execution_date);
```

```sql
CREATE INDEX idx_dag_run_execution_date ON airflow.dag_run(execution_date);
```

```sql
CREATE INDEX idx_celery_taskmeta_date_done ON airflow.celery_taskmeta(date_done);
```

## Variable

+ `max_date`: `utcnow()` &minus; `airflow_db_cleanup__max_db_entry_age_in_days`

## ORM

Using `sqlalchemy` to generate delete statements

## BaseJob

+ table: `airflow.job`

+ column: `latest_heartbeat`

+ query:

    ```sql
    SELECT 
        job.id               AS job_id, 
        job.job_type         AS job_job_type, 
        job.latest_heartbeat AS job_latest_heartbeat
    FROM 
        job
    WHERE 
        job.latest_heartbeat <= max_date;
    ```

#### Log

+ table: `airflow.log`

+ column: `dttm`

+ query:

    ```sql
    SELECT 
        log.id   AS log_id, 
        log.dttm AS log_dttm
    FROM 
        log
    WHERE 
        log.dttm <= max_date;
    ```

#### XCom

+ table: `airflow.xcom`

+ column: `execution_date`

+ query:

    ```sql
    SELECT 
        xcom.`key`          AS xcom_key, 
        xcom.execution_date AS xcom_execution_date, 
        xcom.task_id        AS xcom_task_id, 
        xcom.dag_id         AS xcom_dag_id
    FROM 
        xcom
    WHERE 
        xcom.execution_date <= max_date;
    ```

#### SlaMiss

+ table: `airflow.sla_miss`

+ column: `execution_date`

+ query:

    ```sql
    SELECT 
        sla_miss.task_id        AS sla_miss_task_id, 
        sla_miss.dag_id         AS sla_miss_dag_id, 
        sla_miss.execution_date AS sla_miss_execution_date
    FROM 
        sla_miss
    WHERE 
        sla_miss.execution_date <= max_date;
    ```

#### DagModel

+ table: `airflow.dag`

+ column: `last_parsed_time`

+ query:

    ```sql
    SELECT 
        dag.dag_id           AS dag_dag_id, 
        dag.last_parsed_time AS dag_last_parsed_time
    FROM 
        dag
    WHERE 
        dag.last_parsed_time <= max_date;
    ```

#### TaskFail

+ table: `airflow.task_fail`

+ column: `execution_date`

+ query:

    ```sql
    SELECT 
        task_fail.id             AS task_fail_id, 
        task_fail.execution_date AS task_fail_execution_date
    FROM 
        task_fail
    WHERE 
        task_fail.execution_date <= max_date;
    ```

#### RenderedTaskInstanceFields

+ table: `airflow.rendered_task_instance_fields`

+ column: `execution_date`

+ query:

    ```sql
    SELECT 
        rendered_task_instance_fields.dag_id         AS rendered_task_instance_fields_dag_id, 
        rendered_task_instance_fields.task_id        AS rendered_task_instance_fields_task_id, 
        rendered_task_instance_fields.execution_date AS rendered_task_instance_fields_execution_date
    FROM 
        rendered_task_instance_fields
    WHERE 
        rendered_task_instance_fields.execution_date <= max_date;
    ```

#### ImportError

+ table: `airflow.import_error`

+ column: `timestamp`

+ query:

    ```sql
    SELECT 
        import_error.id        AS import_error_id, 
        import_error.timestamp AS import_error_timestamp
    FROM 
        import_error
    WHERE 
        import_error.timestamp <= max_date
    ```

#### TaskReschedule

+ table: `airflow.task_reschedule`

+ column: `reschedule_date`

+ query:

    ```sql
    SELECT 
        task_reschedule.id              AS task_reschedule_id, 
        task_reschedule.reschedule_date AS task_reschedule_reschedule_date
    FROM 
        task_reschedule
    WHERE 
        task_reschedule.reschedule_date <= max_date;
    ```

+ FK-constraints:

    + `dag_id, run_id` &rightarrow; `dag_run(dag_id, run_id)`

    + `dag_id, task_id, run_id` &rightarrow; `task_instance(dag_id, task_id, run_id)`

#### TaskInstance

> Has impact on TaskReschedule through constraints

+ table: `airflow.task_instance`

+ column: `end_date` and `start_date`

+ query:

    ```sql
    SELECT 
        task_instance.task_id              AS task_instance_task_id, 
        task_instance.dag_id               AS task_instance_dag_id, 
        task_instance.run_id               AS task_instance_run_id, 
        task_instance.end_date             AS task_instance_end_date, 
        dag_run_1.state                    AS dag_run_1_state, 
        dag_run_1.id                       AS dag_run_1_id, 
        dag_run_1.dag_id                   AS dag_run_1_dag_id, 
        dag_run_1.queued_at                AS dag_run_1_queued_at, 
        dag_run_1.execution_date           AS dag_run_1_execution_date, 
        dag_run_1.start_date               AS dag_run_1_start_date, 
        dag_run_1.end_date                 AS dag_run_1_end_date, 
        dag_run_1.run_id                   AS dag_run_1_run_id, 
        dag_run_1.creating_job_id          AS dag_run_1_creating_job_id, 
        dag_run_1.external_trigger         AS dag_run_1_external_trigger, 
        dag_run_1.run_type                 AS dag_run_1_run_type, 
        dag_run_1.conf                     AS dag_run_1_conf, 
        dag_run_1.data_interval_start      AS dag_run_1_data_interval_start, 
        dag_run_1.data_interval_end        AS dag_run_1_data_interval_end, 
        dag_run_1.last_scheduling_decision AS dag_run_1_last_scheduling_decision, 
        dag_run_1.dag_hash                 AS dag_run_1_dag_hash
    FROM 
        task_instance 
    INNER JOIN 
        dag_run AS dag_run_1 
    ON 
        dag_run_1.dag_id = task_instance.dag_id 
    AND dag_run_1.run_id = task_instance.run_id
    WHERE 
        task_instance.end_date <= max_date;
    ```

+ FK-constraints:

    + `dag_id, run_id` &rightarrow; `dag_run(dag_id, run_id)`

    + `trigger_id` &rightarrow; `trigger(id)`

#### DagRun

> Has impact on TaskInstance and TaskReschedule through constraints

+ table: `airflow.dag_run`

+ column: `execution_date`

+ query:

    ```sql
    SELECT 
        dag_run.id             AS dag_run_id, 
        dag_run.execution_date AS dag_run_execution_date
    FROM 
        dag_run
    WHERE 
        dag_run.execution_date NOT IN 
        (   SELECT 
                anon_1.max_1 AS anon_1_max_1
            FROM 
                (   SELECT 
                        MAX(dag_run.execution_date) AS max_1
                    FROM 
                        dag_run
                    WHERE 
                        dag_run.external_trigger IS false 
                    GROUP BY 
                        dag_run.dag_id) AS anon_1) 
    AND dag_run.execution_date <= max_date;
    ```
  
#### Task

> Celery specific

+ table: `airflow.celery_taskmeta`

+ column: `date_done`

+ query:

    ```sql
    SELECT 
        celery_taskmeta.id        AS celery_taskmeta_id, 
        celery_taskmeta.date_done AS celery_taskmeta_date_done
    FROM 
        celery_taskmeta
    WHERE 
        celery_taskmeta.date_done <= max_date;
    ```
  
#### TaskSet

> Celery specific

+ table: `airflow.celery_tasksetmeta`

+ column: `date_done`

+ query:

    ```sql
    SELECT 
        celery_tasksetmeta.id        AS celery_tasksetmeta_id, 
        celery_tasksetmeta.date_done AS celery_tasksetmeta_date_done
    FROM 
        celery_tasksetmeta
    WHERE 
        celery_tasksetmeta.date_done <= max_date;
    ```
