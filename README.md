# airflow-maintenance-dags
A series of DAGs/Workflows to help maintain the operation of Airflow

## DAGs/Workflows

* db-cleanup
    * Periodically cleans out the DagRun and TaskInstance DB entries to avoid having too much data in your Airflow MetaStore.
* log-cleanup
    * Periodically cleans out the task logs to avoid those getting too big.
