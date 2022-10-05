# Airflow SLA Miss Report

  - [About](#about)
    - [Daily SLA Misses (timeframe: `long`)](#daily-sla-misses-timeframe-long)
    - [Hourly SLA Misses (timeframe: `short`)](#hourly-sla-misses-timeframe-short)
    - [DAG SLA Misses (timeframe: `short, medium, long`)](#dag-sla-misses-timeframe-short-medium-long)
    - [Sample Email](#sample-email)
    - [Sample Airflow Task Logs](#sample-airflow-task-logs)
  - [Architecture](#architecture)
  - [Requirements](#requirements)
  - [Deployment](#deployment)
  - [References](#references)


### About
Airflow allows users to define [SLAs](https://github.com/teamclairvoyant/airflow-maintenance-dags/blob/teamclairvoyant/sla-miss-report/sla-miss-report/README.md) at DAG & task levels to track instances where processes are running longer than usual. However, making sense of the data is a challenge.

The `airflow-sla-miss-report` DAG consolidates the data from the metadata tables and provides meaningful insights to ensure SLAs are met when set.

The DAG utilizes **three (3) timeframes** (default: `short`: 1d, `medium`: 3d, `long`: 7d) to calculate the following KPIs:

#### Daily SLA Misses (timeframe: `long`)
Following details broken down on a daily basis for the provided long timeframe (e.g. 7 days):
```
  SLA Miss %: percentage of tasks that missed their SLAs out of total tasks runs
  Top Violator (%): task that violated its SLA the most as a percentage of its total runs
  Top Violator (absolute): task that violated its SLA the most on an absolute count basis during the day
```

#### Hourly SLA Misses (timeframe: `short`)
Following details broken down on an hourly basis for the provided short timeframe (e.g. 1 day):
```
  SLA Miss %: percentage of tasks that missed their SLAs out of total tasks runs
  Top Violator (%): task that violated its SLA the most as a percentage of its total runs
  Top Violator (absolute): task that violated its SLA the most on an absolute count basis during the day
  Longest Running Task: task that took the longest time to execute within the hour window
  Average Task Queue Time (s): avg time taken for tasks in `queued` state; can be used to detect scheduling bottlenecks
```

#### DAG SLA Misses (timeframe: `short, medium, long`)
Following details broken down on a task level for all timeframes:
```
  Current SLA (s): current defined SLA for the task
  Short, Medium, Long Timeframe SLA miss % (avg execution time): % of tasks that missed their SLAs & their avg execution times over the respective timeframes
```

#### **Sample Email**
![Airflow SLA miss Email Report Output1](https://user-images.githubusercontent.com/32403237/193700720-24b88202-edae-4199-a7f3-0e46e54e0d5d.png)

#### **Sample Airflow Task Logs**
![Airflow SLA miss Email Report Output2](https://user-images.githubusercontent.com/32403237/194130208-da532d3a-3ff4-4dbd-9c94-574ef42b2ee8.png)


### Architecture
The process reads data from the Airflow metadata database to calculate SLA misses based on the defined DAG/task level SLAs using information.
The following metadata tables are utilized:
- `SerializedDag`: retrieve defined DAG & task SLAs
- `DagRuns`: details about each DAG run
- `TaskInstances`: details about each task instance in a DAG run

![Airflow SLA Process Flow Architecture](https://user-images.githubusercontent.com/8946659/191114560-2368e2df-916a-4f66-b1ac-b6cfe0b35a47.png)

### Requirements
- Python: 3.7 and above
- Pip packages: `pandas`
- Airflow: v2.3 and above
- Airflow metadata tables: `DagRuns`, `TaskInstances`, `SerializedDag`
- [SMTP details](https://airflow.apache.org/docs/apache-airflow/stable/howto/email-config.html#using-default-smtp) in `airflow.cfg` for sending emails

### Deployment
1. Login to the machine running Airflow
2. Navigate to the `dags` directory
3. Copy the `airflow-sla-miss-report.py` file to the `dags` directory. Here's a fast way:
  ```
  wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/master/sla-miss-report/airflow-sla-miss-report.py
  ```
4. Update the global variables in the DAG with the desired values:
  ```
  EMAIL_ADDRESSES (optional): list of recipient emails to send the SLA report
  SHORT_TIMEFRAME_IN_DAYS: duration in days of the short timeframe to calculate SLA metrics (default: 1)
  MEDIUM_TIMEFRAME_IN_DAYS: duration in days of the medium timeframe to calculate SLA metrics (default: 3)
  LONG_TIMEFRAME_IN_DAYS: duration in days of the long timeframe to calculate SLA metrics (default: 7)
  ```
5. Enable the DAG in the Airflow Webserver