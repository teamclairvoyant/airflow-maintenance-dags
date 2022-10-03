# Airflow SLA Miss Report

  - [About](#about)
  - [Architecture](#architecture)
  - [Requirements](#requirements)
  - [Deployment](#deployment)
  - [References](#references)


### About
Airflow allows users to define [SLAs](https://github.com/teamclairvoyant/airflow-maintenance-dags/blob/teamclairvoyant/sla-miss-report/sla-miss-report/README.md) at DAG & task levels to track instances where processes are running longer than usual. However, making sense of the data is a challenge.

The `airflow-sla-miss-report` DAG consolidates the data from the metadata tables and provides meaningful insights to ensure SLAs are met when set.

The DAG utilizes three (3) timeframes (default: short: 1d, medium: 3d, long: 7d) to calcuate the following KPIs:

1. Daily SLA Misses (timeframe: long)
    - Following details broken down on a daily basis for the provided long timeframe (e.g. 7 days):
      - **SLA Miss %**: percentage of tasks that missed their SLAs out of total tasks runs
      - **Top Violator (%)**: task that violated its SLA the most as a percentage of its total runs
      - **Top Violator (absolute)**: task that violated its SLA the most on an absolute count basis during the day

2. Hourly SLA Misses (timeframe: short)
   - Following details broken down on an hourly basis for the provided short timeframe (e.g. 1 day):
      - **SLA Miss %**: percentage of tasks that missed their SLAs out of total tasks runs
      - **Top Violator (%)**: task that violated its SLA the most as a percentage of its total runs
      - **Top Violator (absolute)**: task that violated its SLA the most on an absolute count basis during the day
      - **Longest Running Task**: task that took the longest time to execute within the hour window
      - **Average Task Queue Time (seconds)**: avg time taken for tasks in `queued` state; can be used to detect scheduling bottlenecks

3. DAG SLA Misses (timeframe: short, medium, long)
    - Following details broken down on a task level for all timeframes:
      - **Current SLA**: current defined SLA for the task
      - **Short, Medium, Long Timeframe SLA miss % (avg execution time)**: % of tasks that missed their SLAs & their avg execution times over the respective timeframes

#### **Sample Email**
![Airflow SLA miss Email Report Output1](https://user-images.githubusercontent.com/32403237/193700720-24b88202-edae-4199-a7f3-0e46e54e0d5d.png)


### Architecture
The process reads data from the Airflow metadata database to calculate SLA misses based on the defined DAG/task level SLAs using information.
The following metadata tables are utilized:
- `SerializedDag`: retrieve defined DAG & task SLAs
- `DagRuns`: details about each DAG run
- `TaskInstances`: datails about each task instance in a DAG run

![Airflow SLA Process Flow Architecture](https://user-images.githubusercontent.com/8946659/191114560-2368e2df-916a-4f66-b1ac-b6cfe0b35a47.png)

### Requirements
- Python: 3.7 and above
- Airflow: v2.3 and above
- Airflow metadata tables: `DagRuns`, `TaskInstances`, `SerializedDag`

### Deployment
- Login to the machine running Airflow
- Navigate to the `dags` directory
- Copy the `airflow-sla-miss-report.py` file to the `dags` directory. Here's a fast way:
  ```
  wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/sla-miss-report/airflow-sla-miss-report.py
  ```
- Update the global variables in the DAG with the desired values:
  ```
  EMAIL_ADDRESS: list of recipient emails to send the SLA report
  SHORT_TIME_FRAME: duration in days of the short time frame to calculate SLA metrics (default: 1)
  MEDIUM_TIME_FRAME: duration in days of the medium time frame to calculate SLA metrics (default: 3)
  LONG_TIME_FRAME: duration in days of the long time frame to calculate SLA metrics (default: 7)
  ```
- Enable the DAG in the Airflow Webserver