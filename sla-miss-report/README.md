# Airflow SLA Miss Analysis Report
## Table Of Contents
1. [About](##about)
2. [Process Flow Architecture](##process-flow-architecture)
3. [Requirements](##requirements)
4. [Installation](##installation)
5. [Deployment](##deployment)
6. [References](##references)


### About

The set of expectations of a customer from the service he/she receives is called the Service Level Agreement, or in short, the SLA. 
In most cases, Apache Airflow helps us achieve this. In Airflow, a DAG – or a Directed Acyclic Graph – is a collection of all the tasks you want to run, organized in a way that reflects their relationships and dependencies. In this project, a DAG file is being created that produces a SLA report with usable metrics and custom KPI integrations which DAG creators/Data Engineers can utilize to adjust existing SLAs and manage expectations. It helps us to overcome the common pain points faced while trying to interpret through the current Airflow SLA miss UI. The report is in the form of an email having all the metrics and insights of SLA Misses. Below are the 3 KPI's and a brief description on them:

1. Daily SLA Misses
    - Based on the longtimeframe duration specified by the user, this KPI calculates the SLA miss % and top violated Dag:Task based on the % and absolute number which has missed it's SLA benchmark for each and every date within the timeframe.
    - The input given is the sla_run detail dataframe filtered for the respective duration.

2. Hourly SLA Misses
    - Based on the longtimeframe duration specified by the user, this KPI calculates the average hourly SLA miss % and top violated Dag:Task based on the % and absolute number. Along with this, it tells us which task took the longest time to run and the average task queue time for that particular hour
    - The input given is the sla_run detail dataframe filtered for the respective duration.

2. DAG SLA Misses
    - Based on the short, medium and longtimeframe duration specified by the user, this KPI calculates the SLA miss % and average execution time of all the Dag:Task in that duration and offers a comparitive view for the DAG creator to compare and analyse the pipelines over the 3 timeframes.
    - The data is sorted based on the SLA miss %
    - The input given is the sla_run detail dataframe filtered for the respective duration and the serialized dag table consisting data of all the dags.



The expected output is an email report with all the metrics. Attached is a sample report. It can be found in the email addresses as specified by the user.
![Airflow SLA miss Email Report Output1](https://user-images.githubusercontent.com/8946659/191114560-2368e2df-916a-4f66-b1ac-b6cfe0b35a47.png")


### Process Flow Architecture
![Airflow SLA Process Flow Architecture](https://user-images.githubusercontent.com/8946659/191114560-2368e2df-916a-4f66-b1ac-b6cfe0b35a47.png)

### Requirements
- Python: 3.7 and above
- Airflow: v2.3 and above

### Installation
The source code can be installed by cloning the repository below
with HTTPS :

    $ git clone https://github.com/teamclairvoyant/airflow-maintenance-dags.git

with git:

    gh repo clone teamclairvoyant/airflow-maintenance-dags

Please note that the project is in the **airflow-maintenance-dags/sla-miss-report/**

### Deployment
- Login to the machine running Airflow
- Navigate to the dags directory
- Copy the airflow-sla-miss-report.py file to this dags directory
a. Here's a fast way:

         $ wget https://raw.githubusercontent.com/teamclairvoyant/airflow-maintenance-dags/sla-miss-report/airflow-sla-miss-report.py
- Update the global variables (EMAIL_ADDRESS, SHORT_TIME_FRAME, MEDIUM_TIME_FRAME and LONG_TIME_FRAME) in the DAG with the desired values
- Enable the DAG in the Airflow Webserver
- Airflow will self detect any DAG placed under that folder and run according to the schedule defined within the DAG file. Reference link is provided on how to create DAG's and how to setup the schedule interval within DAG's.

### References
        
- https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html#using-pypi
- https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html
- https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html
- https://airflow.apache.org/docs/apache-airflow/1.10.10/scheduler.html
- https://betterdatascience.com/apache-airflow-install/