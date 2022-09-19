# Airflow SLA Miss Analysis Report
## Table Of Contents
1. [About](##about)
2. [Process Flow Architecture](##process-flow-architecture)
3. [Dependencies](##dependencies)
4. [Setup](##setup)
    - Installation
    - Deployment


## About

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

 The user have the option to modify 2 things in the code. The first customisation is the list of email addresses to whom the email report should be sent. Second is the short, medium and long timeframe duration for which you want to analyse your airflow data. All you have to do is to update the receiver email addresses and timeframe values and save the file in the dags folder under your airflow directory locally. The Airflow will self detect any DAG placed under that folder and run according to the schedule defined within the DAG file. Reference link is provided on how to create DAG's and how to setup the schedule interval within DAG's.

The expected output is an email report with all the metrics. Attached is a sample report. It can be found in the email addresses as specified by the user.
![Airflow SLA miss Email Report Output1](<img width="1363" alt="af1" src="https://user-images.githubusercontent.com/8946659/191114427-e5ff894d-c888-43d3-920f-b36efa9bdb7b.png">)


## Process Flow Architecture
![Airflow SLA Process Flow Architecture](https://github.com/teamclairvoyant/airflow-maintenance-dags/issues/128#issuecomment-1251536693) 

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
Once Airflow is up and running succesfully on port 8080, the input DAG in the form of a .py file can be processed as per schedule. If no schedule is set, then the report can be generated manually from the https://localhost:8080 Airflow Web UI interface. To ensure this, the file must be placed in the appropriate directory which is accessed by the Airflow code.

- Navigate to the location where Airflow is installed.
- airflow -> dags -> *place .py file here*

Follow the Airflow UI to send the report to the e-mail specified in airflow-sla-miss-report.py. Note that the email will be sourced from the mail ID that is specified during the airflow setup. Refer to official Airfdlow documentation for complete setup of Airflow to run the project.

### References
        
- https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html#using-pypi
- https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html
- https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html
- https://airflow.apache.org/docs/apache-airflow/1.10.10/scheduler.html
- https://betterdatascience.com/apache-airflow-install/