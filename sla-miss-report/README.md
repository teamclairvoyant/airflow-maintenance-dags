# Airflow SLA Miss Analysis Report
## Table Of Contents
1. [About](##about)
2. [Dependencies](##dependecies)
3. [Setup](##setup)
    - Installation
    - Deployment


## About
The set of expectations of a customer from the service he receives is called the Service Level Agreement, or in short, the SLA. 
In most cases, Apache Airflow helps us achieve this. We are provided with a full picture of the current status, what was successful and what has failed – enabling us to see the data completeness. We can also plan our tasks to check that their output is exactly as expected. If the result is a fail and retry, we can tackle the data correctness expectation as well.

In Airflow, a DAG – or a Directed Acyclic Graph – is a collection of all the tasks you want to run, organized in a way that reflects their relationships and dependencies. In this project, a DAG file is being created that produces a SLA report with usable metrics and custom KPI integrations which DAG creators/Data Engineers can utilize to adjust existing SLAs and manage expectations. It helps us to overcome the common pain points faced while trying to interpret through the current Airflow SLA miss UI. The report is in the form of an email having all the metrics and insights of SLA Misses.

If you have any data in your airflow metadatabase regarding your data pipelines, this DAG can be used to analyze them more effectively. All you have to do is to update the receiver email addresses and timeframe values and save the file in the dags folder under your airflow directory locally. The Airflow will self detect any DAG placed under that folder and run according to the schedule defined within the DAG file. Reference link is provided on how to create DAG's and how to setup the schedule interval within DAG's.

Based on the input timeframes, the DAG sources data from the taskinstance, dagrun, serializeddag and SerializedDagModel in the user's airflow metadatabase and stores it in initial dataframes on top of which all the transformations and mappings have been applied and insights have been generated.

The expected output is an email report with all the metrics. Attached is a sample report. It can be found in the email addresses as specified by the user.
![Airflow SLA miss Email Report Output1](/sla-miss-report/af1.png)

Below is the architecture of the process followed.
![Airflow SLA Process Flow Architecture](/sla-miss-report/af4.png) 

## Dependencies

### Requirements
- OS: Mac OS/Windows/Linux
- VCS : git
- Python: 3.7 and above for optimum results *
- Airflow: v2.3 and above
- Packages:
    - pip : https://pypi.org/project/pip/
    - pip packages :  

                % pip install apache-airflow
                % pip install numpy
                % pip install pandas
                % pip install SQLAlchemy

### References
airflow :
        
- https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html#using-pypi
- https://airflow.apache.org/docs/apache-airflow/stable/installation/installing-from-pypi.html
- https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html
- https://airflow.apache.org/docs/apache-airflow/1.10.10/scheduler.html


*Runs successfully with Python 3

## Setup
The first step is to install **pip** from the link in the requirements. **pip** is the package installer for Python. Next, install the set of packages given above as part of the requirements.
The second step is to install airflow locally and run it locally on port 8080. Here is how to set it up and get it running: 

https://betterdatascience.com/apache-airflow-install/

You can use pip to install packages from the Python Package Index and other indexes. Airflow can be installed from the requirements section. An example for the command looks like this:

    $ pip install "apache-airflow" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.3.4/constraints-3.7.txt"

The installation of pip and Airflow marks the completion of setup to run the project. The steps for installation of the project is as below

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