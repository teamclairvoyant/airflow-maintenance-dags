
import pandas as pd
import json

from dataclasses import dataclass

from datetime import date, timedelta   

from sqlalchemy import create_engine
from sqlalchemy import Boolean, Column, ForeignKey, Index, Integer, String, Text, func, not_, or_
from sqlalchemy.orm import backref, joinedload, relationship
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.session import Session

from airflow import settings, utils
from airflow.models import ( 
    DAG,
    XCOM_RETURN_KEY,
    Base,
    BaseOperator,
    BaseOperatorLink,
    Connection,
    DagBag,
    DagModel,
    DagPickle,
    DagRun,
    DagTag,
    Log,
    Pool,
    SkipMixin,
    SlaMiss,
    TaskFail,
    TaskInstance,
    TaskReschedule,
    Variable,
    XCom,
)
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.tasklog import LogTemplate
from airflow.utils import helpers

from airflow.utils.session import NEW_SESSION, create_session, provide_session 
from airflow.version import version

from airflow.operators.python_operator import PythonOperator
import airflow
from pprint import pprint
from yaml import serialize

db_connection_str = 'mysql+pymysql://airflow_user:airflow_pass@localhost/airflow_db'
db_connection = create_engine(db_connection_str)

#df = pd.read_sql('SELECT * FROM serialized_dag limit 5', con=db_connection)

session = settings.Session()

slamiss = session.query(SlaMiss.task_id,SlaMiss.dag_id,SlaMiss.execution_date).all()
slamiss_df= pd.DataFrame(slamiss)  
slamiss_df['run_date'] = pd.to_datetime(slamiss_df['execution_date']).dt.date
slamiss_df['run_date_hour'] = pd.to_datetime(slamiss_df['execution_date']).dt.hour

taskinstance = session.query(TaskInstance.task_id,TaskInstance.dag_id,TaskInstance.start_date,TaskInstance.end_date,TaskInstance.duration,TaskInstance.state,TaskInstance.operator).all()
taskinstance_df = pd.DataFrame(taskinstance) 
taskinstance_df['run_date'] = pd.to_datetime(taskinstance_df['start_date']).dt.date  
taskinstance_df['run_date_hour'] = pd.to_datetime(taskinstance_df['start_date']).dt.hour  

serializeddag = session.query(SerializedDagModel._data).all()
serializeddag_dfjson = pd.DataFrame(serializeddag)
df2=serializeddag_dfjson['_data'].apply(json.dumps)
df3=df2.apply(json.loads)
df4=pd.DataFrame(df3.values.tolist())['dag']
serializeddag_df = pd.json_normalize(df4, "tasks", ["_dag_id"])

def duration_stats(slamiss_df,taskinstance_df):
    run_detail = pd.merge(slamiss_df, taskinstance_df, on=["task_id", "dag_id","run_date"])
    col_names =  ['min_duration', 'avg_duration', 'max_duration']
    duration_summary  = pd.DataFrame(columns = col_names)
    duration_summary.loc[len(duration_summary)] = [run_detail['duration'].min(),run_detail['duration'].mean(),run_detail['duration'].max()]
    return duration_summary

def missed_sla_trend(slamiss_df,taskinstance_df):
    today = date.today()
    week_prior =  today - timedelta(weeks=1)
    run_detail = pd.merge(slamiss_df, taskinstance_df, on=["task_id", "dag_id","run_date"])
    rundetail_last_week = run_detail[run_detail['run_date'] >= week_prior]
    sla_trend_pastweek=rundetail_last_week.groupby('run_date').size().to_frame('failed_instances').reset_index().sort_values('run_date')
    return sla_trend_pastweek

def dag_violators(slamiss_df,taskinstance_df):
    today = date.today()
    day_prior =  today - timedelta(days=1)
    run_detail = pd.merge(slamiss_df, taskinstance_df, on=["task_id", "dag_id","run_date"])
    rundetail_past_day= run_detail[run_detail['run_date'] >= day_prior]
    dag_violators_pastday=rundetail_past_day.groupby(['dag_id','task_id']).size().to_frame('failed_instances').reset_index().sort_values(by = ['dag_id'], ascending = [True])
    dag_violators_pastday=dag_violators_pastday.sort_values(by='failed_instances',ascending=False)
    return dag_violators_pastday


def missed_sla_hourly_trend(slamiss_df,taskinstance_df):
    today = date.today()
    day_prior =  today - timedelta(days=1)
    run_detail = pd.merge(slamiss_df, taskinstance_df, on=["task_id", "dag_id","run_date","run_date_hour"])
    rundetail_past_day= run_detail[run_detail['run_date'] >= day_prior]
    missed_sla_pastday_hourly=rundetail_past_day.groupby(['run_date_hour']).size().to_frame('failed_instances').reset_index()
    return missed_sla_pastday_hourly


today = date.today()
day_prior =  today - timedelta(days=30)
run_detail = pd.merge(slamiss_df, taskinstance_df, on=["task_id", "dag_id","run_date"])
rundetail_past_day = run_detail[run_detail['run_date'] >= day_prior]
failed_operator_pastday=rundetail_past_day.groupby('operator').size().to_frame('failed_instances').reset_index()
print(failed_operator_pastday[:5])

#missed_sla_pastday=dag_violators_pastday.sort_values(by='failed_instances',ascending=False)
#run_detail.info()

#duration_summary = pd.DataFrame(run_detail['duration'].min(),run_detail['duration'].mean(),run_detail['duration'].max())
#duration_summary=pd.DataFrame()

#print(duration_summary[:10])
#taskinstance_df.info()
#result = pd.merge(slamiss_df, taskinstance_df, on=["task_id", "dag_id","run_date"])
#result = pd.merge(slamiss_df,taskinstance_df,how='inner',left_on=['task_id','dag_id'],)

#print(serializeddag_df[["_dag_id","task_id","sla"]])
#print(serializeddag_df[:10])