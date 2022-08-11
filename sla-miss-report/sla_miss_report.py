import airflow
import numpy as np
import pandas as pd
import json

from airflow import settings, utils
from airflow.models import DAG, DagRun, SlaMiss, TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.email import EmailOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, datetime, timedelta

EMAIL_ADDRESSES_FROM = ["nikhil.manjunatha@clairvoyantsoft.com"]
EMAIL_ADDRESSES_TO = ["nikhil.manjunatha@clairvoyantsoft.com"]

session = settings.Session()

slamiss = session.query(
    SlaMiss.task_id, SlaMiss.dag_id, SlaMiss.execution_date, SlaMiss.timestamp
).all()
slamiss_df = pd.DataFrame(slamiss)
slamiss_df["run_date"] = pd.to_datetime(slamiss_df["execution_date"]).dt.date
slamiss_df["run_date_hour"] = pd.to_datetime(slamiss_df["execution_date"]).dt.hour

taskinstance = session.query(
    TaskInstance.task_id,
    TaskInstance.dag_id,
    TaskInstance.run_id,
    TaskInstance.state,
    TaskInstance.start_date,
    TaskInstance.end_date,
    TaskInstance.duration,
    TaskInstance.operator,
    TaskInstance.queued_dttm,
).all()
taskinstance_df = pd.DataFrame(taskinstance)
taskinstance_df["run_date"] = pd.to_datetime(taskinstance_df["start_date"]).dt.date
taskinstance_df["run_date_hour"] = pd.to_datetime(taskinstance_df["start_date"]).dt.hour
taskinstance_df["task_queue_time"] = (
    taskinstance_df["start_date"] - taskinstance_df["queued_dttm"]
).dt.total_seconds()
taskinstance_df = taskinstance_df[taskinstance_df["task_queue_time"] > 0]

dagrun = session.query(DagRun.dag_id, DagRun.run_id, DagRun.data_interval_end).all()
dagrun_df = pd.DataFrame(dagrun)
dagrun_df.rename(columns={"data_interval_end": "actual_start_time"}, inplace=True)


if '_data' in dir(SerializedDagModel):
    serializeddag = session.query(SerializedDagModel._data).all()
else:
    serializeddag = session.query(SerializedDagModel.data).all()

serializeddag_dfjson = pd.DataFrame(serializeddag)
df2 = serializeddag_dfjson["_data"].apply(json.dumps)
df3 = df2.apply(json.loads)
df4 = pd.DataFrame(df3.values.tolist())["dag"]
sdag = pd.json_normalize(df4, "tasks", ["_dag_id"])
serializeddag_df = sdag[["_dag_id", "task_id", "sla"]]
serializeddag_df.rename(columns={"_dag_id": "dag_id"}, inplace=True)
serializeddag_df_filtered = serializeddag_df[serializeddag_df["sla"].notnull()]


run_detail = pd.merge(
    dagrun_df[["dag_id", "run_id", "actual_start_time"]],
    taskinstance_df[
        [
            "task_id",
            "dag_id",
            "run_id",
            "start_date",
            "end_date",
            "duration",
            "task_queue_time",
            "state",
        ]
    ],
    on=["run_id", "dag_id"],
)
sla_miss_run_details = pd.merge(
    run_detail[
        [
            "dag_id",
            "run_id",
            "actual_start_time",
            "task_id",
            "start_date",
            "end_date",
            "duration",
            "task_queue_time",
            "state",
        ]
    ],
    slamiss_df[["dag_id", "task_id", "execution_date", "timestamp"]],
    how="inner",
    left_on=["dag_id", "task_id", "actual_start_time"],
    right_on=["dag_id", "task_id", "execution_date"],
)
sla_run = pd.merge(run_detail, serializeddag_df_filtered, on=["task_id", "dag_id"])
sla_run_detail = sla_run.loc[sla_run["sla"].isnull() == False]
sla_run_detail["sla_missed"] = np.where(
    sla_run_detail["duration"] > sla_run_detail["sla"], 1, 0
)
sla_run_detail["run_date_hour"] = pd.to_datetime(sla_run_detail["start_date"]).dt.hour
sla_run_detail["start_dt"] = sla_run_detail["start_date"].dt.date
sla_run_detail["start_date"] = pd.to_datetime(
    sla_run_detail["start_date"]
).dt.tz_localize(None)


dt = date.today()
today = datetime.combine(dt, datetime.min.time())
list_days=[1,3,7]

# SLA Miss Percent Past one day

week_prior = today - timedelta(days = list_days[2], hours=0, minutes=0)

sla_miss_count = sla_run_detail[sla_run_detail["duration"] > sla_run_detail["sla"]][
    sla_run_detail["start_date"].between(week_prior, today)
].sort_values(["start_date"])

sla_miss_count_datewise = (
    sla_miss_count.groupby(["start_dt"])
    .size()
    .to_frame(name="slamiss_count_datewise")
    .reset_index()
)
sla_count_df = (
    sla_miss_count.groupby(["start_dt", "dag_id", "task_id"])
    .size()
    .to_frame(name="size")
    .reset_index()
)
sla_missed = (
    sla_count_df.sort_values("size", ascending=False)
    .groupby("start_dt", sort=False)
    .head(1)
)
sla_pastweek_run_count_df = sla_run_detail[sla_run_detail["start_date"] > week_prior]
sla_totalcount_datewise = (
    sla_pastweek_run_count_df.groupby(["start_dt"])
    .size()
    .to_frame(name="total_count")
    .sort_values("start_dt", ascending=False)
    .reset_index()
)
sla_totalcount_datewise_taskwise = (
    sla_pastweek_run_count_df.groupby(["start_dt", "dag_id", "task_id"])
    .size()
    .to_frame(name="totalcount")
    .sort_values("start_dt", ascending=False)
    .reset_index()
)
sla_miss_pct_df = pd.merge(
    sla_miss_count_datewise, sla_totalcount_datewise, on=["start_dt"]
)
sla_miss_pct_df["sla_miss_percent"] = (
    sla_miss_pct_df["slamiss_count_datewise"] * 100 / sla_miss_pct_df["total_count"]
).round(2)
sla_miss_pct_df["sla_miss_percent(missed_tasks/total_tasks)"] = (
    sla_miss_pct_df["sla_miss_percent"].apply(str)
    + "% ("
    + sla_miss_pct_df["slamiss_count_datewise"].apply(str)
    + "/"
    + sla_miss_pct_df["total_count"].apply(str)
    + ")"
)
sla_miss_percent = sla_miss_pct_df.filter(
    ["start_dt", "sla_miss_percent(missed_tasks/total_tasks)"], axis=1
)
sla_miss_temp_df_absolute = pd.merge(
    sla_missed, sla_totalcount_datewise_taskwise, on=["start_dt", "dag_id", "task_id"]
)
sla_miss_temp_df_pct1 = pd.merge(
    sla_count_df, sla_totalcount_datewise_taskwise, on=["start_dt", "dag_id", "task_id"]
)
sla_miss_temp_df_pct1["pct_violator"] = (
    sla_miss_temp_df_pct1["size"] * 100 / sla_miss_temp_df_pct1["totalcount"]
).round(2)
sla_miss_temp_df_pct_kpi = (
    sla_miss_temp_df_pct1.sort_values("pct_violator", ascending=False)
    .groupby("start_dt", sort=False)
    .head(1)
)
sla_miss_temp_df_pct_kpi["top_pct_violator"] = (
    sla_miss_temp_df_pct_kpi["dag_id"].apply(str)
    + ","
    + sla_miss_temp_df_pct_kpi["task_id"].apply(str)
    + " ("
    + sla_miss_temp_df_pct_kpi["pct_violator"].apply(str)
    + "%)"
)
slamiss_percent_violator = sla_miss_temp_df_pct_kpi.filter(
    ["start_dt", "top_pct_violator"], axis=1
)
sla_miss_temp_df_absolute_kpi = (
    sla_miss_temp_df_pct1.sort_values("size", ascending=False)
    .groupby("start_dt", sort=False)
    .head(1)
)
sla_miss_temp_df_absolute_kpi["top_absolute_violator"] = (
    sla_miss_temp_df_absolute_kpi["dag_id"].apply(str)
    + ": "
    + sla_miss_temp_df_absolute_kpi["task_id"].apply(str)
    + " ("
    + sla_miss_temp_df_absolute_kpi["size"].apply(str)
    + "/"
    + sla_miss_temp_df_absolute_kpi["totalcount"].apply(str)
    + ")"
)
slamiss_absolute_violator = sla_miss_temp_df_absolute_kpi.filter(
    ["start_dt", "top_absolute_violator"], axis=1
)
slamiss_pct_last7days = pd.merge(
    pd.merge(sla_miss_percent, slamiss_percent_violator, on="start_dt"),
    slamiss_absolute_violator,
    on="start_dt",
).sort_values("start_dt", ascending=False)

slamiss_pct_last7days.rename(
    columns={
        "top_pct_violator": "top violator (%)",
        "top_absolute_violator": "top violator (absolute)",
        "start_dt": "Date",
        "sla_miss_percent(missed_tasks/total_tasks)": "SLA Miss % (missed_tasks/total_tasks)",
    },
    inplace=True,
)


# Hourly Trend

day_prior = today - timedelta(days=list_days[0], hours=0, minutes=0)

sla_miss_count_past_day = sla_run_detail[
    sla_run_detail["duration"] > sla_run_detail["sla"]
][sla_run_detail["start_date"].between(day_prior, today)]

sla_miss_count_hourwise = (
    sla_miss_count_past_day.groupby(["run_date_hour"])
    .size()
    .to_frame(name="slamiss_count_hourwise")
    .reset_index()
)
sla_count_df_past_day = (
    sla_miss_count_past_day.groupby(["run_date_hour", "dag_id", "task_id"])
    .size()
    .to_frame(name="size")
    .reset_index()
)
sla_avg_execution_time_taskwise = (
    sla_miss_count_past_day.groupby(["run_date_hour", "dag_id", "task_id"])["duration"]
    .mean()
    .reset_index()
)
sla_avg_execution_time_hourly = (
    sla_avg_execution_time_taskwise.sort_values("duration", ascending=False)
    .groupby("run_date_hour", sort=False)
    .head(1)
)
sla_missed_past_day = (
    sla_count_df_past_day.sort_values("size", ascending=False)
    .groupby("run_date_hour", sort=False)
    .head(1)
)
sla_pastday_run_count_df = sla_run_detail[
    sla_run_detail["start_date"].between(day_prior, today)
]
sla_avg_queue_time_hourly = (
    sla_pastday_run_count_df.groupby(["run_date_hour"])["task_queue_time"]
    .mean()
    .reset_index()
)
sla_totalcount_hourwise = (
    sla_pastday_run_count_df.groupby(["run_date_hour"])
    .size()
    .to_frame(name="total_count")
    .sort_values("run_date_hour", ascending=False)
    .reset_index()
)
sla_totalcount_hourwise_taskwise = (
    sla_pastday_run_count_df.groupby(["run_date_hour", "dag_id", "task_id"])
    .size()
    .to_frame(name="totalcount")
    .sort_values("run_date_hour", ascending=False)
    .reset_index()
)
sla_miss_pct_df_past_day = pd.merge(
    sla_miss_count_hourwise, sla_totalcount_hourwise, on=["run_date_hour"]
)
sla_miss_pct_df_past_day["sla_miss_percent"] = (
    sla_miss_pct_df_past_day["slamiss_count_hourwise"]
    * 100
    / sla_miss_pct_df_past_day["total_count"]
).round(2)
sla_miss_pct_df_past_day[
    "sla_miss_percent(missed_tasks/total_tasks)"
] = sla_miss_pct_df_past_day["sla_miss_percent"].apply(
    str
) 

sla_highest_sla_miss_hour = (
    sla_miss_pct_df_past_day[["run_date_hour", "sla_miss_percent"]]
    .sort_values("sla_miss_percent", ascending=False)
    .head(1)
)
sla_highest_tasks_hour = (
    sla_miss_pct_df_past_day[["run_date_hour", "total_count"]]
    .sort_values("total_count", ascending=False)
    .head(1)
)


sla_miss_percent_past_day = sla_miss_pct_df_past_day.filter(
    ["run_date_hour", "sla_miss_percent(missed_tasks/total_tasks)"], axis=1
)

sla_miss_temp_df_pct1_past_day = pd.merge(
    sla_count_df_past_day,
    sla_totalcount_hourwise_taskwise,
    on=["run_date_hour", "dag_id", "task_id"],
)
sla_miss_temp_df_pct1_past_day["pct_violator"] = (
    sla_miss_temp_df_pct1_past_day["size"]
    * 100
    / sla_miss_temp_df_pct1_past_day["totalcount"]
).round(2)
sla_miss_temp_df_pct_kpi_past_day = (
    sla_miss_temp_df_pct1_past_day.sort_values("pct_violator", ascending=False)
    .groupby("run_date_hour", sort=False)
    .head(1)
)
sla_miss_temp_df_pct_kpi_past_day["top_pct_violator"] = (
    sla_miss_temp_df_pct_kpi_past_day["dag_id"].apply(str)
    + ": "
    + sla_miss_temp_df_pct_kpi_past_day["task_id"].apply(str)
    + " ("
    + sla_miss_temp_df_pct_kpi_past_day["pct_violator"].apply(str)
    + "%)"
)
slamiss_percent_violator_past_day = sla_miss_temp_df_pct_kpi_past_day.filter(
    ["run_date_hour", "top_pct_violator"], axis=1
)
sla_miss_temp_df_absolute_kpi_past_day = (
    sla_miss_temp_df_pct1_past_day.sort_values("size", ascending=False)
    .groupby("run_date_hour", sort=False)
    .head(1)
)
sla_miss_temp_df_absolute_kpi_past_day["top_absolute_violator"] = (
    sla_miss_temp_df_absolute_kpi_past_day["dag_id"].apply(str)
    + ": "
    + sla_miss_temp_df_absolute_kpi_past_day["task_id"].apply(str)
    + " ("
    + sla_miss_temp_df_absolute_kpi_past_day["size"].apply(str)
    + "/"
    + sla_miss_temp_df_absolute_kpi_past_day["totalcount"].apply(str)
    + ")"
)
slamiss_absolute_violator_past_day = sla_miss_temp_df_absolute_kpi_past_day.filter(
    ["run_date_hour", "top_absolute_violator"], axis=1
)
slamiss_pct_exectime = pd.merge(
    pd.merge(
        sla_miss_percent_past_day, slamiss_percent_violator_past_day, on="run_date_hour"
    ),
    slamiss_absolute_violator_past_day,
    on="run_date_hour",
).sort_values("run_date_hour", ascending=False)


sla_avg_execution_time_hourly["longest_running_task"] = (
    sla_avg_execution_time_hourly["dag_id"].apply(str)
    + ": "
    + sla_avg_execution_time_hourly["task_id"].apply(str)
    + " ("
    + sla_avg_execution_time_hourly["duration"].round(0).astype(int).apply(str)
    + " s)"
)
sla_longest_running_task = sla_avg_execution_time_hourly.filter(
    ["run_date_hour", "longest_running_task"], axis=1
)

sla_miss_pct = pd.merge(
    slamiss_pct_exectime, sla_longest_running_task, on=["run_date_hour"]
)
sla_miss_percent_past_day_hourly = pd.merge(
    sla_miss_pct, sla_avg_queue_time_hourly, on=["run_date_hour"]
)
sla_miss_percent_past_day_hourly["task_queue_time"] = (
    sla_miss_percent_past_day_hourly["task_queue_time"].round(0).astype(int).apply(str)
)
sla_longest_queue_time_hour = (
    sla_miss_percent_past_day_hourly[["run_date_hour", "task_queue_time"]]
    .sort_values("task_queue_time", ascending=False)
    .head(1)
)

sla_miss_percent_past_day_hourly.rename(
    columns={
        "task_queue_time": "avg task queue time (seconds)",
        "longest_running_task": "longest running task",
        "top_pct_violator": "top violator (%)",
        "top_absolute_violator": "top violator (absolute)",
        "run_date_hour": "Hour",
        "sla_miss_percent(missed_tasks/total_tasks)": "SLA Miss % (missed_tasks/total_tasks)",
    },
    inplace=True,
)

obs1_hourlytrend = (
    sla_highest_sla_miss_hour["run_date_hour"].apply(str)
    + " - hour had the highest percentage sla misses"
)
obs2_hourlytrend = (
    sla_longest_queue_time_hour["run_date_hour"].apply(str)
    + " - hour had the longest average queue time ( "
    + sla_longest_queue_time_hour["task_queue_time"].apply(str)
    + " seconds)"
)
obs3_hourlytrend = (
    sla_highest_tasks_hour["run_date_hour"].apply(str)
    + " - hour had the most tasks running"
)

# SLA_Detailed

seven_day_prior = today - timedelta(days= list_days[2], hours=0, minutes=0)
three_day_prior = today - timedelta(days= list_days[1], hours=0, minutes=0)
one_day_prior = today - timedelta(days= list_days[0], hours=0, minutes=0)

sla_miss_count_weekprior = sla_run_detail[
    sla_run_detail["duration"] > sla_run_detail["sla"]
][sla_run_detail["start_date"].between(seven_day_prior, today)]
sla_count_df_weekprior = (
    sla_miss_count_weekprior.groupby(["dag_id", "task_id"])
    .size()
    .to_frame(name="size")
    .reset_index()
)
sla_count_df_weekprior_avgduration = (
    sla_miss_count_weekprior.groupby(["dag_id", "task_id"])["duration"]
    .mean()
    .reset_index()
)


sla_miss_count_threedayprior = sla_run_detail[
    sla_run_detail["duration"] > sla_run_detail["sla"]
][sla_run_detail["start_date"].between(three_day_prior, today)]
sla_count_df_threedayprior = (
    sla_miss_count_threedayprior.groupby(["dag_id", "task_id"])
    .size()
    .to_frame(name="size")
    .reset_index()
)
sla_count_df_threedayprior_avgduration = (
    sla_miss_count_threedayprior.groupby(["dag_id", "task_id"])["duration"]
    .mean()
    .reset_index()
)

sla_miss_count_onedayprior = sla_run_detail[
    sla_run_detail["duration"] > sla_run_detail["sla"]
][sla_run_detail["start_date"].between(one_day_prior, today)]
sla_count_df_onedayprior = (
    sla_miss_count_onedayprior.groupby(["dag_id", "task_id"])
    .size()
    .to_frame(name="size")
    .reset_index()
)
sla_count_df_onedayprior_avgduration = (
    sla_miss_count_onedayprior.groupby(["dag_id", "task_id"])["duration"]
    .mean()
    .reset_index()
)


sla_run_count_week_prior = sla_run_detail[
    sla_run_detail["start_date"].between(seven_day_prior, today)
]
sla_run_count_three_day_prior = sla_run_detail[
    sla_run_detail["start_date"].between(three_day_prior, today)
]
sla_run_count_one_day_prior = sla_run_detail[
    sla_run_detail["start_date"].between(one_day_prior, today)
]


sla_run_count_week_prior_success = (
    sla_run_count_week_prior[sla_run_count_week_prior["state"] == "success"]
    .groupby(["dag_id", "task_id"])
    .size()
    .to_frame(name="success_count")
    .reset_index()
)
sla_run_count_week_prior_failure = (
    sla_run_count_week_prior[sla_run_count_week_prior["state"] == "failed"]
    .groupby(["dag_id", "task_id"])
    .size()
    .to_frame(name="failure_count")
    .reset_index()
)
sla_run_count_week_prior_success_duration_stats = (
    sla_run_count_week_prior[sla_run_count_week_prior["state"] == "success"]
    .groupby(["dag_id", "task_id"])["duration"]
    .agg(["mean", "min", "max"])
    .reset_index()
)
sla_run_count_week_prior_failure_duration_stats = (
    sla_run_count_week_prior[sla_run_count_week_prior["state"] == "failed"]
    .groupby(["dag_id", "task_id"])["duration"]
    .agg(["mean", "min", "max"])
    .reset_index()
)


sla_totalcount_week_prior = (
    sla_run_count_week_prior.groupby(["dag_id", "task_id"])
    .size()
    .to_frame(name="total_count")
    .sort_values("total_count", ascending=False)
    .reset_index()
)
sla_totalcount_three_day_prior = (
    sla_run_count_three_day_prior.groupby(["dag_id", "task_id"])
    .size()
    .to_frame(name="total_count")
    .sort_values("total_count", ascending=False)
    .reset_index()
)
sla_totalcount_one_day_prior = (
    sla_run_count_one_day_prior.groupby(["dag_id", "task_id"])
    .size()
    .to_frame(name="total_count")
    .sort_values("total_count", ascending=False)
    .reset_index()
)


obs5_sladpercent_weekprior = (
    np.nan_to_num(
        (
            (sla_count_df_weekprior["size"].sum() * 100)
            / (sla_totalcount_week_prior["total_count"].sum())
        ),
        0,
    )
    .round(2)
    .astype("str")
)
obs6_sladpercent_threedayprior = (
    np.nan_to_num(
        (
            (sla_count_df_threedayprior["size"].sum() * 100)
            / (sla_totalcount_three_day_prior["total_count"].sum())
        ),
        0,
    )
    .round(2)
    .astype("str")
)
obs7_sladpercent_onedayprior = (
    np.nan_to_num(
        (
            (sla_count_df_onedayprior["size"].sum() * 100)
            / (sla_totalcount_one_day_prior["total_count"].sum())
        ),
        0,
    )
    .round(2)
    .astype("str")
)


obs5_sladetailed_oneday = (
    "In the past 1 day, "
    + obs7_sladpercent_onedayprior
    + "% tasks have missed their SLA"
)
obs6_sladetailed_threeday = (
    "In the past 3 days, "
    + obs6_sladpercent_threedayprior
    + "% tasks have missed their SLA"
)
obs7_sladetailed_week = (
    "In the past 7 days, "
    + obs5_sladpercent_weekprior
    + "% tasks have missed their SLA"
)

sla_miss_pct_df_week_prior = pd.merge(
    pd.merge(
        sla_count_df_weekprior, sla_totalcount_week_prior, on=["dag_id", "task_id"]
    ),
    sla_count_df_weekprior_avgduration,
    on=["dag_id", "task_id"],
)
sla_miss_pct_df_threeday_prior = pd.merge(
    pd.merge(
        sla_count_df_threedayprior,
        sla_totalcount_three_day_prior,
        on=["dag_id", "task_id"],
    ),
    sla_count_df_threedayprior_avgduration,
    on=["dag_id", "task_id"],
)
sla_miss_pct_df_oneday_prior = pd.merge(
    pd.merge(
        sla_count_df_onedayprior, sla_totalcount_one_day_prior, on=["dag_id", "task_id"]
    ),
    sla_count_df_onedayprior_avgduration,
    on=["dag_id", "task_id"],
)

sla_miss_pct_df_week_prior["sla_miss_percent_week"] = (
    sla_miss_pct_df_week_prior["size"] * 100 / sla_miss_pct_df_week_prior["total_count"]
).round(2)
sla_miss_pct_df_threeday_prior["sla_miss_percent_three_day"] = (
    sla_miss_pct_df_threeday_prior["size"]
    * 100
    / sla_miss_pct_df_threeday_prior["total_count"]
).round(2)
sla_miss_pct_df_oneday_prior["sla_miss_percent_one_day"] = (
    sla_miss_pct_df_oneday_prior["size"]
    * 100
    / sla_miss_pct_df_oneday_prior["total_count"]
).round(2)


sla_miss_pct_df1 = sla_miss_pct_df_week_prior.merge(
    sla_miss_pct_df_threeday_prior, on=["dag_id", "task_id"], how="left"
)
sla_miss_pct_df2 = sla_miss_pct_df1.merge(
    sla_miss_pct_df_oneday_prior, on=["dag_id", "task_id"], how="left"
)
sla_miss_pct_df3 = sla_miss_pct_df2.merge(
    serializeddag_df_filtered, on=["dag_id", "task_id"], how="left"
)
sla_miss_pct_df4 = sla_miss_pct_df3.filter(
    [
        "dag_id",
        "task_id",
        "sla",
        "sla_miss_percent_week",
        "duration_x",
        "sla_miss_percent_three_day",
        "duration_y",
        "sla_miss_percent_one_day",
        "duration",
    ],
    axis=1,
)
sla_miss_pct_df4["duration_x"] = (
    sla_miss_pct_df4["duration_x"].fillna(0).round(0).astype(int)
)
sla_miss_pct_df4["duration_y"] = (
    sla_miss_pct_df4["duration_y"].fillna(0).round(0).astype(int)
)
sla_miss_pct_df4["duration"] = (
    sla_miss_pct_df4["duration"].fillna(0).round(0).astype(int)
)
sla_miss_pct_df4["sla"] = sla_miss_pct_df4["sla"].astype(int)
sla_miss_pct_df4["sla_miss_percent_week"] = sla_miss_pct_df4[
    "sla_miss_percent_week"
].fillna(0)
sla_miss_pct_df4["sla_miss_percent_three_day"] = sla_miss_pct_df4[
    "sla_miss_percent_three_day"
].fillna(0)
sla_miss_pct_df4["sla_miss_percent_one_day"] = sla_miss_pct_df4[
    "sla_miss_percent_one_day"
].fillna(0)

sla_miss_pct_df4["Dag: Task"] = (
    sla_miss_pct_df4["dag_id"].apply(str)
    + ": "
    + sla_miss_pct_df4["task_id"].apply(str)
)
sla_miss_pct_df4["1 day SLA Miss % (avg execution time)"] = (
    sla_miss_pct_df4["sla_miss_percent_one_day"].apply(str)
    + "% ("
    + sla_miss_pct_df4["duration"].apply(str)
    + " s)"
)
sla_miss_pct_df4["3 day SLA Miss % (avg execution time)"] = (
    sla_miss_pct_df4["sla_miss_percent_three_day"].apply(str)
    + "% ("
    + sla_miss_pct_df4["duration_y"].apply(str)
    + " s)"
)
sla_miss_pct_df4["7 day SLA Miss % (avg execution time)"] = (
    sla_miss_pct_df4["sla_miss_percent_week"].apply(str)
    + "% ("
    + sla_miss_pct_df4["duration_x"].apply(str)
    + " s)"
)

sla_miss_pct_df5 = sla_miss_pct_df4.filter(
    [
        "Dag: Task",
        "sla",
        "1 day SLA Miss % (avg execution time)",
        "3 day SLA Miss % (avg execution time)",
        "7 day SLA Miss % (avg execution time)",
    ],
    axis=1,
)
sla_miss_pct_df5.rename(columns={"sla": "Current SLA"}, inplace=True)



sla_miss_pct_df4_temp_recc1 = sla_miss_pct_df4.nlargest(
    3, ["sla_miss_percent_week"]
).fillna(0)

sla_miss_pct_df4_temp_recc2 = sla_miss_pct_df4_temp_recc1.filter(
    ["dag_id", "task_id", "sla", "sla_miss_percent_week", "Dag: Task"], axis=1
).fillna(0)
sla_miss_pct_df4_temp_recc3 = pd.merge(
    pd.merge(
        sla_miss_pct_df4_temp_recc2,
        sla_run_count_week_prior_success,
        on=["dag_id", "task_id"],
    ),
    sla_run_count_week_prior_failure,
    on=["dag_id", "task_id"],
).fillna(0)
sla_miss_pct_df4_temp_recc4 = pd.merge(
    pd.merge(
        sla_miss_pct_df4_temp_recc3,
        sla_run_count_week_prior_success_duration_stats,
        on=["dag_id", "task_id"],
        how="left",
    ),
    sla_run_count_week_prior_failure_duration_stats,
    on=["dag_id", "task_id"],
    how="left",
).fillna(0)

sla_miss_pct_df4_temp_recc4["Recommendations"] = (
    sla_miss_pct_df4_temp_recc4["Dag: Task"].apply(str)
    + ":- Of the "
    + sla_miss_pct_df4_temp_recc4["sla_miss_percent_week"].apply(str)
    + "% of the tasks that missed their SLA of "
    + sla_miss_pct_df4_temp_recc4["sla"].apply(str)
    + " seconds, "
    + sla_miss_pct_df4_temp_recc4["success_count"].apply(str)
    + " succeeded (min: "
    + sla_miss_pct_df4_temp_recc4["min_x"].round(0).astype(int).apply(str)
    + " s, avg: "
    + sla_miss_pct_df4_temp_recc4["mean_x"].round(0).astype(int).apply(str)
    + " s, max: "
    + sla_miss_pct_df4_temp_recc4["max_x"].round(0).astype(int).apply(str)
    + " s) & "
    + sla_miss_pct_df4_temp_recc4["failure_count"].apply(str)
    + " failed (min: "
    + sla_miss_pct_df4_temp_recc4["min_y"].round(0).astype(int).apply(str)
    + " s, avg: "
    + sla_miss_pct_df4_temp_recc4["mean_y"].round(0).astype(int).apply(str)
    + " s, max: "
    + sla_miss_pct_df4_temp_recc4["max_y"].round(0).fillna(0).astype(int).apply(str)
    + " s)"
)
pd.set_option("display.max_colwidth", None)

obs4_sladetailed = sla_miss_pct_df4_temp_recc4["Recommendations"].tolist()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": EMAIL_ADDRESSES_FROM,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "demo_sla_miss",
    default_args=default_args,
    description="A simple email ",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    send_email_notification = EmailOperator(
        task_id="send_test_email",
        to=EMAIL_ADDRESSES_TO,
        subject="Airflow SLA Metrics Report - 08/04/2022",
        html_content=f"""\
        <html>
        <head>
        <style>
        table {{
        font-family: Arial, Helvetica, sans-serif;
        border-collapse: collapse;
        width: 100%;
        }}

        td, th {{
        border: 1px solid #ddd;
        padding: 8px;
        }}

        th {{
        padding-top: 12px;
        padding-bottom: 12px;
        text-align: right;
        background-color: #154360;
        color: white;
        }}

        td {{
        text-align: right;
        background-color: #EBF5FB;
        }}
        </style>
        </head>
        <body>
        <h2><u>SLA Miss Details and Top Dag:Task Violators for the past 7 days</u></h2>
        <p>This metric gives us the details for SLA Miss % for the past 7 days. Also, it tells us the task which has missed it's SLA benchmark the most 
        in terms of the absolute number and %</p> 
        <h4>Observations</h4>
        <ul>
            <li>{obs5_sladetailed_oneday}</li>
            <li>{obs6_sladetailed_threeday}</li>
            <li>{obs7_sladetailed_week}</li>
        </ul>
        {slamiss_pct_last7days.to_html(index=False)}  
    
        <h2><u>Average Hourly Trend SLA Miss Metrics for the past 7 days</u></h2>
        <p>This metric gives us the hourly trend for SLA Miss % for the past 7 days. Also, it tells us the task which has missed it's SLA benchmark the most 
        in terms of the absolute number and %. Along with this, it tells us which task took the longest time to run and the average task queue time for that 
        particular hour</p>
        <h4>Observations</h4>
        <ul>
            <li>{obs1_hourlytrend.item()}</li>
            <li>{obs2_hourlytrend.item()}</li>
            <li>{obs3_hourlytrend.item()}</li>
        </ul>
        
        {sla_miss_percent_past_day_hourly.to_html(index=False)}  
        <h2><u>Detailed view of all DAGs and its performance metrics</u></h2>
        <p>This metric gives us a detailed view of all the tasks and it's SLA Miss % with it's average execution time over the past 1 day, 3 day and 7 days. This can
        help in identifying if there has been an improvement in the processing time after a possible optimization in code and to observe the consistency. </p>
        
        <h4>Observations</h4>
        {{% for item in {obs4_sladetailed} %}}
            <li>{{{{ item }}}}</li>
        {{% endfor %}}
        {sla_miss_pct_df5.to_html(index=False)}  
        </body>
        </html> """,
    )
