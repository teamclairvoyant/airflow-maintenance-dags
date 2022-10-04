import numpy as np
import pandas as pd
import json
import os

import airflow
from airflow import settings
from airflow.models import DAG, DagRun, TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.python import PythonOperator
from datetime import date, datetime, timedelta
from airflow.utils.email import send_email

# airflow-clear-missing-dags
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
START_DATE = airflow.utils.dates.days_ago(1)
# How often to Run. @daily - Once a day at Midnight
SCHEDULE_INTERVAL = "@daily"
# Who is listed as the owner of this DAG in the Airflow Web Server
DAG_OWNER_NAME = "operations"
# List of email address to send the SLA report & the email subject
EMAIL_ADDRESS = ["abc@xyz.com", "bcd@xyz.com", "...."]
EMAIL_SUBJECT = f'Airflow SLA Report - {date.today().strftime("%b %d, %Y")}'
# Timeframes to calculate the metrics on in days
SHORT_TIME_FRAME = 1
MEDIUM_TIME_FRAME = 3
LONG_TIME_FRAME = 7

# Setting up a variable to calculate today's date.
dt = date.today()
today = datetime.combine(dt, datetime.min.time())

# Calculating duration intervals between the defined timeframes and today
shorttimeframe_duration = today - timedelta(days=SHORT_TIME_FRAME)
mediumtimeframe_duration = today - timedelta(days=MEDIUM_TIME_FRAME)
longtimeframe_duration = today - timedelta(days=LONG_TIME_FRAME)

pd.options.display.max_columns = None

# Timeframes according to which KPI's will be calculated. Update the timeframes as per the requirement.
## Note: Please make sure the airflow database consists of dag run data and sla misses data for the timeframes entered. If not, the resultant output may not be as expected.


def retrieve_metadata():
    """Retrieve data from taskinstance,dagrun and serialized dag tables to do some processing to create base tables.

    Returns:
        dataframe: Base tables sla_run_detail and serialized_dags_slas for further processing.
    """
    try:
        pd.set_option("display.max_colwidth", None)

        session = settings.Session()
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
        taskinstance_df["task_queue_time"] = (taskinstance_df["start_date"] -
                                              taskinstance_df["queued_dttm"]).dt.total_seconds()
        taskinstance_df = taskinstance_df[taskinstance_df["task_queue_time"] > 0]

        dagrun = session.query(DagRun.dag_id, DagRun.run_id, DagRun.data_interval_end).all()
        dagrun_df = pd.DataFrame(dagrun)
        dagrun_df.rename(columns={"data_interval_end": "actual_start_time"}, inplace=True)

        if "_data" in dir(SerializedDagModel):
            serializeddag = session.query(SerializedDagModel._data).all()
            data_col = "_data"
        else:
            serializeddag = session.query(SerializedDagModel.data).all()
            data_col = "data"

        serializeddag_df = pd.DataFrame(serializeddag)
        serializeddag_json_normalize = pd.json_normalize(
            pd.DataFrame(serializeddag_df[data_col].apply(json.dumps).apply(json.loads).values.tolist())["dag"],
            "tasks", ["_dag_id"])
        serializeddag_filtered = serializeddag_json_normalize[["_dag_id", "task_id", "sla"]]
        serializeddag_filtered.rename(columns={"_dag_id": "dag_id"}, inplace=True)
        serialized_dags_slas = serializeddag_filtered[serializeddag_filtered["sla"].notnull()]

        run_detail = pd.merge(
            dagrun_df[["dag_id", "run_id", "actual_start_time"]],
            taskinstance_df[[
                "task_id",
                "dag_id",
                "run_id",
                "start_date",
                "end_date",
                "duration",
                "task_queue_time",
                "state",
            ]],
            on=["run_id", "dag_id"],
        )

        sla_run = pd.merge(run_detail, serialized_dags_slas, on=["task_id", "dag_id"])
        sla_run_detail = sla_run.loc[sla_run["sla"].isnull() == False]
        sla_run_detail["sla_missed"] = np.where(sla_run_detail["duration"] > sla_run_detail["sla"], 1, 0)
        sla_run_detail["run_date_hour"] = pd.to_datetime(sla_run_detail["start_date"]).dt.hour
        #       sla_run_detail["start_dt"] = sla_run_detail["start_date"].dt.date
        sla_run_detail["start_dt"] = sla_run_detail["start_date"].dt.strftime("%A, %b %d")
        sla_run_detail["start_date"] = pd.to_datetime(sla_run_detail["start_date"]).dt.tz_localize(None)

        return sla_run_detail, serialized_dags_slas

    except:
        no_metadata_found()


def sla_miss_count_func_timeframe(input_df, timeframe):
    """Group the data based on dagid and taskid and calculate its count and avg duration

    Args:
        input_df (dataframe): sla_run_detail base table
        timeframe (integer): Timeframes entered by the user according to which KPI's will be calculated

    Returns:
        dataframes: Intermediate output dataframes required for further processing of data
    """
    df1 = input_df[input_df["duration"] > input_df["sla"]][input_df["start_date"].between(timeframe, today)]
    df2 = df1.groupby(["dag_id", "task_id"]).size().to_frame(name="size").reset_index()
    df3 = df1.groupby(["dag_id", "task_id"])["duration"].mean().reset_index()
    return df2, df3


def observation_slapercent_func_timeframe(input_df1, input_df2):
    """Calculate SLA miss %

    Args:
        input_df1 (dataframe): dataframe consisting of filtered records as per duration and sla misses grouped by DagId and TaskId
        input_df2 (dataframe): dataframe consisting of all the records as per duration and sla misses grouped by DagId and TaskId

    Returns:
        String containing the SLA miss %
    """

    slapct = (np.nan_to_num(
        ((input_df1["size"].sum() * 100) / (input_df2["total_count"].sum())),
        0,
    ).round(2))
    return slapct


def sla_totalcount_func_timeframe(input_df):
    """Group the data based on dagid and taskid and calculate its count

    Args:
        input_df (dataframe): base sla run table

    Returns:
        Dataframe containing the total count of sla grouped by dag_id and task_id
    """
    df = (input_df.groupby(["dag_id",
                            "task_id"]).size().to_frame(name="total_count").sort_values("total_count",
                                                                                        ascending=False).reset_index())
    return df


def sla_run_count_func_timeframe(input_df, timeframe):
    """Filters the sla_run_detail dataframe between the current date and the timeframe mentioned

    Args:
        input_df (dataframe): base sla run table

    Returns:
        dataframe:
    """
    tf = input_df[input_df["start_date"].between(timeframe, today)]
    return tf


def sla_daily_miss(sla_run_detail):
    """SLA miss table which gives us details about the date, SLA miss % on that date and top DAG violators for the long timeframe.

    Args:
        sla_run_detail (dataframe): Table consiting of details of all the dag runs that happened

    Returns:
        dataframe: sla_daily_miss output dataframe
    """
    try:

        sla_pastweek_run_count_df = sla_run_detail[sla_run_detail["start_date"].between(longtimeframe_duration, today)]

        daily_sla_miss_count = sla_run_detail[sla_run_detail["duration"] > sla_run_detail["sla"]][
            sla_run_detail["start_date"].between(longtimeframe_duration, today)].sort_values(["start_date"])

        daily_sla_miss_count_datewise = (daily_sla_miss_count.groupby(
            ["start_dt"]).size().to_frame(name="slamiss_count_datewise").reset_index())
        daily_sla_count_df = (daily_sla_miss_count.groupby(["start_dt", "dag_id",
                                                            "task_id"]).size().to_frame(name="size").reset_index())
        daily_sla_totalcount_datewise = (sla_pastweek_run_count_df.groupby(
            ["start_dt"]).size().to_frame(name="total_count").sort_values("start_dt", ascending=False).reset_index())
        daily_sla_totalcount_datewise_taskwise = (sla_pastweek_run_count_df.groupby(
            ["start_dt", "dag_id",
             "task_id"]).size().to_frame(name="totalcount").sort_values("start_dt", ascending=False).reset_index())
        daily_sla_miss_pct_df = pd.merge(daily_sla_miss_count_datewise, daily_sla_totalcount_datewise, on=["start_dt"])
        daily_sla_miss_pct_df["sla_miss_percent"] = (daily_sla_miss_pct_df["slamiss_count_datewise"] * 100 /
                                                     daily_sla_miss_pct_df["total_count"]).round(2)
        daily_sla_miss_pct_df["sla_miss_percent(missed_tasks/total_tasks)"] = daily_sla_miss_pct_df.apply(
            lambda x: "%s%s(%s/%s)" % (x["sla_miss_percent"], "% ", x["slamiss_count_datewise"], x["total_count"]),
            axis=1,
        )

        daily_sla_miss_percent = daily_sla_miss_pct_df.filter(
            ["start_dt", "sla_miss_percent(missed_tasks/total_tasks)"], axis=1)
        daily_sla_miss_df_pct1 = pd.merge(
            daily_sla_count_df,
            daily_sla_totalcount_datewise_taskwise,
            on=["start_dt", "dag_id", "task_id"],
        )
        daily_sla_miss_df_pct1["pct_violator"] = (daily_sla_miss_df_pct1["size"] * 100 /
                                                  daily_sla_miss_df_pct1["totalcount"]).round(2)
        daily_sla_miss_df_pct_kpi = (daily_sla_miss_df_pct1.sort_values("pct_violator",
                                                                        ascending=False).groupby("start_dt",
                                                                                                 sort=False).head(1))

        daily_sla_miss_df_pct_kpi["top_pct_violator"] = daily_sla_miss_df_pct_kpi.apply(
            lambda x: "%s: %s (%s%s" % (x["dag_id"], x["task_id"], x["pct_violator"], "%)"),
            axis=1,
        )

        daily_slamiss_percent_violator = daily_sla_miss_df_pct_kpi.filter(["start_dt", "top_pct_violator"], axis=1)
        daily_slamiss_df_absolute_kpi = (daily_sla_miss_df_pct1.sort_values("size", ascending=False).groupby(
            "start_dt", sort=False).head(1))

        daily_slamiss_df_absolute_kpi["top_absolute_violator"] = daily_slamiss_df_absolute_kpi.apply(
            lambda x: "%s: %s (%s/%s)" % (x["dag_id"], x["task_id"], x["size"], x["totalcount"]),
            axis=1,
        )

        daily_slamiss_absolute_violator = daily_slamiss_df_absolute_kpi.filter(["start_dt", "top_absolute_violator"],
                                                                               axis=1)
        daily_slamiss_pct_last7days = pd.merge(
            pd.merge(daily_sla_miss_percent, daily_slamiss_percent_violator, on="start_dt"),
            daily_slamiss_absolute_violator,
            on="start_dt",
        ).sort_values("start_dt", ascending=False)

        daily_slamiss_pct_last7days.rename(
            columns={
                "top_pct_violator": "Top Violator (%)",
                "top_absolute_violator": "Top Violator (absolute)",
                "start_dt": "Date",
                "sla_miss_percent(missed_tasks/total_tasks)": "SLA Miss % (Missed/Total Tasks)",
            },
            inplace=True,
        )
        return daily_slamiss_pct_last7days
    except:
        daily_slamiss_pct_last7days = pd.DataFrame(
            columns=["Date", "SLA Miss % (Missed/Total Tasks)", "Top Violator (%)", "Top Violator (absolute)"])
        return daily_slamiss_pct_last7days


def sla_hourly_miss(sla_run_detail):
    """Generate hourly SLA miss table giving us details about the hour, SLA miss % for that hour, top DAG violators
    and the longest running task and avg task queue time for the given short timeframe.

    Args:
        sla_run_detail (dataframe): Base table consiting of details of all the dag runs that happened

    Returns:
        datframe, list: observations_hourly_reccomendations list and  sla_miss_percent_past_day_hourly dataframe
    """
    try:

        sla_miss_count_past_day = sla_run_detail[sla_run_detail["duration"] > sla_run_detail["sla"]][
            sla_run_detail["start_date"].between(shorttimeframe_duration, today)]

        sla_miss_count_hourly = (sla_miss_count_past_day.groupby(
            ["run_date_hour"]).size().to_frame(name="slamiss_count_hourwise").reset_index())
        sla_count_df_past_day_hourly = (sla_miss_count_past_day.groupby(["run_date_hour", "dag_id", "task_id"
                                                                         ]).size().to_frame(name="size").reset_index())
        sla_avg_execution_time_taskwise_hourly = (sla_miss_count_past_day.groupby(
            ["run_date_hour", "dag_id", "task_id"])["duration"].mean().reset_index())
        sla_avg_execution_time_hourly = (sla_avg_execution_time_taskwise_hourly.sort_values(
            "duration", ascending=False).groupby("run_date_hour", sort=False).head(1))

        sla_pastday_run_count_df = sla_run_detail[sla_run_detail["start_date"].between(shorttimeframe_duration, today)]
        sla_avg_queue_time_hourly = (sla_pastday_run_count_df.groupby(["run_date_hour"
                                                                       ])["task_queue_time"].mean().reset_index())
        sla_totalcount_hourly = (sla_pastday_run_count_df.groupby(
            ["run_date_hour"]).size().to_frame(name="total_count").sort_values("run_date_hour",
                                                                               ascending=False).reset_index())
        sla_totalcount_taskwise_hourly = (sla_pastday_run_count_df.groupby(
            ["run_date_hour", "dag_id",
             "task_id"]).size().to_frame(name="totalcount").sort_values("run_date_hour", ascending=False).reset_index())
        sla_miss_pct_past_day_hourly = pd.merge(sla_miss_count_hourly, sla_totalcount_hourly, on=["run_date_hour"])
        sla_miss_pct_past_day_hourly["sla_miss_percent"] = (sla_miss_pct_past_day_hourly["slamiss_count_hourwise"] *
                                                            100 / sla_miss_pct_past_day_hourly["total_count"]).round(2)

        sla_miss_pct_past_day_hourly["sla_miss_percent(missed_tasks/total_tasks)"] = sla_miss_pct_past_day_hourly.apply(
            lambda x: "%s%s(%s/%s)" % (
                x["sla_miss_percent"].astype(int),
                "% ",
                x["slamiss_count_hourwise"].astype(int),
                x["total_count"].astype(int),
            ),
            axis=1,
        )

        sla_highest_sla_miss_hour = (sla_miss_pct_past_day_hourly[["run_date_hour", "sla_miss_percent"
                                                                   ]].sort_values("sla_miss_percent",
                                                                                  ascending=False).head(1))
        sla_highest_tasks_hour = (sla_miss_pct_past_day_hourly[["run_date_hour",
                                                                "total_count"]].sort_values("total_count",
                                                                                            ascending=False).head(1))

        sla_miss_percent_past_day = sla_miss_pct_past_day_hourly.filter(
            ["run_date_hour", "sla_miss_percent(missed_tasks/total_tasks)"], axis=1)

        sla_miss_temp_df_pct1_past_day = pd.merge(
            sla_count_df_past_day_hourly,
            sla_totalcount_taskwise_hourly,
            on=["run_date_hour", "dag_id", "task_id"],
        )

        sla_miss_temp_df_pct1_past_day["pct_violator"] = (sla_miss_temp_df_pct1_past_day["size"] * 100 /
                                                          sla_miss_temp_df_pct1_past_day["totalcount"]).round(2)
        sla_miss_pct_past_day_hourly = (sla_miss_temp_df_pct1_past_day.sort_values(
            "pct_violator", ascending=False).groupby("run_date_hour", sort=False).head(1))

        sla_miss_pct_past_day_hourly["top_pct_violator"] = sla_miss_pct_past_day_hourly.apply(
            lambda x: "%s: %s (%s%s" % (x["dag_id"], x["task_id"], x["pct_violator"], "%)"),
            axis=1,
        )

        sla_miss_percent_violator_past_day_hourly = sla_miss_pct_past_day_hourly.filter(
            ["run_date_hour", "top_pct_violator"], axis=1)
        sla_miss_absolute_kpi_past_day_hourly = (sla_miss_temp_df_pct1_past_day.sort_values(
            "size", ascending=False).groupby("run_date_hour", sort=False).head(1))
        sla_miss_absolute_kpi_past_day_hourly["top_absolute_violator"] = sla_miss_absolute_kpi_past_day_hourly.apply(
            lambda x: "%s: %s (%s/%s)" % (x["dag_id"], x["task_id"], x["size"], x["totalcount"]),
            axis=1,
        )

        sla_miss_absolute_violator_past_day_hourly = sla_miss_absolute_kpi_past_day_hourly.filter(
            ["run_date_hour", "top_absolute_violator"], axis=1)
        slamiss_pct_exectime = pd.merge(
            pd.merge(
                sla_miss_percent_past_day,
                sla_miss_percent_violator_past_day_hourly,
                on="run_date_hour",
            ),
            sla_miss_absolute_violator_past_day_hourly,
            on="run_date_hour",
        ).sort_values("run_date_hour", ascending=False)

        sla_avg_execution_time_hourly["duration"] = (
            sla_avg_execution_time_hourly["duration"].round(0).astype(int).astype(str))
        sla_avg_execution_time_hourly["longest_running_task"] = sla_avg_execution_time_hourly.apply(
            lambda x: "%s: %s (%s s)" % (x["dag_id"], x["task_id"], x["duration"]), axis=1)

        sla_longest_running_task_hourly = sla_avg_execution_time_hourly.filter(
            ["run_date_hour", "longest_running_task"], axis=1)

        sla_miss_pct = pd.merge(slamiss_pct_exectime, sla_longest_running_task_hourly, on=["run_date_hour"])
        sla_miss_percent_past_day_hourly = pd.merge(sla_miss_pct, sla_avg_queue_time_hourly, on=["run_date_hour"])
        sla_miss_percent_past_day_hourly["task_queue_time"] = (
            sla_miss_percent_past_day_hourly["task_queue_time"].round(0).astype(int).apply(str))
        sla_longest_queue_time_hourly = (sla_miss_percent_past_day_hourly[["run_date_hour", "task_queue_time"
                                                                           ]].sort_values("task_queue_time",
                                                                                          ascending=False).head(1))

        sla_miss_percent_past_day_hourly.rename(
            columns={
                "task_queue_time": "Average Task Queue Time (seconds)",
                "longest_running_task": "Longest Running Task",
                "top_pct_violator": "Top Violator (%)",
                "top_absolute_violator": "Top Violator (absolute)",
                "run_date_hour": "Hour",
                "sla_miss_percent(missed_tasks/total_tasks)": "SLA miss % (Missed/Total Tasks)",
            },
            inplace=True,
        )

        obs1_hourlytrend = "Hour " + (sla_highest_sla_miss_hour["run_date_hour"].apply(str) +
                            " had the highest percentage of sla misses").to_string(index=False)
        obs2_hourlytrend = "Hour " + (sla_longest_queue_time_hourly["run_date_hour"].apply(str) +
                            " had the longest average queue time (" +
                            sla_longest_queue_time_hourly["task_queue_time"].apply(str) +
                            " seconds)").to_string(index=False)
        obs3_hourlytrend = "Hour " + (sla_highest_tasks_hour["run_date_hour"].apply(str) +
                            " had the most tasks running").to_string(index=False)

        observations_hourly_reccomendations = [obs1_hourlytrend, obs2_hourlytrend, obs3_hourlytrend]
        return observations_hourly_reccomendations, sla_miss_percent_past_day_hourly
    except:
        sla_miss_percent_past_day_hourly = pd.DataFrame(columns=[
            "SLA miss % (Missed/Total Tasks)",
            "Top Violator (%)",
            "Top Violator (absolute)",
            "Longest Running Task",
            "Hour",
            "Average Task Queue Time (seconds)",
        ])
        observations_hourly_reccomendations = ""
        return observations_hourly_reccomendations, sla_miss_percent_past_day_hourly


def sla_dag_miss(sla_run_detail, serialized_dags_slas):
    """
    Generate SLA dag miss table giving us details about the SLA miss % for the given timeframes along with the average execution time and
    reccomendations for weekly observations.

    Args:
        sla_run_detail (dataframe): Base table consiting of details of all the dag runs that happened
        serialized_dags_slas (dataframe): table consisting of all the dag details

    Returns:
        2 lists consisting of sla_daily_miss and sla_dag_miss reccomendations and 1 dataframe consisting of sla_dag_miss reccomendation
    """
    try:

        dag_sla_count_df_weekprior, dag_sla_count_df_weekprior_avgduration = sla_miss_count_func_timeframe(
            sla_run_detail, longtimeframe_duration)
        dag_sla_count_df_threedayprior, dag_sla_count_df_threedayprior_avgduration = sla_miss_count_func_timeframe(
            sla_run_detail, mediumtimeframe_duration)
        dag_sla_count_df_onedayprior, dag_sla_count_df_onedayprior_avgduration = sla_miss_count_func_timeframe(
            sla_run_detail, shorttimeframe_duration)

        dag_sla_run_count_week_prior = sla_run_count_func_timeframe(sla_run_detail, longtimeframe_duration)
        dag_sla_run_count_three_day_prior = sla_run_count_func_timeframe(sla_run_detail, mediumtimeframe_duration)
        dag_sla_run_count_one_day_prior = sla_run_count_func_timeframe(sla_run_detail, shorttimeframe_duration)

        dag_sla_run_count_week_prior_success = (
            dag_sla_run_count_week_prior[dag_sla_run_count_week_prior["state"] == "success"].groupby(
                ["dag_id", "task_id"]).size().to_frame(name="success_count").reset_index())
        dag_sla_run_count_week_prior_failure = (
            dag_sla_run_count_week_prior[dag_sla_run_count_week_prior["state"] == "failed"].groupby(
                ["dag_id", "task_id"]).size().to_frame(name="failure_count").reset_index())

        dag_sla_run_count_week_prior_success_duration_stats = (
            dag_sla_run_count_week_prior[dag_sla_run_count_week_prior["state"] == "success"].groupby(
                ["dag_id", "task_id"])["duration"].agg(["mean", "min", "max"]).reset_index())
        dag_sla_run_count_week_prior_failure_duration_stats = (
            dag_sla_run_count_week_prior[dag_sla_run_count_week_prior["state"] == "failed"].groupby(
                ["dag_id", "task_id"])["duration"].agg(["mean", "min", "max"]).reset_index())

        dag_sla_totalcount_week_prior = sla_totalcount_func_timeframe(dag_sla_run_count_week_prior)
        dag_sla_totalcount_three_day_prior = sla_totalcount_func_timeframe(dag_sla_run_count_three_day_prior)
        dag_sla_totalcount_one_day_prior = sla_totalcount_func_timeframe(dag_sla_run_count_one_day_prior)

        dag_obs5_sladpercent_weekprior = observation_slapercent_func_timeframe(dag_sla_count_df_weekprior,
                                                                               dag_sla_totalcount_week_prior)
        dag_obs6_sladpercent_threedayprior = observation_slapercent_func_timeframe(dag_sla_count_df_threedayprior,
                                                                                   dag_sla_totalcount_three_day_prior)
        dag_obs7_sladpercent_onedayprior = observation_slapercent_func_timeframe(dag_sla_count_df_onedayprior,
                                                                                 dag_sla_totalcount_one_day_prior)

        dag_obs7_sladetailed_week = f'In the past {str(LONG_TIME_FRAME)} days, {dag_obs5_sladpercent_weekprior}% of the tasks have missed their SLA'
        dag_obs6_sladetailed_threeday = f'In the past {str(MEDIUM_TIME_FRAME)} days, {dag_obs6_sladpercent_threedayprior}% of the tasks have missed their SLA'
        dag_obs5_sladetailed_oneday = f'In the past {str(SHORT_TIME_FRAME)} days, {dag_obs7_sladpercent_onedayprior}% of the tasks have missed their SLA'

        dag_sla_miss_pct_df_week_prior = pd.merge(
            pd.merge(dag_sla_count_df_weekprior, dag_sla_totalcount_week_prior, on=["dag_id", "task_id"]),
            dag_sla_count_df_weekprior_avgduration,
            on=["dag_id", "task_id"],
        )
        dag_sla_miss_pct_df_threeday_prior = pd.merge(
            pd.merge(
                dag_sla_count_df_threedayprior,
                dag_sla_totalcount_three_day_prior,
                on=["dag_id", "task_id"],
            ),
            dag_sla_count_df_threedayprior_avgduration,
            on=["dag_id", "task_id"],
        )
        dag_sla_miss_pct_df_oneday_prior = pd.merge(
            pd.merge(
                dag_sla_count_df_onedayprior,
                dag_sla_totalcount_one_day_prior,
                on=["dag_id", "task_id"],
            ),
            dag_sla_count_df_onedayprior_avgduration,
            on=["dag_id", "task_id"],
        )

        dag_sla_miss_pct_df_week_prior["sla_miss_percent_week"] = (
            dag_sla_miss_pct_df_week_prior["size"] * 100 / dag_sla_miss_pct_df_week_prior["total_count"]).round(2)
        dag_sla_miss_pct_df_threeday_prior["sla_miss_percent_three_day"] = (
            dag_sla_miss_pct_df_threeday_prior["size"] * 100 /
            dag_sla_miss_pct_df_threeday_prior["total_count"]).round(2)
        dag_sla_miss_pct_df_oneday_prior["sla_miss_percent_one_day"] = (
            dag_sla_miss_pct_df_oneday_prior["size"] * 100 / dag_sla_miss_pct_df_oneday_prior["total_count"]).round(2)

        dag_sla_miss_pct_df1 = dag_sla_miss_pct_df_week_prior.merge(dag_sla_miss_pct_df_threeday_prior,
                                                                    on=["dag_id", "task_id"],
                                                                    how="left")
        dag_sla_miss_pct_df2 = dag_sla_miss_pct_df1.merge(dag_sla_miss_pct_df_oneday_prior,
                                                          on=["dag_id", "task_id"],
                                                          how="left")
        dag_sla_miss_pct_df3 = dag_sla_miss_pct_df2.merge(serialized_dags_slas, on=["dag_id", "task_id"], how="left")

        dag_sla_miss_pct_detailed = dag_sla_miss_pct_df3.filter(
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

        float_column_names = dag_sla_miss_pct_detailed.select_dtypes(float).columns
        dag_sla_miss_pct_detailed[float_column_names] = dag_sla_miss_pct_detailed[float_column_names].fillna(0)

        round_int_column_names = ["duration_x", "duration_y", "duration"]
        dag_sla_miss_pct_detailed[round_int_column_names] = dag_sla_miss_pct_detailed[round_int_column_names].round(
            0).astype(int)
        dag_sla_miss_pct_detailed["sla"] = dag_sla_miss_pct_detailed["sla"].astype(int)
        dag_sla_miss_pct_detailed["Dag: Task"] = (dag_sla_miss_pct_detailed["dag_id"].apply(str) + ": " +
                                                  dag_sla_miss_pct_detailed["task_id"].apply(str))

        short_time_frame_col_name = f'{SHORT_TIME_FRAME}-Day SLA miss % (avg execution time)'
        medium_time_frame_col_name = f'{MEDIUM_TIME_FRAME}-Day SLA miss % (avg execution time)'
        long_time_frame_col_name = f'{LONG_TIME_FRAME}-Day SLA miss % (avg execution time)'

        dag_sla_miss_pct_detailed[short_time_frame_col_name] = (
            dag_sla_miss_pct_detailed["sla_miss_percent_one_day"].apply(str) + "% (" +
            dag_sla_miss_pct_detailed["duration"].apply(str) + " s)")

        dag_sla_miss_pct_detailed[medium_time_frame_col_name] = (
            dag_sla_miss_pct_detailed["sla_miss_percent_three_day"].apply(str) + "% (" +
            dag_sla_miss_pct_detailed["duration_y"].apply(str) + " s)")

        dag_sla_miss_pct_detailed[long_time_frame_col_name] = (
            dag_sla_miss_pct_detailed["sla_miss_percent_week"].apply(str) + "% (" +
            dag_sla_miss_pct_detailed["duration_x"].apply(str) + " s)")

        dag_sla_miss_pct_filtered = dag_sla_miss_pct_detailed.filter(
            [
                "Dag: Task",
                "sla",
                short_time_frame_col_name,
                medium_time_frame_col_name,
                long_time_frame_col_name,
            ],
            axis=1,
        ).sort_values(by=[long_time_frame_col_name], ascending=False)

        dag_sla_miss_pct_filtered.rename(columns={"sla": "Current SLA"}, inplace=True)

        dag_sla_miss_pct_recc1 = dag_sla_miss_pct_detailed.nlargest(3, ["sla_miss_percent_week"]).fillna(0)
        dag_sla_miss_pct_recc2 = dag_sla_miss_pct_recc1.filter(
            ["dag_id", "task_id", "sla", "sla_miss_percent_week", "Dag: Task"], axis=1).fillna(0)
        dag_sla_miss_pct_df4_recc3 = pd.merge(
            pd.merge(
                dag_sla_miss_pct_recc2,
                dag_sla_run_count_week_prior_success,
                on=["dag_id", "task_id"],
            ),
            dag_sla_run_count_week_prior_failure,
            on=["dag_id", "task_id"],
            how="left",
        ).fillna(0)
        dag_sla_miss_pct_df4_recc4 = pd.merge(
            pd.merge(
                dag_sla_miss_pct_df4_recc3,
                dag_sla_run_count_week_prior_success_duration_stats,
                on=["dag_id", "task_id"],
                how="left",
            ),
            dag_sla_run_count_week_prior_failure_duration_stats,
            on=["dag_id", "task_id"],
            how="left",
        ).fillna(0)
        dag_sla_miss_pct_df4_recc4["Recommendations"] = (
            dag_sla_miss_pct_df4_recc4["Dag: Task"].apply(str) + " - Of the " +
            dag_sla_miss_pct_df4_recc4["sla_miss_percent_week"].apply(str) +
            "% of the tasks that missed their SLA of " + dag_sla_miss_pct_df4_recc4["sla"].apply(str) + " seconds, " +
            dag_sla_miss_pct_df4_recc4["success_count"].apply(str) + " succeeded (min: " +
            dag_sla_miss_pct_df4_recc4["min_x"].round(0).astype(int).apply(str) + " s, avg: " +
            dag_sla_miss_pct_df4_recc4["mean_x"].round(0).astype(int).apply(str) + " s, max: " +
            dag_sla_miss_pct_df4_recc4["max_x"].round(0).astype(int).apply(str) + " s) & " +
            dag_sla_miss_pct_df4_recc4["failure_count"].apply(str) + " failed (min: " +
            dag_sla_miss_pct_df4_recc4["min_y"].round(0).astype(int).apply(str) + " s, avg: " +
            dag_sla_miss_pct_df4_recc4["mean_y"].round(0).astype(int).apply(str) + " s, max: " +
            dag_sla_miss_pct_df4_recc4["max_y"].round(0).fillna(0).astype(int).apply(str) + " s)")

        daily_weeklytrend_observations_loop = [
            dag_obs5_sladetailed_oneday,
            dag_obs6_sladetailed_threeday,
            dag_obs7_sladetailed_week,
        ]

        dag_sla_miss_trend = dag_sla_miss_pct_df4_recc4["Recommendations"].tolist()

        return daily_weeklytrend_observations_loop, dag_sla_miss_trend, dag_sla_miss_pct_filtered
    except:
        short_time_frame_col_name = f'{SHORT_TIME_FRAME}-Day SLA miss % (avg execution time)'
        medium_time_frame_col_name = f'{MEDIUM_TIME_FRAME}-Day SLA miss % (avg execution time)'
        long_time_frame_col_name = f'{LONG_TIME_FRAME}-Day SLA miss % (avg execution time)'
        daily_weeklytrend_observations_loop = ""
        dag_sla_miss_trend = ""
        dag_sla_miss_pct_filtered = pd.DataFrame(columns=[
            "Dag: Task",
            "Current SLA",
            short_time_frame_col_name,
            medium_time_frame_col_name,
            long_time_frame_col_name,
        ])
        return daily_weeklytrend_observations_loop, dag_sla_miss_trend, dag_sla_miss_pct_filtered


def sla_miss_report():
    """Embed all the resultant output datframes within html format and send the email report to the intented recipients."""

    sla_run_detail, serialized_dags_slas = retrieve_metadata()
    daily_slamiss_pct_last7days = sla_daily_miss(sla_run_detail)
    observations_hourly_reccomendations, sla_miss_percent_past_day_hourly = sla_hourly_miss(sla_run_detail)
    daily_weeklytrend_observations_loop, dag_sla_miss_trend, dag_sla_miss_pct_filtered = sla_dag_miss(
        sla_run_detail, serialized_dags_slas)

    new_line = '\n'
    print(f"""{new_line}Daily SLA Misses
{new_line.join(map(str, daily_weeklytrend_observations_loop))}
{daily_slamiss_pct_last7days.to_markdown()}
    """)

    print(f"""{new_line}Hourly SLA Misses
{new_line.join(map(str, observations_hourly_reccomendations))}
{sla_miss_percent_past_day_hourly.to_markdown()}
    """)

    print(f"""{new_line}DAG SLA Misses
{new_line.join(map(str, dag_sla_miss_trend))}
{dag_sla_miss_pct_filtered.to_markdown()}
    """)

    daily_weeklytrend_observations_loop = "".join([f"<li>{item}</li>" for item in daily_weeklytrend_observations_loop])
    observations_hourly_reccomendations = "".join([f"<li>{item}</li>" for item in observations_hourly_reccomendations])
    dag_sla_miss_trend = "".join([f"<li>{item}</li>" for item in dag_sla_miss_trend])

    short_time_frame_print = f'<b>Short</b>: {SHORT_TIME_FRAME}d ({shorttimeframe_duration.strftime("%b %d")} - {(today - timedelta(days=1)).strftime("%b %d")})'
    medium_time_frame_print = f'<b>Medium</b>: {MEDIUM_TIME_FRAME}d ({mediumtimeframe_duration.strftime("%b %d")} - {(today - timedelta(days=1)).strftime("%b %d")})'
    long_time_frame_print = f'<b>Long</b>: {LONG_TIME_FRAME}d ({longtimeframe_duration.strftime("%b %d")} - {(today - timedelta(days=1)).strftime("%b %d")})'
    time_frame_prints = f'{short_time_frame_print} | {medium_time_frame_print} | {long_time_frame_print}'

    html_content1 = f"""\
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
    text-align: left;
    background-color: #154360;
    color: white;
    }}

    td {{
    text-align: left;
    background-color: #EBF5FB;
    }}
    </style>
    </head>
    <body>
    The following timeframes are used to generate this report. To change them, update the [SHORT, MEDIUM, LONG]_TIME_FRAME variables in airflow-sla-miss-report.py.
    <br></br><br></br>
    {time_frame_prints}

    <h2>Daily SLA Misses</h2>
    <p>Daily breakdown of SLA misses and the <b>worst offenders</b> over the past {LONG_TIME_FRAME} days.</p>
    {daily_weeklytrend_observations_loop}
    {daily_slamiss_pct_last7days.to_html(index=False)}


    <h2>Hourly SLA Misses</h2>
    <p>Hourly breakdown of tasks missing their SLAs and the worst offenders over the past {SHORT_TIME_FRAME} days. Useful for identifying <b>scheduling bottlenecks</b>.</p>
    {observations_hourly_reccomendations}
    {sla_miss_percent_past_day_hourly.to_html(index=False)}

    <h2>DAG SLA Misses</h2>
    <p>Task level breakdown showcasing the SLA miss percentage & average exectution time over the past {SHORT_TIME_FRAME}, {MEDIUM_TIME_FRAME}, and {LONG_TIME_FRAME} days. Useful for <b>identifying trends and updating defined SLAs</b> to meet actual exectution times.</p>
    {dag_sla_miss_trend}
    {dag_sla_miss_pct_filtered.to_html(index=False)}

    </body>
    </html>
    """

    send_email(to=EMAIL_ADDRESS, subject=EMAIL_SUBJECT, html_content=html_content1)


def no_metadata_found():
    """Stock html email template to send if there is no data present in the base tables"""

    short_time_frame_print = f'Short: {SHORT_TIME_FRAME}d ({shorttimeframe_duration.strftime("%b %d")} - {(today - timedelta(days=1)).strftime("%b %d")})'
    medium_time_frame_print = f'Medium: {MEDIUM_TIME_FRAME}d ({mediumtimeframe_duration.strftime("%b %d")} - {(today - timedelta(days=1)).strftime("%b %d")})'
    long_time_frame_print = f'Long: {LONG_TIME_FRAME}d ({longtimeframe_duration.strftime("%b %d")} - {(today - timedelta(days=1)).strftime("%b %d")})'
    time_frame_prints = f'{short_time_frame_print} | {medium_time_frame_print} | {long_time_frame_print}'

    html_content = f"""\
    <html>
    <head>
    </head>
    <body>
    The following timeframes are used to generate this report. To change them, update the [SHORT/MEDIUM/LONG]_TIME_FRAME variables in airflow-sla-miss-report.py.
    {time_frame_prints}
    <h2 style="color:red"><u>No Data Available</u></h2>
    <p><b>Please make sure the respective DAG run data is avaialble in the airflow metadata database.</b></p>
    </body>
    </html>
    """
    send_email(to=EMAIL_ADDRESS, subject=EMAIL_SUBJECT, html_content=html_content)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": EMAIL_ADDRESS,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
        DAG_ID,
        default_args=default_args,
        description="DAG generating the SLA miss email report",
        schedule_interval=SCHEDULE_INTERVAL,
        start_date=START_DATE,
        tags=['teamclairvoyant', 'airflow-maintenance-dags']
) as dag:
    run_this = PythonOperator(task_id="sla_miss_report", python_callable=sla_miss_report, dag=dag)
