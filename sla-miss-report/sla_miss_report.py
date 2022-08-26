# All the neccesary imports required for the execution of the code
import numpy as np
import pandas as pd
import json

from airflow import settings
from airflow.models import DAG, DagRun, TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.python import PythonOperator
from datetime import date, datetime, timedelta
from airflow.utils.email import send_email

# List of Receiver Email Addresses.
EMAIL_ADDRESS = ["abc@xyz.com", "bcd@xyz.com", "...."]

# Setting up a variable to calculate today's date.
dt = date.today()
today = datetime.combine(dt, datetime.min.time())

# Timeframes according to which KPI's will be calculated. Update the timeframes as per the requirement.
## Note: Please make sure the airflow database consists of dag run data and sla misses data for the timeframes entered. If not, the resultant output may not be as expected.
short_time_frame = 20
medium_time_frame = 30
long_time_frame = 40


def initial():
    """Function to retrieve data from taskinstance,dagrun and serialized dag table and store it in dataframes.
    Setting up base tables daily_sla_miss_count, sla_run_detail, sla_pastweek_run_count_df, serializeddag_notnull to use it further"""
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
        taskinstance_df["task_queue_time"] = (
            taskinstance_df["start_date"] - taskinstance_df["queued_dttm"]
        ).dt.total_seconds()
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
        serializeddag_json = serializeddag_df["_data"].apply(json.dumps).apply(json.loads)
        serializeddag_json_list = pd.DataFrame(serializeddag_json.values.tolist())["dag"]
        serializeddag_json_normalize = pd.json_normalize(serializeddag_json_list, "tasks", ["_dag_id"])
        serializeddag_filtered = serializeddag_json_normalize[["_dag_id", "task_id", "sla"]]
        serializeddag_filtered.rename(columns={"_dag_id": "dag_id"}, inplace=True)
        serializeddag_notnull = serializeddag_filtered[serializeddag_filtered["sla"].notnull()]

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

        sla_run = pd.merge(run_detail, serializeddag_notnull, on=["task_id", "dag_id"])
        sla_run_detail = sla_run.loc[sla_run["sla"].isnull() == False]
        sla_run_detail["sla_missed"] = np.where(sla_run_detail["duration"] > sla_run_detail["sla"], 1, 0)
        sla_run_detail["run_date_hour"] = pd.to_datetime(sla_run_detail["start_date"]).dt.hour
        #       sla_run_detail["start_dt"] = sla_run_detail["start_date"].dt.date
        sla_run_detail["start_dt"] = sla_run_detail["start_date"].dt.strftime("%A, %b %d")
        sla_run_detail["start_date"] = pd.to_datetime(sla_run_detail["start_date"]).dt.tz_localize(None)

        return sla_run_detail, serializeddag_notnull

    except:
        no_data_print()


def sla_miss_count_func_timeframe(input_df, timeframe):
    """
    Function to filter records which have missed their SLA and are within the given timeframe
    """
    df1 = input_df[input_df["duration"] > input_df["sla"]][input_df["start_date"].between(timeframe, today)]
    df2 = df1.groupby(["dag_id", "task_id"]).size().to_frame(name="size").reset_index()
    df3 = df1.groupby(["dag_id", "task_id"])["duration"].mean().reset_index()
    return df2, df3


def observation_slapercent_func_timeframe(input_df1, input_df2):
    """
    Function to calculate SLA miss %
    """

    df = (
        np.nan_to_num(
            ((input_df1["size"].sum() * 100) / (input_df2["total_count"].sum())),
            0,
        )
        .round(2)
        .astype("str")
    )
    return df


def sla_totalcount_func_timeframe(input_df):

    df = (
        input_df.groupby(["dag_id", "task_id"])
        .size()
        .to_frame(name="total_count")
        .sort_values("total_count", ascending=False)
        .reset_index()
    )
    return df


def sla_daily_miss(sla_run_detail):
    """
    Function to generate SLA miss table giving us details about the date, SLA miss % on that date and top DAG violators for the long timeframe.
    """
    try:

        week_prior = today - timedelta(days=long_time_frame, hours=0, minutes=0)

        sla_pastweek_run_count_df = sla_run_detail[sla_run_detail["start_date"].between(week_prior, today)]

        daily_sla_miss_count = sla_run_detail[sla_run_detail["duration"] > sla_run_detail["sla"]][
            sla_run_detail["start_date"].between(week_prior, today)
        ].sort_values(["start_date"])

        daily_sla_miss_count_datewise = (
            daily_sla_miss_count.groupby(["start_dt"]).size().to_frame(name="slamiss_count_datewise").reset_index()
        )
        daily_sla_count_df = (
            daily_sla_miss_count.groupby(["start_dt", "dag_id", "task_id"]).size().to_frame(name="size").reset_index()
        )
        daily_sla_totalcount_datewise = (
            sla_pastweek_run_count_df.groupby(["start_dt"])
            .size()
            .to_frame(name="total_count")
            .sort_values("start_dt", ascending=False)
            .reset_index()
        )
        daily_sla_totalcount_datewise_taskwise = (
            sla_pastweek_run_count_df.groupby(["start_dt", "dag_id", "task_id"])
            .size()
            .to_frame(name="totalcount")
            .sort_values("start_dt", ascending=False)
            .reset_index()
        )
        daily_sla_miss_pct_df = pd.merge(daily_sla_miss_count_datewise, daily_sla_totalcount_datewise, on=["start_dt"])
        daily_sla_miss_pct_df["sla_miss_percent"] = (
            daily_sla_miss_pct_df["slamiss_count_datewise"] * 100 / daily_sla_miss_pct_df["total_count"]
        ).round(2)
        daily_sla_miss_pct_df["sla_miss_percent(missed_tasks/total_tasks)"] = daily_sla_miss_pct_df.apply(
            lambda x: "%s%s(%s/%s)" % (x["sla_miss_percent"], "% ", x["slamiss_count_datewise"], x["total_count"]),
            axis=1,
        )

        daily_sla_miss_percent = daily_sla_miss_pct_df.filter(
            ["start_dt", "sla_miss_percent(missed_tasks/total_tasks)"], axis=1
        )
        daily_sla_miss_df_pct1 = pd.merge(
            daily_sla_count_df,
            daily_sla_totalcount_datewise_taskwise,
            on=["start_dt", "dag_id", "task_id"],
        )
        daily_sla_miss_df_pct1["pct_violator"] = (
            daily_sla_miss_df_pct1["size"] * 100 / daily_sla_miss_df_pct1["totalcount"]
        ).round(2)
        daily_sla_miss_df_pct_kpi = (
            daily_sla_miss_df_pct1.sort_values("pct_violator", ascending=False).groupby("start_dt", sort=False).head(1)
        )

        daily_sla_miss_df_pct_kpi["top_pct_violator"] = daily_sla_miss_df_pct_kpi.apply(
            lambda x: "%s: %s (%s%s" % (x["dag_id"], x["task_id"], x["pct_violator"], "%)"),
            axis=1,
        )

        daily_slamiss_percent_violator = daily_sla_miss_df_pct_kpi.filter(["start_dt", "top_pct_violator"], axis=1)
        daily_slamiss_df_absolute_kpi = (
            daily_sla_miss_df_pct1.sort_values("size", ascending=False).groupby("start_dt", sort=False).head(1)
        )

        daily_slamiss_df_absolute_kpi["top_absolute_violator"] = daily_slamiss_df_absolute_kpi.apply(
            lambda x: "%s: %s (%s/%s)" % (x["dag_id"], x["task_id"], x["size"], x["totalcount"]),
            axis=1,
        )

        daily_slamiss_absolute_violator = daily_slamiss_df_absolute_kpi.filter(
            ["start_dt", "top_absolute_violator"], axis=1
        )
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
                "sla_miss_percent(missed_tasks/total_tasks)": "SLA miss % (Missed Tasks/Total Tasks)",
            },
            inplace=True,
        )
        daily_weeklytrend_observations_loop = ""
        return daily_slamiss_pct_last7days, daily_weeklytrend_observations_loop
    except:
        daily_weeklytrend_observations_loop = ""
        daily_slamiss_pct_last7days = pd.DataFrame(
            columns=["Date", "SLA miss % (Missed Tasks/Total Tasks)", "Top Violator (%)", "Top Violator (absolute)"]
        )
        return daily_slamiss_pct_last7days, daily_weeklytrend_observations_loop


def sla_hourly_miss(sla_run_detail):
    """
    Function to generate Hourly SLA miss table giving us details about the hour, SLA miss % for that hour, top DAG violators
    and the longest running task and avg task queue time for the given short timeframe.
    """
    try:
        day_prior = today - timedelta(days=short_time_frame, hours=0, minutes=0)

        sla_miss_count_past_day = sla_run_detail[sla_run_detail["duration"] > sla_run_detail["sla"]][
            sla_run_detail["start_date"].between(day_prior, today)
        ]

        sla_miss_count_hourly = (
            sla_miss_count_past_day.groupby(["run_date_hour"])
            .size()
            .to_frame(name="slamiss_count_hourwise")
            .reset_index()
        )
        sla_count_df_past_day_hourly = (
            sla_miss_count_past_day.groupby(["run_date_hour", "dag_id", "task_id"])
            .size()
            .to_frame(name="size")
            .reset_index()
        )
        sla_avg_execution_time_taskwise_hourly = (
            sla_miss_count_past_day.groupby(["run_date_hour", "dag_id", "task_id"])["duration"].mean().reset_index()
        )
        sla_avg_execution_time_hourly = (
            sla_avg_execution_time_taskwise_hourly.sort_values("duration", ascending=False)
            .groupby("run_date_hour", sort=False)
            .head(1)
        )

        sla_pastday_run_count_df = sla_run_detail[sla_run_detail["start_date"].between(day_prior, today)]
        sla_avg_queue_time_hourly = (
            sla_pastday_run_count_df.groupby(["run_date_hour"])["task_queue_time"].mean().reset_index()
        )
        sla_totalcount_hourly = (
            sla_pastday_run_count_df.groupby(["run_date_hour"])
            .size()
            .to_frame(name="total_count")
            .sort_values("run_date_hour", ascending=False)
            .reset_index()
        )
        sla_totalcount_taskwise_hourly = (
            sla_pastday_run_count_df.groupby(["run_date_hour", "dag_id", "task_id"])
            .size()
            .to_frame(name="totalcount")
            .sort_values("run_date_hour", ascending=False)
            .reset_index()
        )
        sla_miss_pct_past_day_hourly = pd.merge(sla_miss_count_hourly, sla_totalcount_hourly, on=["run_date_hour"])
        sla_miss_pct_past_day_hourly["sla_miss_percent"] = (
            sla_miss_pct_past_day_hourly["slamiss_count_hourwise"] * 100 / sla_miss_pct_past_day_hourly["total_count"]
        ).round(2)

        sla_miss_pct_past_day_hourly["sla_miss_percent(missed_tasks/total_tasks)"] = sla_miss_pct_past_day_hourly.apply(
            lambda x: "%s%s(%s/%s)"
            % (
                x["sla_miss_percent"].astype(int),
                "% ",
                x["slamiss_count_hourwise"].astype(int),
                x["total_count"].astype(int),
            ),
            axis=1,
        )

        sla_highest_sla_miss_hour = (
            sla_miss_pct_past_day_hourly[["run_date_hour", "sla_miss_percent"]]
            .sort_values("sla_miss_percent", ascending=False)
            .head(1)
        )
        sla_highest_tasks_hour = (
            sla_miss_pct_past_day_hourly[["run_date_hour", "total_count"]]
            .sort_values("total_count", ascending=False)
            .head(1)
        )

        sla_miss_percent_past_day = sla_miss_pct_past_day_hourly.filter(
            ["run_date_hour", "sla_miss_percent(missed_tasks/total_tasks)"], axis=1
        )

        sla_miss_temp_df_pct1_past_day = pd.merge(
            sla_count_df_past_day_hourly,
            sla_totalcount_taskwise_hourly,
            on=["run_date_hour", "dag_id", "task_id"],
        )

        sla_miss_temp_df_pct1_past_day["pct_violator"] = (
            sla_miss_temp_df_pct1_past_day["size"] * 100 / sla_miss_temp_df_pct1_past_day["totalcount"]
        ).round(2)
        sla_miss_pct_past_day_hourly = (
            sla_miss_temp_df_pct1_past_day.sort_values("pct_violator", ascending=False)
            .groupby("run_date_hour", sort=False)
            .head(1)
        )

        sla_miss_pct_past_day_hourly["top_pct_violator"] = sla_miss_pct_past_day_hourly.apply(
            lambda x: "%s: %s (%s%s" % (x["dag_id"], x["task_id"], x["pct_violator"], "%)"),
            axis=1,
        )

        sla_miss_percent_violator_past_day_hourly = sla_miss_pct_past_day_hourly.filter(
            ["run_date_hour", "top_pct_violator"], axis=1
        )
        sla_miss_absolute_kpi_past_day_hourly = (
            sla_miss_temp_df_pct1_past_day.sort_values("size", ascending=False)
            .groupby("run_date_hour", sort=False)
            .head(1)
        )
        sla_miss_absolute_kpi_past_day_hourly["top_absolute_violator"] = sla_miss_absolute_kpi_past_day_hourly.apply(
            lambda x: "%s: %s (%s/%s)" % (x["dag_id"], x["task_id"], x["size"], x["totalcount"]),
            axis=1,
        )

        sla_miss_absolute_violator_past_day_hourly = sla_miss_absolute_kpi_past_day_hourly.filter(
            ["run_date_hour", "top_absolute_violator"], axis=1
        )
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
            sla_avg_execution_time_hourly["duration"].round(0).astype(int).astype(str)
        )
        sla_avg_execution_time_hourly["longest_running_task"] = sla_avg_execution_time_hourly.apply(
            lambda x: "%s: %s (%s s)" % (x["dag_id"], x["task_id"], x["duration"]), axis=1
        )

        sla_longest_running_task_hourly = sla_avg_execution_time_hourly.filter(
            ["run_date_hour", "longest_running_task"], axis=1
        )

        sla_miss_pct = pd.merge(slamiss_pct_exectime, sla_longest_running_task_hourly, on=["run_date_hour"])
        sla_miss_percent_past_day_hourly = pd.merge(sla_miss_pct, sla_avg_queue_time_hourly, on=["run_date_hour"])
        sla_miss_percent_past_day_hourly["task_queue_time"] = (
            sla_miss_percent_past_day_hourly["task_queue_time"].round(0).astype(int).apply(str)
        )
        sla_longest_queue_time_hourly = (
            sla_miss_percent_past_day_hourly[["run_date_hour", "task_queue_time"]]
            .sort_values("task_queue_time", ascending=False)
            .head(1)
        )

        sla_miss_percent_past_day_hourly.rename(
            columns={
                "task_queue_time": "Average Task Queue Time (seconds)",
                "longest_running_task": "Longest Running Task",
                "top_pct_violator": "Top Violator (%)",
                "top_absolute_violator": "Top Violator (absolute)",
                "run_date_hour": "Hour",
                "sla_miss_percent(missed_tasks/total_tasks)": "SLA miss % (Missed Tasks/Total Tasks)",
            },
            inplace=True,
        )

        obs1_hourlytrend = (
            sla_highest_sla_miss_hour["run_date_hour"].apply(str) + " - hour had the highest percentage sla misses"
        ).to_string(index=False)
        obs2_hourlytrend = (
            sla_longest_queue_time_hourly["run_date_hour"].apply(str)
            + " - hour had the longest average queue time ( "
            + sla_longest_queue_time_hourly["task_queue_time"].apply(str)
            + " seconds)"
        ).to_string(index=False)
        obs3_hourlytrend = (
            sla_highest_tasks_hour["run_date_hour"].apply(str) + " - hour had the most tasks running"
        ).to_string(index=False)

        observations_hourly_list = [obs1_hourlytrend, obs2_hourlytrend, obs3_hourlytrend]

        observations_hourly_reccomendations = "".join([f"<li>{item}</li>" for item in observations_hourly_list])

        return observations_hourly_reccomendations, sla_miss_percent_past_day_hourly
    except:
        sla_miss_percent_past_day_hourly = pd.DataFrame(
            columns=[
                "SLA miss % (Missed Tasks/Total Tasks)",
                "Top Violator (%)",
                "Top Violator (absolute)",
                "Longest Running Task",
                "Hour",
                "Average Task Queue Time (seconds)",
            ]
        )
        observations_hourly_reccomendations = ""
        return observations_hourly_reccomendations, sla_miss_percent_past_day_hourly


def sla_dag_miss(sla_run_detail, serializeddag_notnull):
    """
    Function to generate DAG miss table giving us details about the SLA miss % for the given timeframes along with the average execution time.
    """
    try:
        seven_day_prior = today - timedelta(days=long_time_frame, hours=0, minutes=0)
        three_day_prior = today - timedelta(days=medium_time_frame, hours=0, minutes=0)
        one_day_prior = today - timedelta(days=short_time_frame, hours=0, minutes=0)

        dag_sla_count_df_weekprior, dag_sla_count_df_weekprior_avgduration = sla_miss_count_func_timeframe(
            sla_run_detail, seven_day_prior
        )
        dag_sla_count_df_threedayprior, dag_sla_count_df_threedayprior_avgduration = sla_miss_count_func_timeframe(
            sla_run_detail, three_day_prior
        )
        dag_sla_count_df_onedayprior, dag_sla_count_df_onedayprior_avgduration = sla_miss_count_func_timeframe(
            sla_run_detail, one_day_prior
        )

        def sla_run_count_func_timeframe(timeframe):

            tf = sla_run_detail[sla_run_detail["start_date"].between(timeframe, today)]
            return tf

        dag_sla_run_count_week_prior = sla_run_count_func_timeframe(seven_day_prior)
        dag_sla_run_count_three_day_prior = sla_run_count_func_timeframe(three_day_prior)
        dag_sla_run_count_one_day_prior = sla_run_count_func_timeframe(one_day_prior)

        dag_sla_run_count_week_prior_success = (
            dag_sla_run_count_week_prior[dag_sla_run_count_week_prior["state"] == "success"]
            .groupby(["dag_id", "task_id"])
            .size()
            .to_frame(name="success_count")
            .reset_index()
        )
        dag_sla_run_count_week_prior_failure = (
            dag_sla_run_count_week_prior[dag_sla_run_count_week_prior["state"] == "failed"]
            .groupby(["dag_id", "task_id"])
            .size()
            .to_frame(name="failure_count")
            .reset_index()
        )

        dag_sla_run_count_week_prior_success_duration_stats = (
            dag_sla_run_count_week_prior[dag_sla_run_count_week_prior["state"] == "success"]
            .groupby(["dag_id", "task_id"])["duration"]
            .agg(["mean", "min", "max"])
            .reset_index()
        )
        dag_sla_run_count_week_prior_failure_duration_stats = (
            dag_sla_run_count_week_prior[dag_sla_run_count_week_prior["state"] == "failed"]
            .groupby(["dag_id", "task_id"])["duration"]
            .agg(["mean", "min", "max"])
            .reset_index()
        )

        dag_sla_totalcount_week_prior = sla_totalcount_func_timeframe(dag_sla_run_count_week_prior)
        dag_sla_totalcount_three_day_prior = sla_totalcount_func_timeframe(dag_sla_run_count_three_day_prior)
        dag_sla_totalcount_one_day_prior = sla_totalcount_func_timeframe(dag_sla_run_count_one_day_prior)

        dag_obs5_sladpercent_weekprior = observation_slapercent_func_timeframe(
            dag_sla_count_df_weekprior, dag_sla_totalcount_week_prior
        )
        dag_obs6_sladpercent_threedayprior = observation_slapercent_func_timeframe(
            dag_sla_count_df_threedayprior, dag_sla_totalcount_three_day_prior
        )
        dag_obs7_sladpercent_onedayprior = observation_slapercent_func_timeframe(
            dag_sla_count_df_onedayprior, dag_sla_totalcount_one_day_prior
        )

        dag_obs7_sladetailed_week = (
            "In the past "
            + str(long_time_frame)
            + " days, "
            + dag_obs5_sladpercent_weekprior
            + "% tasks have missed their SLA"
        )
        dag_obs6_sladetailed_threeday = (
            "In the past "
            + str(medium_time_frame)
            + " days, "
            + dag_obs6_sladpercent_threedayprior
            + "% tasks have missed their SLA"
        )
        dag_obs5_sladetailed_oneday = (
            "In the past "
            + str(short_time_frame)
            + " days, "
            + dag_obs7_sladpercent_onedayprior
            + "% tasks have missed their SLA"
        )

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
            dag_sla_miss_pct_df_week_prior["size"] * 100 / dag_sla_miss_pct_df_week_prior["total_count"]
        ).round(2)
        dag_sla_miss_pct_df_threeday_prior["sla_miss_percent_three_day"] = (
            dag_sla_miss_pct_df_threeday_prior["size"] * 100 / dag_sla_miss_pct_df_threeday_prior["total_count"]
        ).round(2)
        dag_sla_miss_pct_df_oneday_prior["sla_miss_percent_one_day"] = (
            dag_sla_miss_pct_df_oneday_prior["size"] * 100 / dag_sla_miss_pct_df_oneday_prior["total_count"]
        ).round(2)

        dag_sla_miss_pct_df1 = dag_sla_miss_pct_df_week_prior.merge(
            dag_sla_miss_pct_df_threeday_prior, on=["dag_id", "task_id"], how="left"
        )
        dag_sla_miss_pct_df2 = dag_sla_miss_pct_df1.merge(
            dag_sla_miss_pct_df_oneday_prior, on=["dag_id", "task_id"], how="left"
        )
        dag_sla_miss_pct_df3 = dag_sla_miss_pct_df2.merge(serializeddag_notnull, on=["dag_id", "task_id"], how="left")

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

        dag_sla_miss_pct_detailed["duration_x"] = dag_sla_miss_pct_detailed["duration_x"].fillna(0).round(0).astype(int)
        dag_sla_miss_pct_detailed["duration_y"] = dag_sla_miss_pct_detailed["duration_y"].fillna(0).round(0).astype(int)
        dag_sla_miss_pct_detailed["duration"] = dag_sla_miss_pct_detailed["duration"].fillna(0).round(0).astype(int)
        dag_sla_miss_pct_detailed["sla"] = dag_sla_miss_pct_detailed["sla"].astype(int)
        dag_sla_miss_pct_detailed["sla_miss_percent_week"] = dag_sla_miss_pct_detailed["sla_miss_percent_week"].fillna(
            0
        )
        dag_sla_miss_pct_detailed["sla_miss_percent_three_day"] = dag_sla_miss_pct_detailed[
            "sla_miss_percent_three_day"
        ].fillna(0)
        dag_sla_miss_pct_detailed["sla_miss_percent_one_day"] = dag_sla_miss_pct_detailed[
            "sla_miss_percent_one_day"
        ].fillna(0)

        dag_sla_miss_pct_detailed["Dag: Task"] = (
            dag_sla_miss_pct_detailed["dag_id"].apply(str) + ": " + dag_sla_miss_pct_detailed["task_id"].apply(str)
        )
        dag_sla_miss_pct_detailed["Short Timeframe SLA miss % (avg execution time)"] = (
            dag_sla_miss_pct_detailed["sla_miss_percent_one_day"].apply(str)
            + "% ("
            + dag_sla_miss_pct_detailed["duration"].apply(str)
            + " s)"
        )
        dag_sla_miss_pct_detailed["Medium Timeframe SLA miss % (avg execution time)"] = (
            dag_sla_miss_pct_detailed["sla_miss_percent_three_day"].apply(str)
            + "% ("
            + dag_sla_miss_pct_detailed["duration_y"].apply(str)
            + " s)"
        )
        dag_sla_miss_pct_detailed["Long Timeframe SLA miss % (avg execution time)"] = (
            dag_sla_miss_pct_detailed["sla_miss_percent_week"].apply(str)
            + "% ("
            + dag_sla_miss_pct_detailed["duration_x"].apply(str)
            + " s)"
        )

        dag_sla_miss_pct_filtered = dag_sla_miss_pct_detailed.filter(
            [
                "Dag: Task",
                "sla",
                "Short Timeframe SLA miss % (avg execution time)",
                "Medium Timeframe SLA miss % (avg execution time)",
                "Long Timeframe SLA miss % (avg execution time)",
            ],
            axis=1,
        ).sort_values(by=["Long Timeframe SLA miss % (avg execution time)"], ascending=False)

        dag_sla_miss_pct_filtered.rename(columns={"sla": "Current SLA"}, inplace=True)

        dag_sla_miss_pct_recc1 = dag_sla_miss_pct_detailed.nlargest(3, ["sla_miss_percent_week"]).fillna(0)
        dag_sla_miss_pct_recc2 = dag_sla_miss_pct_recc1.filter(
            ["dag_id", "task_id", "sla", "sla_miss_percent_week", "Dag: Task"], axis=1
        ).fillna(0)
        dag_sla_miss_pct_df4_recc3 = pd.merge(
            pd.merge(
                dag_sla_miss_pct_recc2,
                dag_sla_run_count_week_prior_success,
                on=["dag_id", "task_id"],
            ),
            dag_sla_run_count_week_prior_failure,
            on=["dag_id", "task_id"],
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
            dag_sla_miss_pct_df4_recc4["Dag: Task"].apply(str)
            + ":- Of the "
            + dag_sla_miss_pct_df4_recc4["sla_miss_percent_week"].apply(str)
            + "% of the tasks that missed their SLA of "
            + dag_sla_miss_pct_df4_recc4["sla"].apply(str)
            + " seconds, "
            + dag_sla_miss_pct_df4_recc4["success_count"].apply(str)
            + " succeeded (min: "
            + dag_sla_miss_pct_df4_recc4["min_x"].round(0).astype(int).apply(str)
            + " s, avg: "
            + dag_sla_miss_pct_df4_recc4["mean_x"].round(0).astype(int).apply(str)
            + " s, max: "
            + dag_sla_miss_pct_df4_recc4["max_x"].round(0).astype(int).apply(str)
            + " s) & "
            + dag_sla_miss_pct_df4_recc4["failure_count"].apply(str)
            + " failed (min: "
            + dag_sla_miss_pct_df4_recc4["min_y"].round(0).astype(int).apply(str)
            + " s, avg: "
            + dag_sla_miss_pct_df4_recc4["mean_y"].round(0).astype(int).apply(str)
            + " s, max: "
            + dag_sla_miss_pct_df4_recc4["max_y"].round(0).fillna(0).astype(int).apply(str)
            + " s)"
        )

        daily_weeklytrend_observations = [
            dag_obs5_sladetailed_oneday,
            dag_obs6_sladetailed_threeday,
            dag_obs7_sladetailed_week,
        ]
        daily_weeklytrend_observations_loop = "".join([f"<li>{item}</li>" for item in daily_weeklytrend_observations])

        dag_obs4_sladetailed = dag_sla_miss_pct_df4_recc4["Recommendations"].tolist()
        dag_sla_miss_trend = "".join([f"<li>{item}</li>" for item in dag_obs4_sladetailed])

        return daily_weeklytrend_observations_loop, dag_sla_miss_trend, dag_sla_miss_pct_filtered
    except:
        daily_weeklytrend_observations_loop = ""
        dag_sla_miss_trend = ""
        dag_sla_miss_pct_filtered = pd.DataFrame(
            columns=[
                "Dag: Task",
                "Current SLA",
                "Short Timeframe SLA miss % (avg execution time)",
                "Medium Timeframe SLA miss % (avg execution time)",
                "Long Timeframe SLA miss % (avg execution time)",
            ]
        )
        return daily_weeklytrend_observations_loop, dag_sla_miss_trend, dag_sla_miss_pct_filtered


def print_output():
    """
    Function to retrieve the output dataframes and embed them in html format and generate an email report.
    """
    sla_run_detail, serializeddag_notnull = initial()
    daily_slamiss_pct_last7days, daily_weeklytrend_observations_loop = sla_daily_miss(sla_run_detail)
    observations_hourly_reccomendations, sla_miss_percent_past_day_hourly = sla_hourly_miss(sla_run_detail)
    daily_weeklytrend_observations_loop, dag_sla_miss_trend, dag_sla_miss_pct_filtered = sla_dag_miss(
        sla_run_detail, serializeddag_notnull
    )

    short_tf = today - timedelta(days=short_time_frame)
    medium_tf = today - timedelta(days=medium_time_frame)
    long_tf = today - timedelta(days=long_time_frame)

    short_time_frame_print = "Short Timeframe: " + str(short_time_frame) + " day (" + short_tf.strftime("%b %d") + ")"
    medium_time_frame_print = (
        "Medium Timeframe: " + str(medium_time_frame) + " day (" + medium_tf.strftime("%b %d") + ")"
    )
    long_time_frame_print = "Short Timeframe: " + str(long_time_frame) + " day (" + long_tf.strftime("%b %d") + ")"

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
        <b>{short_time_frame_print}<br>
        {medium_time_frame_print}<br>
        {long_time_frame_print}</b><br>
        <h2><u>Daily SLA Misses</u></h2>
        <p>Details for SLA Miss Percentage for the past {long_time_frame} days. Also, it tells us the task which has missed it's SLA benchmark the most
        in terms of the absolute number and %</p>
        {daily_weeklytrend_observations_loop}
        {daily_slamiss_pct_last7days.to_html(index=False)}

    
        <h2><u>Hourly SLA Misses</u></h2>
        <p>Average hourly trend for SLA Miss % for the past {short_time_frame} days. Also, it tells us the task which has missed it's SLA benchmark the most
        in terms of the absolute number and %. Along with this, it tells us which task took the longest time to run and the average task queue time for that
        particular hour</p>

        {observations_hourly_reccomendations}
        {sla_miss_percent_past_day_hourly.to_html(index=False)}
        </body>
        </html>

        <h2><u>DAG SLA Misses</u></h2>
        <p>Detailed view of all the tasks and it's SLA Miss % with it's average execution time over the past {short_time_frame} day, {medium_time_frame} days and {long_time_frame} days. This can
        help in identifying if there has been an improvement in the processing time after a possible optimization in code and to observe the consistency. </p>
        {dag_sla_miss_trend}
        {dag_sla_miss_pct_filtered.to_html(index=False)}
        """

    send_email(to=EMAIL_ADDRESS, subject="Airflow SLA Report", html_content=html_content1)


def no_data_print():

    short_tf = today - timedelta(days=short_time_frame)
    medium_tf = today - timedelta(days=medium_time_frame)
    long_tf = today - timedelta(days=long_time_frame)

    short_time_frame_print = "Short Timeframe: " + str(short_time_frame) + " day (" + short_tf.strftime("%b %d") + ")"
    medium_time_frame_print = (
        "Medium Timeframe: " + str(medium_time_frame) + " day (" + medium_tf.strftime("%b %d") + ")"
    )
    long_time_frame_print = "Short Timeframe: " + str(long_time_frame) + " day (" + long_tf.strftime("%b %d") + ")"
    html_content = f"""\
    <html>
    <head>
    </head>
    <body>
    {short_time_frame_print}
    {medium_time_frame_print}
    {long_time_frame_print}
    <h2 style="color:red"><u>No Data Available</u></h2>
    <p><b>Please make sure the respective DAG run data is avaialble in the airflow metadatabase.</b></p>
    </body>
    </html>
    """
    send_email(to=EMAIL_ADDRESS, subject="Airflow SLA Report", html_content=html_content)


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
    "sla_miss_airflow_dag",
    default_args=default_args,
    description="DAG generating the SLA miss email report",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    run_this = PythonOperator(task_id="call_sla_mail_func", python_callable=print_output, dag=dag)
