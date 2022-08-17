import numpy as np
import pandas as pd
import json

from airflow import settings
from airflow.models import DAG, DagRun, TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.python import PythonOperator
from datetime import date, datetime, timedelta
from airflow.utils.email import send_email

EMAIL_ADDRESS = ["nikhil.manjunatha@clairvoyantsoft.com"]


def sla_mail():

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
    serializeddag_json_dump = serializeddag_df[data_col].apply(json.dumps)
    serializeddag_json_load = serializeddag_json_dump.apply(json.loads)
    serializeddag_json_list = pd.DataFrame(serializeddag_json_load.values.tolist())["dag"]
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
    sla_run_detail["start_dt"] = sla_run_detail["start_date"].dt.date
    sla_run_detail["start_date"] = pd.to_datetime(sla_run_detail["start_date"]).dt.tz_localize(None)

    dt = date.today()
    today = datetime.combine(dt, datetime.min.time())

    short_time_frame = 8
    medium_time_frame = 12
    long_time_frame = 18

    # SLA Miss Percent Past one week

    week_prior = today - timedelta(days=long_time_frame, hours=0, minutes=0)

    sla_miss_count = sla_run_detail[sla_run_detail["duration"] > sla_run_detail["sla"]][
        sla_run_detail["start_date"].between(week_prior, today)
    ].sort_values(["start_date"])

    sla_miss_count_datewise = (
        sla_miss_count.groupby(["start_dt"]).size().to_frame(name="slamiss_count_datewise").reset_index()
    )
    sla_count_df = sla_miss_count.groupby(["start_dt", "dag_id", "task_id"]).size().to_frame(name="size").reset_index()
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
    sla_miss_pct_df = pd.merge(sla_miss_count_datewise, sla_totalcount_datewise, on=["start_dt"])
    sla_miss_pct_df["sla_miss_percent"] = (
        sla_miss_pct_df["slamiss_count_datewise"] * 100 / sla_miss_pct_df["total_count"]
    ).round(2)

    sla_miss_pct_df["sla_miss_percent(missed_tasks/total_tasks)"] = sla_miss_pct_df.apply(
        lambda x: "%s%s(%s/%s)" % (x["sla_miss_percent"], "% ", x["slamiss_count_datewise"], x["total_count"]),
        axis=1,
    )

    sla_miss_percent = sla_miss_pct_df.filter(["start_dt", "sla_miss_percent(missed_tasks/total_tasks)"], axis=1)
    sla_miss_temp_df_pct1 = pd.merge(
        sla_count_df,
        sla_totalcount_datewise_taskwise,
        on=["start_dt", "dag_id", "task_id"],
    )
    sla_miss_temp_df_pct1["pct_violator"] = (
        sla_miss_temp_df_pct1["size"] * 100 / sla_miss_temp_df_pct1["totalcount"]
    ).round(2)
    sla_miss_temp_df_pct_kpi = (
        sla_miss_temp_df_pct1.sort_values("pct_violator", ascending=False).groupby("start_dt", sort=False).head(1)
    )

    sla_miss_temp_df_pct_kpi["top_pct_violator"] = sla_miss_temp_df_pct_kpi.apply(
        lambda x: "%s,%s (%s%s" % (x["dag_id"], x["task_id"], x["pct_violator"], "%)"),
        axis=1,
    )

    slamiss_percent_violator = sla_miss_temp_df_pct_kpi.filter(["start_dt", "top_pct_violator"], axis=1)
    sla_miss_temp_df_absolute_kpi = (
        sla_miss_temp_df_pct1.sort_values("size", ascending=False).groupby("start_dt", sort=False).head(1)
    )

    sla_miss_temp_df_absolute_kpi["top_absolute_violator"] = sla_miss_temp_df_absolute_kpi.apply(
        lambda x: "%s: %s (%s/%s)" % (x["dag_id"], x["task_id"], x["size"], x["totalcount"]),
        axis=1,
    )

    slamiss_absolute_violator = sla_miss_temp_df_absolute_kpi.filter(["start_dt", "top_absolute_violator"], axis=1)
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
    # return slamiss_pct_last7days

    # Hourly Trend

    day_prior = today - timedelta(days=short_time_frame, hours=0, minutes=0)

    sla_miss_count_past_day = sla_run_detail[sla_run_detail["duration"] > sla_run_detail["sla"]][
        sla_run_detail["start_date"].between(day_prior, today)
    ]

    sla_miss_count_hourwise = (
        sla_miss_count_past_day.groupby(["run_date_hour"]).size().to_frame(name="slamiss_count_hourwise").reset_index()
    )
    sla_count_df_past_day = (
        sla_miss_count_past_day.groupby(["run_date_hour", "dag_id", "task_id"])
        .size()
        .to_frame(name="size")
        .reset_index()
    )
    sla_avg_execution_time_taskwise = (
        sla_miss_count_past_day.groupby(["run_date_hour", "dag_id", "task_id"])["duration"].mean().reset_index()
    )
    sla_avg_execution_time_hourly = (
        sla_avg_execution_time_taskwise.sort_values("duration", ascending=False)
        .groupby("run_date_hour", sort=False)
        .head(1)
    )

    sla_pastday_run_count_df = sla_run_detail[sla_run_detail["start_date"].between(day_prior, today)]
    sla_avg_queue_time_hourly = (
        sla_pastday_run_count_df.groupby(["run_date_hour"])["task_queue_time"].mean().reset_index()
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
    sla_miss_pct_df_past_day = pd.merge(sla_miss_count_hourwise, sla_totalcount_hourwise, on=["run_date_hour"])
    sla_miss_pct_df_past_day["sla_miss_percent"] = (
        sla_miss_pct_df_past_day["slamiss_count_hourwise"] * 100 / sla_miss_pct_df_past_day["total_count"]
    ).round(2)
    sla_miss_pct_df_past_day["sla_miss_percent(missed_tasks/total_tasks)"] = sla_miss_pct_df_past_day[
        "sla_miss_percent"
    ].apply(str)

    sla_highest_sla_miss_hour = (
        sla_miss_pct_df_past_day[["run_date_hour", "sla_miss_percent"]]
        .sort_values("sla_miss_percent", ascending=False)
        .head(1)
    )
    sla_highest_tasks_hour = (
        sla_miss_pct_df_past_day[["run_date_hour", "total_count"]].sort_values("total_count", ascending=False).head(1)
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
        sla_miss_temp_df_pct1_past_day["size"] * 100 / sla_miss_temp_df_pct1_past_day["totalcount"]
    ).round(2)
    sla_miss_temp_df_pct_kpi_past_day = (
        sla_miss_temp_df_pct1_past_day.sort_values("pct_violator", ascending=False)
        .groupby("run_date_hour", sort=False)
        .head(1)
    )

    sla_miss_temp_df_pct_kpi_past_day["top_pct_violator"] = sla_miss_temp_df_pct_kpi_past_day.apply(
        lambda x: "%s: %s (%s%s" % (x["dag_id"], x["task_id"], x["pct_violator"], "%)"),
        axis=1,
    )

    slamiss_percent_violator_past_day = sla_miss_temp_df_pct_kpi_past_day.filter(
        ["run_date_hour", "top_pct_violator"], axis=1
    )
    sla_miss_temp_df_absolute_kpi_past_day = (
        sla_miss_temp_df_pct1_past_day.sort_values("size", ascending=False).groupby("run_date_hour", sort=False).head(1)
    )

    sla_miss_temp_df_absolute_kpi_past_day["top_absolute_violator"] = sla_miss_temp_df_absolute_kpi_past_day.apply(
        lambda x: "%s: %s (%s/%s)" % (x["dag_id"], x["task_id"], x["size"], x["totalcount"]),
        axis=1,
    )

    slamiss_absolute_violator_past_day = sla_miss_temp_df_absolute_kpi_past_day.filter(
        ["run_date_hour", "top_absolute_violator"], axis=1
    )
    slamiss_pct_exectime = pd.merge(
        pd.merge(
            sla_miss_percent_past_day,
            slamiss_percent_violator_past_day,
            on="run_date_hour",
        ),
        slamiss_absolute_violator_past_day,
        on="run_date_hour",
    ).sort_values("run_date_hour", ascending=False)

    sla_avg_execution_time_hourly["duration"] = (
        sla_avg_execution_time_hourly["duration"].round(0).astype(int).astype(str)
    )
    sla_avg_execution_time_hourly["longest_running_task"] = sla_avg_execution_time_hourly.apply(
        lambda x: "%s: %s (%s s)" % (x["dag_id"], x["task_id"], x["duration"]), axis=1
    )

    sla_longest_running_task = sla_avg_execution_time_hourly.filter(["run_date_hour", "longest_running_task"], axis=1)

    sla_miss_pct = pd.merge(slamiss_pct_exectime, sla_longest_running_task, on=["run_date_hour"])
    sla_miss_percent_past_day_hourly = pd.merge(sla_miss_pct, sla_avg_queue_time_hourly, on=["run_date_hour"])
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
        sla_highest_sla_miss_hour["run_date_hour"].apply(str) + " - hour had the highest percentage sla misses"
    )
    obs2_hourlytrend = (
        sla_longest_queue_time_hour["run_date_hour"].apply(str)
        + " - hour had the longest average queue time ( "
        + sla_longest_queue_time_hour["task_queue_time"].apply(str)
        + " seconds)"
    )
    obs3_hourlytrend = sla_highest_tasks_hour["run_date_hour"].apply(str) + " - hour had the most tasks running"

    # hourlytrend_observations = [obs1_hourlytrend.to_string(index=False),obs2_hourlytrend.to_string(index=False),obs3_hourlytrend.to_string(index=False)]

    # return sla_miss_percent_past_day_hourly,hourlytrend_observations

    # SLA Detailed Metric

    seven_day_prior = today - timedelta(days=long_time_frame, hours=0, minutes=0)
    three_day_prior = today - timedelta(days=medium_time_frame, hours=0, minutes=0)
    one_day_prior = today - timedelta(days=short_time_frame, hours=0, minutes=0)

    def sla_miss_count_func_timeframe(input_df, timeframe):

        df = input_df[input_df["duration"] > input_df["sla"]][input_df["start_date"].between(timeframe, today)]
        return df

    sla_miss_count_weekprior = sla_miss_count_func_timeframe(sla_run_detail, seven_day_prior)
    sla_miss_count_threedayprior = sla_miss_count_func_timeframe(sla_run_detail, three_day_prior)
    sla_miss_count_onedayprior = sla_miss_count_func_timeframe(sla_run_detail, one_day_prior)

    def sla_count_df_func_timeframe(input_df):

        df = input_df.groupby(["dag_id", "task_id"]).size().to_frame(name="size").reset_index()
        return df

    sla_count_df_weekprior = sla_count_df_func_timeframe(sla_miss_count_weekprior)
    sla_count_df_threedayprior = sla_count_df_func_timeframe(sla_miss_count_threedayprior)
    sla_count_df_onedayprior = sla_count_df_func_timeframe(sla_miss_count_onedayprior)

    def sla_count_df_func_timeframe_duration(input_df):

        df = input_df.groupby(["dag_id", "task_id"])["duration"].mean().reset_index()
        return df

    sla_count_df_weekprior_avgduration = sla_count_df_func_timeframe_duration(sla_miss_count_weekprior)
    sla_count_df_threedayprior_avgduration = sla_count_df_func_timeframe_duration(sla_miss_count_threedayprior)
    sla_count_df_onedayprior_avgduration = sla_count_df_func_timeframe_duration(sla_miss_count_onedayprior)

    def sla_run_count_func_timeframe(timeframe):

        tf = sla_run_detail[sla_run_detail["start_date"].between(timeframe, today)]
        return tf

    sla_run_count_week_prior = sla_run_count_func_timeframe(seven_day_prior)
    sla_run_count_three_day_prior = sla_run_count_func_timeframe(three_day_prior)
    sla_run_count_one_day_prior = sla_run_count_func_timeframe(one_day_prior)

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

    def sla_totalcount_func_timeframe(input_df):

        df = (
            input_df.groupby(["dag_id", "task_id"])
            .size()
            .to_frame(name="total_count")
            .sort_values("total_count", ascending=False)
            .reset_index()
        )
        return df

    sla_totalcount_week_prior = sla_totalcount_func_timeframe(sla_run_count_week_prior)
    sla_totalcount_three_day_prior = sla_totalcount_func_timeframe(sla_run_count_three_day_prior)
    sla_totalcount_one_day_prior = sla_totalcount_func_timeframe(sla_run_count_one_day_prior)

    def observation_slapercent_func_timeframe(input_df1, input_df2):

        df = (
            np.nan_to_num(
                ((input_df1["size"].sum() * 100) / (input_df2["total_count"].sum())),
                0,
            )
            .round(2)
            .astype("str")
        )
        return df

    obs5_sladpercent_weekprior = observation_slapercent_func_timeframe(
        sla_count_df_weekprior, sla_totalcount_week_prior
    )
    obs6_sladpercent_threedayprior = observation_slapercent_func_timeframe(
        sla_count_df_threedayprior, sla_totalcount_three_day_prior
    )
    obs7_sladpercent_onedayprior = observation_slapercent_func_timeframe(
        sla_count_df_onedayprior, sla_totalcount_one_day_prior
    )

    obs7_sladetailed_week = "In the past 7 days, " + obs5_sladpercent_weekprior + "% tasks have missed their SLA"
    obs6_sladetailed_threeday = (
        "In the past 3 days, " + obs6_sladpercent_threedayprior + "% tasks have missed their SLA"
    )
    obs5_sladetailed_oneday = "In the past 1 day, " + obs7_sladpercent_onedayprior + "% tasks have missed their SLA"

    sla_miss_pct_df_week_prior = pd.merge(
        pd.merge(sla_count_df_weekprior, sla_totalcount_week_prior, on=["dag_id", "task_id"]),
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
            sla_count_df_onedayprior,
            sla_totalcount_one_day_prior,
            on=["dag_id", "task_id"],
        ),
        sla_count_df_onedayprior_avgduration,
        on=["dag_id", "task_id"],
    )

    sla_miss_pct_df_week_prior["sla_miss_percent_week"] = (
        sla_miss_pct_df_week_prior["size"] * 100 / sla_miss_pct_df_week_prior["total_count"]
    ).round(2)
    sla_miss_pct_df_threeday_prior["sla_miss_percent_three_day"] = (
        sla_miss_pct_df_threeday_prior["size"] * 100 / sla_miss_pct_df_threeday_prior["total_count"]
    ).round(2)
    sla_miss_pct_df_oneday_prior["sla_miss_percent_one_day"] = (
        sla_miss_pct_df_oneday_prior["size"] * 100 / sla_miss_pct_df_oneday_prior["total_count"]
    ).round(2)

    sla_miss_pct_df1 = sla_miss_pct_df_week_prior.merge(
        sla_miss_pct_df_threeday_prior, on=["dag_id", "task_id"], how="left"
    )
    sla_miss_pct_df2 = sla_miss_pct_df1.merge(sla_miss_pct_df_oneday_prior, on=["dag_id", "task_id"], how="left")
    sla_miss_pct_df3 = sla_miss_pct_df2.merge(serializeddag_notnull, on=["dag_id", "task_id"], how="left")

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

    sla_miss_pct_df4["duration_x"] = sla_miss_pct_df4["duration_x"].fillna(0).round(0).astype(int)
    sla_miss_pct_df4["duration_y"] = sla_miss_pct_df4["duration_y"].fillna(0).round(0).astype(int)
    sla_miss_pct_df4["duration"] = sla_miss_pct_df4["duration"].fillna(0).round(0).astype(int)
    sla_miss_pct_df4["sla"] = sla_miss_pct_df4["sla"].astype(int)
    sla_miss_pct_df4["sla_miss_percent_week"] = sla_miss_pct_df4["sla_miss_percent_week"].fillna(0)
    sla_miss_pct_df4["sla_miss_percent_three_day"] = sla_miss_pct_df4["sla_miss_percent_three_day"].fillna(0)
    sla_miss_pct_df4["sla_miss_percent_one_day"] = sla_miss_pct_df4["sla_miss_percent_one_day"].fillna(0)

    sla_miss_pct_df4["Dag: Task"] = (
        sla_miss_pct_df4["dag_id"].apply(str) + ": " + sla_miss_pct_df4["task_id"].apply(str)
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
        sla_miss_pct_df4["sla_miss_percent_week"].apply(str) + "% (" + sla_miss_pct_df4["duration_x"].apply(str) + " s)"
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

    sla_miss_pct_df4_temp_recc1 = sla_miss_pct_df4.nlargest(3, ["sla_miss_percent_week"]).fillna(0)
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

    obs4_sladetailed = sla_miss_pct_df4_temp_recc4["Recommendations"].tolist()
    obs4_sladetailed = "".join([f"<li>{item}</li>" for item in obs4_sladetailed])
    # weeklytrend_observations = [obs5_sladetailed_oneday,obs6_sladetailed_threeday,obs7_sladetailed_week]

    html_content = f"""\
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
        <h2>Daily SLA Misses</h2>
        <p>Details for SLA Miss Percentage for the past 7 days. Also, it tells us the task which has missed it's SLA benchmark the most
        in terms of the absolute number and %</p>
        <li>{ obs5_sladetailed_oneday }</li>
        <li>{ obs6_sladetailed_threeday }</li>
        <li>{ obs7_sladetailed_week }</li>
        {slamiss_pct_last7days.to_html(index=False)}

       <h2>DAG SLA Misses</h2>
        <p>Detailed view of all the tasks and it's SLA Miss % with it's average execution time over the past 1 day, 3 day and 7 days. This can
        help in identifying if there has been an improvement in the processing time after a possible optimization in code and to observe the consistency. </p>
        {obs4_sladetailed}
        {sla_miss_pct_df5.to_html(index=False)}

        <h2>Hourly SLA Misses</h2>
        <p>Hourly trend for SLA Miss % for the past 7 days. Also, it tells us the task which has missed it's SLA benchmark the most
        in terms of the absolute number and %. Along with this, it tells us which task took the longest time to run and the average task queue time for that
        particular hour</p>

        <li>{ obs1_hourlytrend.to_string(index=False) }</li>
        <li>{ obs2_hourlytrend.to_string(index=False) }</li>
        <li>{ obs3_hourlytrend.to_string(index=False) }</li>
        {sla_miss_percent_past_day_hourly.to_html(index=False)}

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
    "a1_test_sla",
    default_args=default_args,
    description="A simple email ",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    run_this = PythonOperator(task_id="call_sla_mail_func", python_callable=sla_mail, dag=dag)
