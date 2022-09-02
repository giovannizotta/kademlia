import altair as alt
import pandas as pd
from runexpy.utils import IterParamsT

from plot.data import get_join_time, get_crash_time, get_slots_with_ci, get_line_ci_chart

DATA_FILE = "data.json"


def get_healthy_chart(conf: IterParamsT, start_time: float, end_time: float, dht: str) -> alt.Chart:
    join_time_df = get_join_time(conf)
    crash_time_df = get_crash_time(conf)
    healthy_df = get_healthy_nodes(crash_time_df, join_time_df)
    healthy_df = healthy_df[(healthy_df["time"] >= start_time) & (healthy_df["time"] <= end_time)]

    df = get_slots_with_ci(healthy_df, "time", "healthy_nodes", "mean")
    df["dht"] = dht

    line, ci = get_line_ci_chart(df, "Time", "Healthy nodes")
    return line + ci


def get_healthy_nodes(crash_time_df, join_time_df):
    join_time_df["increment"] = 1
    crash_time_df["increment"] = -1
    df = pd.concat([join_time_df, crash_time_df], ignore_index=True).sort_values(by="time", ignore_index=True)
    df["healthy_nodes"] = df.groupby("seed")["increment"].cumsum()
    return df