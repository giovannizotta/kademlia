import json

import altair as alt
import pandas as pd
import streamlit as st
from runexpy.campaign import Campaign
from runexpy.utils import IterParamsT


@st.experimental_memo
def get_queue_load(conf: IterParamsT) -> pd.DataFrame:
    c = Campaign.load('campaigns/experiment')
    result = list()
    for campaign_results, files in c.get_results_for(conf):
        with open(files["data.json"]) as f:
            dct = json.load(f)
        for node, queue_load in dct["queue_load"].items():
            tmp = pd.DataFrame(queue_load, columns=["time", "load"])
            tmp["node"] = node
            for k, v in campaign_results.params.items():
                tmp[k] = v
            result.append(tmp)
    return pd.concat(result, ignore_index=True)


@st.experimental_memo
def get_crash_time(conf: IterParamsT) -> pd.DataFrame:
    c = Campaign.load('campaigns/experiment')
    result = list()
    for campaign_results, files in c.get_results_for(conf):
        with open(files["data.json"]) as f:
            dct = json.load(f)
        for node, time in dct["crashed_time"].items():
            tmp = {"node": node, "time": time}
            for k, v in campaign_results.params.items():
                tmp[k] = v
            result.append(tmp)
    return pd.DataFrame(result)


@st.experimental_memo
def get_join_time(conf: IterParamsT) -> pd.DataFrame:
    c = Campaign.load('campaigns/experiment')
    result = list()
    for campaign_results, files in c.get_results_for(conf):
        with open(files["data.json"]) as f:
            dct = json.load(f)
        for node, time in dct["joined_time"].items():
            tmp = {"node": node, "time": time}
            for k, v in campaign_results.params.items():
                tmp[k] = v
            result.append(tmp)
    return pd.DataFrame(result)


@st.experimental_memo
# @st.experimental_memo
def get_client_requests(conf: IterParamsT) -> pd.DataFrame:
    c = Campaign.load('campaigns/experiment')
    result = list()
    for campaign_results, files in c.get_results_for(conf):
        with open(files["data.json"]) as f:
            dct = json.load(f)
        tmp = pd.DataFrame(dct["client_requests"], columns=["time", "latency", "hops"])
        for k, v in campaign_results.params.items():
            tmp[k] = v
        result.append(tmp)
    df = pd.concat(result, ignore_index=True)
    return df


@st.experimental_memo
def get_client_timeout(conf: IterParamsT) -> pd.DataFrame:
    c = Campaign.load('campaigns/experiment')
    result = list()
    for campaign_results, files in c.get_results_for(conf):
        with open(files["data.json"]) as f:
            dct = json.load(f)
        tmp = pd.DataFrame(dct["timed_out_requests"], columns=["time"])
        for k, v in campaign_results.params.items():
            tmp[k] = v
        result.append(tmp)
    df = pd.concat(result, ignore_index=True)
    return df


def get_slots_with_ci(df: pd.DataFrame, slot_column: str, metric: str, slot_agg: str,
                      nslots: int = 100) -> pd.DataFrame:
    slot_width = (df[slot_column].max() - df[slot_column].min()) / nslots

    df["slot"] = df[slot_column].apply(lambda x: x // slot_width).astype(int)

    df = df.groupby(["seed", "slot"]).agg(
        slot_metric=(metric, slot_agg),
    ).unstack().stack(dropna=False).reset_index()
    df["slot_metric"] = df.groupby("seed")["slot_metric"].ffill().fillna(0)

    df = df.groupby(["slot"]).agg(
        mean=("slot_metric", "mean"),
        sem=("slot_metric", "sem"),
    ).reset_index().fillna(0)

    df["slot"] *= slot_width

    df["ci95_hi"] = df["mean"] + 1.96 * df["sem"]
    df["ci95_lo"] = df["mean"] - 1.96 * df["sem"]

    return df


def get_line_ci_chart(df, xlabel, ylabel):
    line = alt.Chart(df).mark_line().encode(
        x=alt.X('slot', axis=alt.Axis(title=xlabel)),
        y=alt.Y('mean', title=ylabel),
        color=alt.Color('dht', legend=alt.Legend(title="DHT")),
        tooltip=['mean', 'ci95_lo', 'ci95_hi'],
    )
    ci = alt.Chart(df).mark_area(opacity=0.2).encode(
        x=alt.X('slot', axis=alt.Axis(title=xlabel)),
        y='ci95_lo',
        y2='ci95_hi',
        color=alt.Color('dht'),
        tooltip=['mean', 'ci95_hi', 'ci95_lo'],
    )
    return line, ci


def get_ecdf_ci_horizontal(df, xlabel, ylabel):
    points = alt.Chart(df).mark_point(filled=True, size=30).encode(
        x=alt.X('mean', axis=alt.Axis(title=xlabel)),
        y=alt.Y('slot', title=ylabel),
        color=alt.Color('dht', legend=alt.Legend(title="DHT")),
        tooltip=['mean', 'ci95_lo', 'ci95_hi'],
    )
    ci = alt.Chart(df).mark_errorbar(ticks=True, size=5).encode(
        x=alt.X('ci95_lo', axis=alt.Axis(title="")),
        x2='ci95_hi',
        y='slot',
        color=alt.Color('dht'),
        tooltip=['mean', 'ci95_hi', 'ci95_lo'],
    )
    return points, ci
