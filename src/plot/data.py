import json
from functools import partial
from multiprocess.pool import Pool
from typing import List, Tuple, Dict

import altair as alt
import pandas as pd
import streamlit as st
from runexpy.campaign import Campaign
from runexpy.utils import IterParamsT
from runexpy.result import Result

PROCESSES = 32
CAMPAIGN_DIR = 'campaigns/experiment'

def read_load(res: Tuple[Result, Dict[str, str]]) -> pd.DataFrame:
    # return pd.DataFrame([])
    campaign_results, files = res
    with open(files["data.json"]) as f:
        dct = json.load(f)
    result = []
    for node, queue_load in dct["queue_load"].items():
        tmp = pd.DataFrame(queue_load, columns=["time", "load"])
        tmp["node"] = node
        for column in ['seed', 'dht']:
            tmp[column] = campaign_results.params[column]
        result.append(tmp)
    return pd.concat(result, ignore_index=True)


@st.experimental_memo
def get_queue_load(conf: IterParamsT) -> pd.DataFrame:
    c = Campaign.load(CAMPAIGN_DIR)
    result = Pool(processes=PROCESSES).map(read_load, c.get_results_for(conf))
    return pd.concat(result, ignore_index=True)


def read_times(res: Tuple[Result, Dict[str, str]], target: str) -> pd.DataFrame:
    campaign_results, files = res
    with open(files["data.json"]) as f:
        dct = json.load(f)
    tmp = pd.DataFrame(dct[target].items(), columns=["node", "time"])
    for column in ['seed', 'dht']:
        tmp[column] = campaign_results.params[column]
    return tmp


@st.experimental_memo
def get_crash_time(conf: IterParamsT) -> pd.DataFrame:
    c = Campaign.load(CAMPAIGN_DIR)
    fn = partial(read_times, target="crashed_time")
    result = Pool(processes=PROCESSES).map(fn, c.get_results_for(conf))
    return pd.concat(result, ignore_index=True)


@st.experimental_memo
def get_join_time(conf: IterParamsT) -> pd.DataFrame:
    c = Campaign.load(CAMPAIGN_DIR)
    fn = partial(read_times, target="joined_time")
    result = Pool(processes=PROCESSES).map(fn, c.get_results_for(conf))
    return pd.concat(result, ignore_index=True)


def read_client(res: Tuple[Result, Dict[str, str]], target: str, columns: List[str]) -> pd.DataFrame:
    campaign_results, files = res
    with open(files["data.json"]) as f:
        dct = json.load(f)
    tmp = pd.DataFrame(dct[target], columns=columns)
    for column in ['seed', 'dht']:
        tmp[column] = campaign_results.params[column]
    return tmp


@st.experimental_memo
def get_client_requests(conf: IterParamsT) -> pd.DataFrame:
    c = Campaign.load(CAMPAIGN_DIR)
    fn = partial(read_client, target="client_requests", columns=["time", "latency", "hops"])
    result = Pool(processes=PROCESSES).map(fn, c.get_results_for(conf))
    df = pd.concat(result, ignore_index=True)
    return df


@st.experimental_memo
def get_client_timeout(conf: IterParamsT) -> pd.DataFrame:
    c = Campaign.load(CAMPAIGN_DIR)
    fn = partial(read_client, target="timed_out_requests", columns=["time"])
    result = Pool(processes=PROCESSES).map(fn, c.get_results_for(conf))
    df = pd.concat(result, ignore_index=True)
    return df

@st.experimental_memo
def get_stored_values(conf: IterParamsT) -> pd.DataFrame:
    c = Campaign.load(CAMPAIGN_DIR)
    fn = partial(read_client, target="true_value", columns=["time", "key", "value"])
    result = Pool(processes=PROCESSES).map(fn, c.get_results_for(conf))
    df = pd.concat(result, ignore_index=True)
    return df

@st.experimental_memo
def get_found_values(conf: IterParamsT) -> pd.DataFrame:
    c = Campaign.load(CAMPAIGN_DIR)
    fn = partial(read_client, target="returned_value", columns=["time", "key", "value"])
    result = Pool(processes=PROCESSES).map(fn, c.get_results_for(conf))
    df = pd.concat(result, ignore_index=True)
    return df

def get_slots_with_ci(df: pd.DataFrame, slot_column: str, metric: str, slot_agg: str,
                      nslots: int = 100) -> pd.DataFrame:
    # df["slot_column_max"] = df.groupby("seed")[slot_column].transform("max")
    # df["slot_column_min"] = df.groupby("seed")[slot_column].transform("min")
    # df["slot_width"] = (df["slot_column_max"] - df["slot_column_min"]) / nslots
    slot_width = (df[slot_column].max() - df[slot_column].min()) / nslots
    df["slot"] = df[slot_column] // slot_width

    # foreach seed, foreach slot, aggregate the metric + fill missing values
    df = df.groupby(["seed", "slot"]).agg(
        slot_metric=(metric, slot_agg),
        slot_metric_count=(metric, "count"),
    ).unstack().stack(dropna=False).reset_index()
    df["slot_metric"] = df.groupby("seed")["slot_metric"].ffill().fillna(0)

    # foreach slot, compute the mean across seeds aggregate results with confidence intervals
    df = df.groupby(["slot"]).agg(
        mean=("slot_metric", "mean"),
        sem=("slot_metric", "sem"),
        count=("slot_metric_count", "sum"),
    ).reset_index().fillna(0)

    df["slot"] *= slot_width

    df["ci95_hi"] = df["mean"] + 1.96 * df["sem"]
    df["ci95_lo"] = df["mean"] - 1.96 * df["sem"]

    return df


def get_line_ci_chart(df, xlabel, ylabel):
    line = alt.Chart(df).mark_line().encode(
        x=alt.X('slot', axis=alt.Axis(title=xlabel)),
        y=alt.Y('mean', title=ylabel),
        color=alt.Color('dht', legend=alt.Legend(title="DHT", orient="bottom")),
        tooltip=['mean', 'ci95_lo', 'ci95_hi'],
    ).properties(width=800, height=600)
    ci = alt.Chart(df).mark_area(opacity=0.2).encode(
        x=alt.X('slot', axis=alt.Axis(title=xlabel)),
        y='ci95_lo',
        y2='ci95_hi',
        color=alt.Color('dht', legend=None),
        tooltip=['mean', 'ci95_hi', 'ci95_lo', 'count'],
    ).properties(width=800, height=600)
    return line, ci


def get_ecdf_ci_horizontal(df, xlabel, ylabel):
    points = alt.Chart(df).mark_point(filled=True, size=30).encode(
        x=alt.X('mean', axis=alt.Axis(title=xlabel)),
        y=alt.Y('slot', title=ylabel),
        color=alt.Color('dht', legend=alt.Legend(title="DHT", orient="bottom")),
        tooltip=['mean', 'ci95_lo', 'ci95_hi'],
    ).properties(width=800, height=600)
    ci = alt.Chart(df).mark_errorbar(ticks=True, size=5).encode(
        x=alt.X('ci95_lo', axis=alt.Axis(title="")),
        x2='ci95_hi',
        y='slot',
        color=alt.Color('dht', legend=None),
        tooltip=['mean', 'ci95_hi', 'ci95_lo'],
    ).properties(width=800, height=600)
    return points, ci
