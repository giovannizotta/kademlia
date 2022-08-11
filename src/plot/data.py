import json

import pandas as pd
from runexpy.campaign import Campaign
from runexpy.utils import IterParamsT

from common.collector import DataCollector

import streamlit as st


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


# @st.experimental_memo
def get_data() -> pd.DataFrame:
    c = Campaign.load('campaigns/experiment')
    result = list()
    for campaign_results, files in c.get_results_for(CONF):
        with open(files["data.json"]) as f:
            dct = json.load(f)
        simulation_outputs = DataCollector.from_dict(dct).to_pandas()
        entry = campaign_results.params
        entry.update(simulation_outputs)
        result.append(entry)
        print("Rate: {}, client_requests: {}".format(entry["rate"], len(simulation_outputs["client_requests"])))
    df = pd.DataFrame(result)
    return df
