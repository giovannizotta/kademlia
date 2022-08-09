import json

import pandas as pd
from runexpy.campaign import Campaign

from common.collector import DataCollector

from simulation.campaigns import CONF

import streamlit as st

# @st.experimental_memo
def get_client_requests() -> pd.DataFrame:
    c = Campaign.load('campaigns/experiment')
    result = list()
    for campaign_results, files in c.get_results_for(CONF):
        with open(files["data.json"]) as f:
            dct = json.load(f)
        tmp = pd.DataFrame(dct["client_requests"], columns=["latency", "hops"])
        for k, v in campaign_results.params.items():
            tmp[k] = v
        result.append(tmp)
    df = pd.concat(result)
    return df

def get_client_timeout() -> pd.DataFrame:
    c = Campaign.load('campaigns/experiment')
    result = list()
    for campaign_results, files in c.get_results_for(CONF):
        with open(files["data.json"]) as f:
            dct = json.load(f)
        tmp = {"client_timeout": dct["timed_out_requests"]}
        for k, v in campaign_results.params.items():
            tmp[k] = v
        result.append(tmp)
    df = pd.DataFrame(result)
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
