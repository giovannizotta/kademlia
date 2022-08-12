import altair as alt
import pandas as pd
import streamlit as st

from plot.data import get_client_requests, get_client_timeout, get_slots_with_ci, get_line_ci_chart
from plot.healthy import get_healthy_chart
from plot.options import option_select_slider
from simulation.campaigns import CONF
from simulation.constants import DEFAULT_PEER_TIMEOUT, CLIENT_TIMEOUT_MULTIPLIER


def main():
    st.markdown("Client latency")

    clientrate = option_select_slider(CONF.get("rate"), "client arrival rate")
    joinrate = option_select_slider(CONF.get("joinrate"), "join rate")
    crashrate = option_select_slider(CONF.get("crashrate"), "crash rate")
    nkeys = option_select_slider(CONF.get("nkeys"), "number of keys")

    st.markdown(f"Client arrival rate: {clientrate}")

    cdfs = list()
    healthy = list()
    print("==========")
    for dht in CONF.get("dht"):
        conf = {
            "seed": CONF.get("seed"),
            "rate": clientrate,
            "joinrate": joinrate,
            "crashrate": crashrate,
            "nkeys": nkeys,
            "dht": dht,
        }
        cdfs.append(get_latency_cdf(conf))
        healthy.append(get_healthy_chart(conf))

    layers = alt.layer(*cdfs)
    st.altair_chart(layers, use_container_width=True)

    layers = alt.layer(*healthy)
    st.altair_chart(layers, use_container_width=True)


def get_latency_cdf(conf):
    dht = conf.get("dht")
    df = get_client_requests(conf)
    to = get_client_timeout(conf)

    to["latency"] = DEFAULT_PEER_TIMEOUT * CLIENT_TIMEOUT_MULTIPLIER
    to["hops"] = -1
    df = pd.concat([df, to], ignore_index=True)

    df = df.sort_values(by="latency", ignore_index=True)
    df["count"] = df.groupby("seed").cumcount()

    df = get_slots_with_ci(df, "latency", "count", "max")
    df["dht"] = dht
    max = df["mean"].max()
    df["mean"] /= max
    df["ci95_lo"] /= max
    df["ci95_hi"] /= max

    line, ci = get_line_ci_chart(df, "Latency", "CDF")

    return line + ci


if __name__ == "__main__":
    main()
