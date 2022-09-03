import altair as alt
import pandas as pd
import streamlit as st

from plot.data import get_client_requests, get_client_timeout, get_slots_with_ci, get_line_ci_chart, \
    get_ecdf_ci_horizontal, get_join_time, get_crash_time
from plot.healthy import get_healthy_chart, get_healthy_nodes
from plot.options import select_slider, time_slider
from simulation.campaigns import CONF
from simulation.constants import DEFAULT_PEER_TIMEOUT, CLIENT_TIMEOUT_MULTIPLIER, DEFAULT_MAX_TIME


def main():
    st.title("Latency experienced by clients")

    start_time, end_time = time_slider(DEFAULT_MAX_TIME)
    clientrate = select_slider(CONF.get("rate"), "client arrival rate")
    joinrate = select_slider(CONF.get("joinrate"), "join rate")
    crashrate = select_slider(CONF.get("crashrate"), "crash rate")
    nkeys = select_slider(CONF.get("nkeys"), "number of keys")

    st.markdown(f"Client arrival rate: {clientrate}")

    testcdfs = list()
    cdfs = list()
    healthy = list()
    for dht in CONF.get("dht"):
        conf = {
            "seed": CONF.get("seed"),
            "rate": clientrate,
            "joinrate": joinrate,
            "crashrate": crashrate,
            "nkeys": nkeys,
            "dht": dht,
        }
        dht = conf.get("dht")
        client_df = get_client_requests(conf)
        timeout_df = get_client_timeout(conf)

        client_df = client_df[(client_df["time"] >= start_time) & (client_df["time"] <= end_time)]
        timeout_df = timeout_df[(timeout_df["time"] >= start_time) & (timeout_df["time"] <= end_time)]

        cdfs.append(get_latency_ecdf(client_df, timeout_df, dht))
        testcdfs.append(get_latency_ecdf_test(client_df, timeout_df, dht))
        healthy.append(get_healthy_chart(conf, start_time, end_time, dht))

    layers = alt.layer(*cdfs)
    st.altair_chart(layers, use_container_width=True)

    layers = alt.layer(*testcdfs)
    st.altair_chart(layers, use_container_width=True)

    st.markdown("Number of active nodes in the network over time")
    layers = alt.layer(*healthy)
    st.altair_chart(layers, use_container_width=True)


def get_latency_ecdf_test(client_df: pd.DataFrame, timeout_df: pd.DataFrame, dht: str) -> alt.Chart:
    client_df = client_df[client_df["seed"] == 420]
    client_df = client_df.sort_values(by="latency", ignore_index=True)
    client_df["count"] = range(len(client_df))

    return alt.Chart(client_df).mark_point(filled=True, size=30).encode(
        x=alt.X('latency', axis=alt.Axis(title="Latency")),
        y=alt.Y('count', title="CDF"),
        color=alt.Color('dht', legend=alt.Legend(title="DHT")),
    )


def get_latency_ecdf(client_df: pd.DataFrame, timeout_df: pd.DataFrame, dht: str) -> alt.Chart:
    timeout_df["latency"] = DEFAULT_PEER_TIMEOUT * CLIENT_TIMEOUT_MULTIPLIER
    timeout_df["hops"] = -1
    client_df = pd.concat([client_df, timeout_df], ignore_index=True)
    print(f"{dht} has had {len(client_df)} successes and {len(timeout_df)} timeouts")

    client_df = client_df.sort_values(by="latency", ignore_index=True)
    client_df["count"] = client_df.groupby("seed").cumcount()

    client_df = get_slots_with_ci(client_df, "count", "latency", "mean", nslots=20)
    client_df["dht"] = dht
    max = client_df["slot"].max()
    client_df["slot"] /= max

    line, ci = get_ecdf_ci_horizontal(client_df, "Latency", "CDF")

    return line + ci


if __name__ == "__main__":
    main()
