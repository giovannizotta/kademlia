import numpy as np
import streamlit as st
import altair as alt

from plot.data import get_queue_load, get_crash_time, get_join_time, get_slots_with_ci, get_ecdf_ci_horizontal
from plot.healthy import get_healthy_chart
from plot.options import select_slider, time_slider
from simulation.campaigns import CONF
from simulation.constants import DEFAULT_MAX_TIME


def main():
    st.title("Average queue load in the network")

    start_time, end_time = time_slider(DEFAULT_MAX_TIME)
    clientrate = select_slider(CONF.get("rate"), "client arrival rate")
    joinrate = select_slider(CONF.get("joinrate"), "join rate")
    crashrate = select_slider(CONF.get("crashrate"), "crash rate")
    nkeys = select_slider(CONF.get("nkeys"), "number of keys")

    barplots = list()
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
        df = get_queue_load(conf)

        # max_time = DEFAULT_MAX_TIME
        # bars = alt.Chart(df, title=f"Average queue load, simulation time: {max_time}") \
        #     .mark_bar().encode(
        #     x=alt.X('node', axis=alt.Axis(title="Node")),
        #     y=alt.Y('avg_load', title="Load"),
        #     color=alt.Color('dht', legend=alt.Legend(title="DHT")),
        #     tooltip=['avg_load', 'count_load', 'node', 'time_join', 'time_crash'],
        # )
        barplots.append(get_loads_cdf(df, dht))
        healthy.append(get_healthy_chart(conf, start_time, end_time, dht))

    layers = alt.layer(*barplots)
    st.altair_chart(layers, use_container_width=True)

    st.markdown("Number of active nodes in the network over time")
    layers = alt.layer(*healthy)
    st.altair_chart(layers, use_container_width=True)


def get_loads_cdf(df, dht):
    df = df.sort_values(by=["seed", "node", "time"], ignore_index=True)
    df["time_gap"] = df.groupby(["seed", "node"])["time"].diff().reset_index(drop=True)
    df = df[df["time_gap"].notna()]
    # fill value != 0?
    df["load"] = df.groupby(["seed", "node"])["load"].shift(1, fill_value=0)
    print(df)

    weighted_average = lambda x: np.average(x, weights=df.loc[x.index, "time_gap"])
    df = df.groupby(["seed", "node"]).agg(
        load=("load", weighted_average),
    ).reset_index()
    df["dht"] = dht

    df = df.sort_values(by="load", ignore_index=True)
    df["count"] = df.groupby("seed").cumcount()

    df = get_slots_with_ci(df, "count", "load", "mean", nslots=20)
    df["dht"] = dht
    max = df["slot"].max()
    df["slot"] /= max

    line, ci = get_ecdf_ci_horizontal(df, "Load", "CDF")

    return line + ci


if __name__ == "__main__":
    main()
