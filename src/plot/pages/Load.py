import numpy as np
import streamlit as st
import altair as alt

from plot.data import get_queue_load, get_crash_time, get_join_time
from plot.options import option_select_slider
from simulation.campaigns import CONF


def main():
    st.markdown("Client latency")

    clientrate = option_select_slider(CONF.get("rate"), "client arrival rate")
    joinrate = option_select_slider(CONF.get("joinrate"), "join rate")
    crashrate = option_select_slider(CONF.get("crashrate"), "crash rate")
    nkeys = option_select_slider(CONF.get("nkeys"), "number of keys")

    barplots = list()
    for dht in CONF.get("dht"):
        conf = {
            "rate": clientrate,
            "joinrate": joinrate,
            "crashrate": crashrate,
            "nkeys": nkeys,
            "dht": dht,
        }
        load_df = get_queue_load(conf)
        jointime_df = get_join_time(conf)
        crashtime_df = get_crash_time(conf)

        maxtime = load_df["max-time"].iloc[0]

        load_df["time_gap"] = load_df.sort_values(["node", "time"]).groupby("node")["time"].diff().reset_index(
            drop=True)
        load_df = load_df[load_df["time_gap"].notna()]

        weighted_average = lambda x: np.average(x, weights=load_df.loc[x.index, "time_gap"])
        load_df = load_df.groupby(["node"]).agg(
            avg_load=("load", weighted_average),
            count_load=("load", "count"),
        ).reset_index()

        jointime_df = jointime_df[["node", "time"]].rename(columns={"time": "time_join"})
        load_df = load_df.merge(jointime_df, on="node")
        crashtime_df = crashtime_df[["node", "time"]].rename(columns={"time": "time_crash"})
        load_df = load_df.merge(crashtime_df, on="node", how="left")

        load_df["dht"] = dht
        bars = alt.Chart(load_df, title=f"Average queue load, simulation time: {maxtime}") \
            .mark_bar().encode(
            x=alt.X('node', axis=alt.Axis(title="Node")),
            y=alt.Y('avg_load', title="Load"),
            color=alt.Color('dht', legend=alt.Legend(title="DHT")),
            tooltip=['avg_load', 'count_load', 'node', 'time_join', 'time_crash'],
        )
        barplots.append(bars)

    layers = alt.layer(*barplots)
    st.altair_chart(layers, use_container_width=True)


if __name__ == "__main__":
    main()
