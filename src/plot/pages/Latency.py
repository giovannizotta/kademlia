import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from plot.data import get_client_requests, get_client_timeout
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

    client_timeout = DEFAULT_PEER_TIMEOUT * CLIENT_TIMEOUT_MULTIPLIER
    st.markdown(f"Client arrival rate: {clientrate}")

    cdfs = list()
    healthy = list()
    for dht in CONF.get("dht"):
        conf = {
            "rate": clientrate,
            "joinrate": joinrate,
            "crashrate": crashrate,
            "nkeys": nkeys,
            "dht": dht,
        }
        df = get_client_requests(conf)
        to = get_client_timeout(conf)

        x = df['latency'].sort_values(ignore_index=True)
        timeouts = to['time'].count()

        print(f"Timeouts: {timeouts}")
        x = x.append(pd.Series([client_timeout] * timeouts), ignore_index=True)
        y = np.arange(len(x)) / len(x)

        alt_chart = alt.Chart(pd.DataFrame({"Latency": x, "y": y, "dht": dht})).mark_line().encode(
            x=alt.X('Latency', axis=alt.Axis(title="Latency")),
            y=alt.Y('y', title="CDF"),
            color=alt.Color('dht', legend=alt.Legend(title="DHT")),
            tooltip=['Latency', 'y'],
        )
        # alt_chart = alt_chart.encode(tooltip=['x', 'y'])
        cdfs.append(alt_chart)

        healthy.extend(get_healthy_chart(conf))

    layers = alt.layer(*cdfs)
    st.altair_chart(layers, use_container_width=True)

    layers = alt.layer(*healthy)
    st.altair_chart(layers, use_container_width=True)


if __name__ == "__main__":
    main()
