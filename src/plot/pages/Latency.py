import altair as alt
import numpy as np
import pandas as pd
import streamlit as st

from plot.data import get_client_requests, get_client_timeout


def main():
    st.markdown("Client latency")
    df = get_client_requests()
    to = get_client_timeout()

    print(to)

    for rate in df['rate'].unique():
        cdfs = list()
        st.markdown(f"Client arrival rate: {rate}")
        for dht in df['dht'].unique():
            x = df[(df['rate'] == rate) & (df['dht'] == dht)]['latency'].sort_values(ignore_index=True)
            timeouts = to[(to['rate'] == rate) & (to['dht'] == dht)]['client_timeout'].iloc[0]

            print(to[(to['rate'] == rate) & (to['dht'] == dht)]['client_timeout'])
            print(f"Timeouts: {timeouts}")
            x = x.append(pd.Series([10000] * timeouts), ignore_index=True)
            y = np.arange(len(x)) / len(x)

            alt_chart = alt.Chart(pd.DataFrame({"Latency": x, "y": y, "dht": dht})).mark_line().encode(
                x=alt.X('Latency', axis=alt.Axis(title="Latency")),
                y=alt.Y('y', title="CDF"),
                color=alt.Color('dht', legend=alt.Legend(title="DHT")),
                tooltip=['Latency', 'y'],
            )
            # alt_chart = alt_chart.encode(tooltip=['x', 'y'])
            cdfs.append(alt_chart)

        layers = alt.layer(*cdfs)
        st.altair_chart(layers, use_container_width=True)


if __name__ == "__main__":
    main()
