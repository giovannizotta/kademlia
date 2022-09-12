import numpy as np

from plot.data import *
from plot.options import time_slider, select_slider
from simulation.constants import DEFAULT_PEER_TIMEOUT, CLIENT_TIMEOUT_MULTIPLIER, DEFAULT_MAX_TIME


def plots(conf: IterParamsT):
    st.title("Latency experienced by clients")

    start_time, end_time = time_slider(DEFAULT_MAX_TIME)
    clientrate = select_slider(conf.get("rate"), "client arrival rate")
    joinrate = select_slider(conf.get("joinrate"), "join rate")
    crashrate = select_slider(conf.get("crashrate"), "crash rate")
    nkeys = select_slider(conf.get("nkeys"), "number of keys")

    st.markdown(f"Client arrival rate: {clientrate}")

    latency_ecdfs = list()
    load_ecdfs = list()
    active_over_time = list()
    latency_over_time = list()
    hit_rates_over_time = list()
    for dht in conf.get("dht"):
        conf = {
            "seed": conf.get("seed"),
            "rate": clientrate,
            "joinrate": joinrate,
            "crashrate": crashrate,
            "nkeys": nkeys,
            "dht": dht,
        }
        dht = conf.get("dht")
        client_df = get_client_requests(conf)
        timeout_df = get_client_timeout(conf)
        find_df = get_found_values(conf)
        store_df = get_stored_values(conf)
        load_df = get_queue_load(conf)
        if joinrate > 0 or crashrate > 0:
            active_df = get_active_df(conf)
            active_df = active_df[(active_df["time"] >= start_time) & (active_df["time"] <= end_time)]
            active_over_time.append(get_active_chart(active_df, dht))

        client_df = client_df[(client_df["time"] >= start_time) & (client_df["time"] <= end_time)]
        timeout_df = timeout_df[(timeout_df["time"] >= start_time) & (timeout_df["time"] <= end_time)]
        find_df = find_df[(find_df["time"] >= start_time) & (find_df["time"] <= end_time)]

        latency_ecdfs.append(get_latency_ecdf(client_df, timeout_df, dht))
        latency_over_time.append(get_latency_over_time(client_df, timeout_df, dht))
        load_ecdfs.append(get_loads_ecdf(load_df, dht))
        hit_rates_over_time.append(get_hit_rate_chart(find_df, store_df, dht))

    plot(latency_ecdfs, "Latency ECDF")

    plot(latency_over_time, "Latency over time")

    plot(hit_rates_over_time, "Hit rate over time")

    plot(load_ecdfs, "Load ECDF")

    if joinrate > 0 or crashrate > 0:
        plot(active_over_time, "Active nodes over time")


def plot(data, title):
    st.markdown(f"## {title}")
    layers = alt.layer(*data)
    layers.save(f"plots/{title}.svg")
    st.altair_chart(layers, use_container_width=True)


def get_latency_over_time(client_df: pd.DataFrame, timeout_df: pd.DataFrame, dht: str) -> alt.Chart:
    timeout_df["latency"] = DEFAULT_PEER_TIMEOUT * CLIENT_TIMEOUT_MULTIPLIER
    timeout_df["hops"] = -1
    client_df = pd.concat([client_df, timeout_df], ignore_index=True)

    client_df = client_df.sort_values(by="latency", ignore_index=True)

    client_df = get_slots_with_ci(client_df, "time", "latency", "mean", nslots=100)
    client_df["dht"] = dht

    line, ci = get_line_ci_chart(client_df, "Time", "Latency")

    return line + ci


def get_latency_ecdf(client_df: pd.DataFrame, timeout_df: pd.DataFrame, dht: str) -> alt.Chart:
    timeout_df["latency"] = DEFAULT_PEER_TIMEOUT * CLIENT_TIMEOUT_MULTIPLIER
    timeout_df["hops"] = -1
    client_df = pd.concat([client_df, timeout_df], ignore_index=True)

    client_df = client_df.sort_values(by="latency", ignore_index=True)
    client_df["count"] = client_df.groupby("seed").cumcount()

    client_df = get_slots_with_ci(client_df, "count", "latency", "mean", nslots=20)
    client_df["dht"] = dht
    max = client_df["slot"].max()
    client_df["slot"] /= max

    line, ci = get_ecdf_ci_horizontal(client_df, "Latency", "CDF")

    return line + ci


def get_hit_rate_chart(find_df: pd.DataFrame, store_df: pd.DataFrame, dht: str):
    find_df = find_df.sort_values(by="time", ignore_index=True)
    store_df = store_df.sort_values(by="time", ignore_index=True)
    hit_df = pd.merge_asof(find_df, store_df, on="time", by=["seed", "key"], suffixes=("_find", "_store"))

    hit_df = hit_df[(hit_df["value_store"].notna()) & (hit_df["value_find"].notna())]
    hit_df["hit"] = hit_df["value_store"] == hit_df["value_find"]
    hit_df = get_slots_with_ci(hit_df, "time", "hit", "mean", nslots=100)
    hit_df["dht"] = dht

    line, ci = get_line_ci_chart(hit_df, "Time", "Hit rate")
    return line + ci


def get_loads_ecdf(df, dht):
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


def get_active_df(conf):
    join_time_df = get_join_time(conf)
    crash_time_df = get_crash_time(conf)
    active_df = get_active_nodes(crash_time_df, join_time_df)
    return active_df


def get_active_nodes(crash_time_df, join_time_df):
    join_time_df["increment"] = 1
    crash_time_df["increment"] = -1
    df = pd.concat([join_time_df, crash_time_df], ignore_index=True).sort_values(by="time", ignore_index=True)
    df["active_nodes"] = df.groupby("seed")["increment"].cumsum()
    return df


def get_active_chart(active_df: pd.DataFrame, dht: str) -> alt.Chart:
    df = get_slots_with_ci(active_df, "time", "active_nodes", "mean")
    df["dht"] = dht

    line, ci = get_line_ci_chart(df, "Time", "Healthy nodes")
    return line + ci
