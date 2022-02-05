import pickle
import matplotlib.pyplot as plt
import numpy as np
from common.node import DHTNode, DataCollector
from common.utils import *
from common.simulator import Simulator
from argparse import ArgumentParser, Namespace
from itertools import chain
dhts = (Simulator.CHORD, Simulator.KAD)


def parse_args() -> Namespace:
    ap = ArgumentParser("Kademlia and chord simulator")
    # sp = ap.add_subparsers(dest="action")
    # kad_parser = sp.add_parser("kad", help="Kademlia")
    # chord_parser = sp.add_parser("chord", help="Chord")
    ap.add_argument("-d", "--dht",
                    choices=[Simulator.KAD, Simulator.CHORD], help="DHT to use")
    ap.add_argument("-e", "--ext", default="pdf",
                    choices=["png", "pdf"], help="File extension")
    ap.add_argument("-a", "--arrivals", action="store_true",
                    help="Compare arrivals or not")
    return ap.parse_args()


def get_heatmap(ax, delays, hops, max_delay, max_hops):
    hop_bins = [[] for _ in range(max_hops+1)]
    for delay, hop in zip(delays, hops):
        hop_bins[hop].append(delay)
    im = []
    for i in range(max_hops):
        hh, _ = np.histogram(hop_bins[i], bins=list(range(int(max_delay)+1)))
        im.append(hh)

    im_ratio = len(im) / len(im[0])
    im = np.array(im)
    print("shape:", im.shape)
    im = ax.imshow(im, origin="lower")
    # plt.show()
    cbar = ax.figure.colorbar(
        im, ax=ax, fraction=0.046*im_ratio, location="right")
    cbar.ax.set_ylabel("Density", rotation=-90, va="bottom")


def get_data(rate=0):
    data = dict()
    max_hops = 0
    max_delay = 0
    max_time = 0
    for dht in dhts:
        if rate == 0:
            pick = open(f"{dht}.data", "rb")
        else:
            pick = open(f"{dht}_{rate}.data", "rb")
        dht_data: DataCollector = pickle.load(pick)
        data[dht] = dht_data
        delays, hops = zip(*data[dht].client_requests)
        t, _ = zip(*list(data[dht].queue_load.values())[0])
        max_time = max(max_time, max(t))
        max_hops = max(max_hops, max(hops))
        max_delay = max(max_delay, np.quantile(delays, 0.99))
    return data, max_hops, max_delay, max_time


def plot_comparison(data, max_hops, ext):
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(6, 12))
    for dht in dhts:
        print(
            f"{dht} has had {data[dht].timed_out_requests} timeouts and {len(data[dht].client_requests)} successful requests")
        DHT_delays, DHT_hops = zip(*data[dht].client_requests)
        ax1.hist(DHT_delays, bins=30,
                 label=f"{dht} delays", alpha=0.6, density=True)
        ax2.hist(DHT_hops, bins=list(range(max_hops + 1)),
                 label=f"{dht} hops", alpha=0.6, density=True)

    ax1.set_title("Client request delays")
    ax2.set_title("Number of hops needed to fulfill a client request")
    ax1.set_xlabel("Client waiting time (s)")
    ax2.set_xlabel("Number of hops")
    ax1.set_ylabel("Estimated probability")
    ax2.set_ylabel("Estimated probability")
    ax1.legend()
    ax2.legend()
    plt.savefig(f"delay_hops_comparison.{ext}",
                format=ext, bbox_inches="tight")


def plot_heatmap(data, max_hops, max_delay, ext):
    fig, (ax3, ax4) = plt.subplots(2, 1, figsize=(6, 10))

    for ax, dht in zip((ax3, ax4), dhts):
        #ax3.scatter(DHT_delays, DHT_hops, label=f"{dht}", alpha=0.6)
        DHT_delays, DHT_hops = zip(*data[dht].client_requests)
        ax.set_title(f"Correlation between delay and hops for {dht}")
        ax.set_ylabel("Number of hops")
        ax.set_xlabel("Client waiting time (s)")
        get_heatmap(ax, DHT_delays, DHT_hops, max_delay, max_hops)

    plt.savefig(f"heatmap.{ext}", format=ext, bbox_inches="tight")


def plot_single_node(data: Dict[str, DataCollector], ext: str):
    node = "node_00047"

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(6, 8))
    for dht in dhts:

        previous_load = 0
        time = 0
        sum = 0
        data_points = []
        times = []
        for t, load in data[dht].queue_load[node]:
            sum += previous_load * (t - time)
            previous_load = load
            time = t
            times.append(t)
            data_points.append(sum/time)
        ax1.plot(times, data_points, label=f"{dht}")
        ax1.set_xlabel("Time (s)")
        ax1.set_ylabel("Average load")
        ax1.set_title("Load of a random DHT node")

        _, load = zip(*data[dht].queue_load[node])
        ax2.hist(load, alpha=0.6, bins=10)
        ax2.set_xlabel("Time (s)")
        ax2.set_ylabel("Average load")
        ax2.set_title("Distribution of load in DHT node")

    ax1.legend()
    # ax2.legend()
    plt.savefig(f"node_load.{ext}", format=ext, bbox_inches="tight")


def get_avg_load(load: List[Tuple[float, int]], until: float = np.inf) -> float:
    previous_load = 0
    time = 0
    load_integral = 0
    for t, l in load:
        if t > until:
            break
        load_integral += previous_load * (t - time)
        previous_load = l
        time = t
    if time == 0:
        return 0
    return load_integral / time


def get_load_dist(data, dht, time):
    loads = []
    for _, load in data[dht].queue_load.items():
        avg_load= get_avg_load(load, until=time)
        loads.append(avg_load)
    loads = np.array(loads)
    loads = loads[loads < np.quantile(loads, 0.95)]

    return loads

def plot_load_distrib(data: Dict[str, DataCollector], ext, max_time: float):
    fig, axes = plt.subplots(4, 2, figsize=(10, 14), sharex=True, sharey=True)
    for ax, time in zip(chain(*axes), np.linspace(1, max_time, num=9)[1:]):
        time = round(time)
        for dht in dhts:
            loads = get_load_dist(data, dht, time)
            # hh, bb = np.histogram(loads, bins=20)
            # print(hh[-10:], bb[-10:])
            ax.hist(loads, bins=10, alpha=0.6, label=dht, density=True)
        ax.set_xlabel("Average queue load")
        ax.set_ylabel("Density")
        ax.set_title(f"Until {time} (s)")
        ax.legend()

    fig.suptitle("Average node load distribution")
    plt.savefig(f"load_distr.{ext}", format=ext)

def plot_arrival_comparison(ext):
    fig, axes = plt.subplots(2, 2, figsize=(10, 10))
    for ax, rate in zip(chain(*axes), (0.01, 0.02, 0.05, 0.1)):
        data, _, _, max_time = get_data(rate=rate)
        time = round(max_time)
        for dht in dhts:
            loads = get_load_dist(data, dht, time)
            total_reqs = data[dht].timed_out_requests + len(data[dht].client_requests)
            labl = f"{dht}, timeouts: {data[dht].timed_out_requests} / {total_reqs}"
            ax.hist(loads, bins=10, alpha=0.6, label=labl, density=True)
        ax.set_xlabel("Average queue load")
        ax.set_ylabel("Density")
        ax.set_title(f"Arrival rate: {rate}")
        ax.legend()

    fig.suptitle("Average load distribution with different arrival rates")
    plt.savefig(f"arrivals.{ext}", format=ext)


def main():
    args = parse_args()
    if not args.arrivals:
        data, max_hops, max_delay, max_time= get_data()
        plot_comparison(data, max_hops, args.ext)
        plt.clf()
        plot_heatmap(data, max_hops, max_delay, args.ext)
        plt.clf()
        plot_single_node(data, args.ext)
        plt.clf()
        plot_load_distrib(data, args.ext, max_time)
        plt.clf()
    else:
        plot_arrival_comparison(args.ext)
            


if __name__ == "__main__":
    main()
