from common.client import Client
from common.net_manager import NetManager
from common.node import DHTNode
from common.utils import *


@dataclass
class Simulator(Loggable):
    net_manager: NetManager
    keys: Sequence[str]
    plot: bool
    max_value: int = 10 ** 9
    mean_arrival: float = 0.1
    mean_crash: float = 50
    mean_join: float = 50
    ext: str = "pdf"
    capacity: int = 100

    FIND: ClassVar[str] = "FIND"
    STORE: ClassVar[str] = "STORE"
    KAD: ClassVar[str] = "KAD"
    CHORD: ClassVar[str] = "CHORD"
    CLIENT_ACTIONS: ClassVar[Tuple[str, str]] = (FIND, STORE)
    DHT: ClassVar[Tuple[str, str]] = (KAD, CHORD)

    def __post_init__(self) -> None:
        super().__post_init__()
        self.id = -1

    def get_arrival_time(self) -> float:
        return self.rbg.get_exponential(self.mean_arrival)

    def get_crash_time(self) -> float:
        return self.rbg.get_exponential(self.mean_crash)

    def get_join_time(self) -> float:
        return self.rbg.get_exponential(self.mean_join)

    def get_random_node(self) -> DHTNode:
        n_id = self.rbg.get_from_range(len(self.net_manager.nodes))
        return self.net_manager.nodes[n_id]

    def get_random_action(self) -> str:
        a_id = self.rbg.get_from_range(2)
        return Simulator.CLIENT_ACTIONS[a_id]

    def get_random_key(self) -> str:
        k_id = self.rbg.get_from_range(len(self.keys))
        return self.keys[k_id]

    def build_network(self) -> SimpyProcess[None]:
        for i in range(2, len(self.net_manager.nodes)):
            self.log(f"node {i} trying to join")
            yield self.env.process(
                self.net_manager.nodes[i].join_network(
                    self.net_manager.nodes[self.rbg.get_from_range(i)])
            )
            self.log(f"node {i} joined")
        self.log("All nodes joined", level=logging.INFO)

    def get_client_behaviour(self, client: Client) -> SimpyProcess[None]:
        """Get random client action (find or store a key/value pair)"""
        action = self.get_random_action()
        key = self.get_random_key()
        # send it to a random node
        # node = self.get_random_node()
        ask_to = self.get_random_node()
        if action == Simulator.STORE:
            return client.find_value(ask_to, key)
        else:
            value = self.rbg.get_from_range(self.max_value)
            return client.store_value(ask_to, key, value)

    def simulate_join(self) -> SimpyProcess[None]:
        """Simulate the join process of the network"""
        yield from self.build_network()

        yield from self.net_manager.prepare_updates()

        # yield simpy.AllOf(self.env, updates)
        self.log(f"Updates are done for all nodes.", level=logging.INFO)

        if self.plot:
            self.net_manager.print_network(
                self.net_manager.nodes[10], self.ext)

    def change_env(self, env: simpy.Environment) -> None:
        self.env = env
        self.net_manager.change_env(env)

    def simulate_clients(self) -> SimpyProcess[None]:
        i = 0
        while True:
            # generate request after random time
            t = self.get_arrival_time()
            yield self.env.timeout(t)
            client_name = f"client_{i:05d}"
            client = Client(self.env,
                            client_name,
                            self.net_manager.datacollector,
                            log_world_size=self.net_manager.nodes[0].log_world_size)
            proc = self.get_client_behaviour(client)
            self.env.process(proc)
            i += 1

    def simulate_crashes(self) -> SimpyProcess[None]:
        while True:
            # generate crashes on random exponential time
            t = self.get_crash_time()
            yield self.env.timeout(t)
            self.net_manager.crash_next()

    def simulate_joins(self) -> SimpyProcess[None]:
        while True:
            # generate joining requests on random exponential time
            t = self.get_join_time()
            yield self.env.timeout(t)
            self.net_manager.join_next()
