from __future__ import annotations
from common.utils import *
from abc import abstractmethod
from dataclasses import dataclass, field
import hashlib
from bitstring import BitArray
from collections.abc import Callable
from collections import defaultdict


@dataclass
class Packet():
    id: int = field(init=False)
    data: Dict = field(default_factory=dict)
    sender: Node = field(init=False)

    instances: ClassVar[int] = 0

    def __post_init__(self) -> None:
        self.id = Packet.instances
        Packet.instances += 1

def packet_service(
    operation: Method[T]
) -> Method[SimpyProcess[T]]:
    """Wait for the Node's resource, perform the operation and wait a random service time."""

    def wrapper(self: DHTNode, *args: Any) -> SimpyProcess[T]:
        with self.in_queue.request() as res:
            self.datacollector.queue_load[self.name].append(
                (self.env.now, len(self.in_queue.queue)))
            self.log("Trying to acquire queue")
            yield res
            self.log("Queue acquired")
            ans = operation(self, *args)
            service_time = self.rbg.get_exponential(self.mean_service_time)
            yield self.env.timeout(service_time)

        self.log("Queue released")
        self.datacollector.queue_load[self.name].append(
            (self.env.now, len(self.in_queue.queue)))
        return ans
    return wrapper


@dataclass
class DataCollector:
    """Collect the data from the simulation"""
    timed_out_requests: int = 0
    client_requests: List[Tuple[float, int]] = field(default_factory=list)
    queue_load: Dict[str, List[Tuple[float, int]]] = field(
        default_factory=lambda: defaultdict(list))

    def clear(self):
        self.timed_out_requests = 0
        self.client_requests.clear()
        self.queue_load.clear()

    def to_dict(self):
        return self.__dict__

    @classmethod
    def from_dict(self, dct):
        return DataCollector(**dct)


@dataclass
class Node(Loggable):
    """Network node"""
    datacollector: DataCollector = field(repr=False)
    max_timeout: float = field(repr=False, default=50.0)
    log_world_size: int = field(repr=False, default=10)
    mean_transmission_delay: float = field(repr=False, default=0.5)

    @abstractmethod
    def __post_init__(self) -> None:
        super().__post_init__()
        self.id = Node._compute_key(self.name, self.log_world_size)

    def new_req(self) -> Request:
        """Generate a new request event"""
        return Request(self.env.event())

    def _transmit(self) -> SimpyProcess[None]:
        """Wait for the transmission delay of a message"""
        transmission_time = self.rbg.get_exponential(
            self.mean_transmission_delay)
        transmission_delay = self.env.timeout(transmission_time)
        yield transmission_delay

    def _req(self, answer_method: Callable[..., SimpyProcess[None]], packet: Packet, sent_req: Request) -> SimpyProcess[None]:
        """Send a packet after waiting for the transmission time

        Args:
            answer_method ([SimpyProcess]): the process to run
            packet (Packet): the packet to send
            sent_req (Request): the event to be triggered when done with the process
        """
        packet.sender = self
        self.log(f"sending packet {packet}...")
        yield from self._transmit()
        yield self.env.process(answer_method(packet, sent_req))

    def send_req(self, answer_method: Callable[..., SimpyProcess[None]], packet: Packet) -> Request:
        """Send a packet and bind it to a callback

        Args:
            answer_method (SimpyProcess): the callback method to be called by the receiver
            packet (Packet): the packet to be sent

        Returns:
            Request: the request event that will be triggered by the receiver when it is done
        """
        sent_req = self.new_req()
        # transmission time
        self.env.process(self._req(answer_method, packet, sent_req))
        return sent_req

    def wait_resps(self, sent_reqs: Sequence[Request], packets: List[Packet]) -> SimpyProcess[None]:
        """Wait for the responses of the recipients

        Args:
            sent_reqs (Sequence[Request]): the requests associated to the wait event
            packets (List[Packet]): the list filled with packets received within the timeout

        Raises:
            DHTTimeoutError: if at least one response times out

        Returns:
            List[Packet]: list of packets received
        """
        sent_req = self.env.all_of(sent_reqs)
        timeout = self.env.timeout(self.max_timeout)
        wait_event = self.env.any_of((timeout, sent_req))
        ans = yield wait_event
        timeout_found = False
        packets_received = 0
        for event, ret_val in ans.items():
            if event is timeout:
                timeout_found = True
            else:
                assert isinstance(ret_val, Packet)
                packets.append(ret_val)
                packets_received += 1
        self.log(f"received {packets_received}/{len(sent_reqs)} response")
        if timeout_found:
            self.log("Some responses timed out", level=logging.WARNING)
            raise DHTTimeoutError()

    def wait_resp(self, sent_req: Request) -> SimpyProcess[Packet]:
        """Wait for the response of the recipient (see wait_resps)"""
        ans: List[Packet] = []
        yield from self.wait_resps([sent_req], ans)
        return ans.pop()

    def _resp(self, recv_req: Request, packet: Packet) -> SimpyProcess[None]:
        """Trigger the event of reception after waiting for the transmission delay

        Args:
            recv_req (Request): the reception event that has to be triggered
            packet (Packet): the packet to send
        """
        packet.sender = self
        yield from self._transmit()
        recv_req.succeed(value=packet)

    def send_resp(self, recv_req: Request, packet: Packet) -> None:
        """Send the response to an event

        Args:
            recv_req (Request): the event to be processed
            packet (Packet): the packet to send back
        """
        self.log(f"sending response...")
        self.env.process(self._resp(recv_req, packet))

    @staticmethod
    def _compute_key(key_str: str, log_world_size: int) -> int:
        digest = hashlib.sha256(bytes(key_str, "utf-8")).hexdigest()
        bindigest = BitArray(hex=digest).bin
        subbin = bindigest[:log_world_size]
        return BitArray(bin=subbin).uint


@dataclass
class DHTNode(Node):
    mean_service_time: float = field(repr=False, default=0.1)

    ht: Dict[int, Any] = field(init=False, repr=False)
    in_queue: simpy.Resource = field(init=False, repr=False)

    @abstractmethod
    def __post_init__(self) -> None:
        super().__post_init__()
        self.ht = dict()
        self.in_queue = simpy.Resource(self.env, capacity=1)

    def change_env(self, env: simpy.Environment) -> None:
        self.env = env
        self.in_queue = simpy.Resource(self.env, capacity=1)

    @abstractmethod
    def find_node(
        self,
        key: int,
        ask_to: Optional[DHTNode] = None
    ) -> SimpyProcess[Optional[DHTNode]]:
        """Iteratively find the closest node(s) to the given key

        Args:
            key (int): The key
            ask_to (Optional[DHTNode], optional): If given, this is the first node that is contacted. Defaults to None.
        """
        pass

    @abstractmethod
    def on_find_node_request(
        self,
        packet: Packet,
        recv_req: Request
    ) -> SimpyProcess:
        """Answer with the node(s) closest to the key among the known ones

        Args:
            packet (Packet): The packet received
            recv_req (Request): The request to answer to
        """
        pass

    @abstractmethod
    def join_network(self, to: DHTNode) -> SimpyProcess:
        """Send necessary requests to join the network

        Args:
            to (DHTNode): The node to contact first
        """
        pass

    @classmethod
    @abstractmethod
    def _compute_distance(cls, key1: int, key2: int, log_world_size: int) -> int:
        pass

    def get_value(self, packet: Packet, recv_req: Request) -> None:
        """Get value associated to a given key in the node's hash table

        Args:
            packet (Packet): The packet containing the asked key
            recv_req (Request): The request event to answer to
        """
        key = packet.data["key"]
        packet.data["value"] = self.ht.get(key)
        self.send_resp(recv_req, packet)

    def set_value(self, packet: Packet, recv_req: Request) -> None:
        """Set the value to be associated to a given key in the node's hash table

        Args:
            packet (Packet): The packet containing the asked key
            recv_req (Request): The request event to answer to
        """
        key = packet.data["key"]
        self.ht[key] = packet.data["value"]
        self.send_resp(recv_req, packet)

    @abstractmethod
    def find_value(self, packet: Packet, recv_req: Request) -> SimpyProcess[None]:
        """Iteratively find the value associated to a given key

        Args:
            packet (Packet): The packet containing the asked key
            recv_req (Request): The request event to answer to
        """
        pass

    @abstractmethod
    def store_value(self, packet: Packet, recv_req: Request) -> SimpyProcess[None]:
        """Iteratively store the given value associated to the given key

        Args:
            packet (Packet): The packet containing the key and the value
            recv_req (Request): The request event to answer to
        """
        pass
