from __future__ import annotations
from common.utils import *
from abc import abstractmethod
from dataclasses import dataclass, field
import hashlib
from bitstring import BitArray
from collections import defaultdict
from enum import Enum

class PacketType(Enum):
    FIND_NODE = 1
    SET_PRED = 2
    SET_SUCC = 3
    ASK_SUCC = 4
    FIND_VALUE_REPLY = 5
    STORE_VALUE_REPLY = 6
    GET_NODE = 7
    GET_NODE_REPLY = 8
    FIND_VALUE = 9
    STORE_VALUE = 10
    GET_VALUE = 11
    SET_VALUE = 12
    GET_VALUE_REPLY = 13
    SET_VALUE_REPLY = 14

    @staticmethod
    def is_reply(packet: Packet):
        reply_types = [PacketType.FIND_VALUE_REPLY, PacketType.GET_NODE_REPLY, PacketType.SET_VALUE_REPLY, PacketType.STORE_VALUE_REPLY]
        return packet.ptype in reply_types

@dataclass
class Packet():
    ptype: PacketType
    id: int = field(init=False)
    data: Dict = field(default_factory=dict)
    sender: Optional[Node] = field(init=False, default=None)
    event: Optional[simpy.Event] = field(default=None)

    instances: ClassVar[int] = 0

    def __post_init__(self) -> None:
        self.id = Packet.instances
        Packet.instances += 1

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
    in_queue: simpy.Resource = field(init=False, repr=False)
    queue_capacity: int = field(repr=False, default=100)
    mean_service_time: float = field(repr=False, default=0.1)

    @abstractmethod
    def __post_init__(self) -> None:
        super().__post_init__()
        self.id = Node._compute_key(self.name, self.log_world_size)
        self.in_queue = simpy.Resource(self.env, capacity=1)

    @abstractmethod
    def manage_packet(self, packet: Packet):
        if(PacketType.is_reply(packet)):
            assert packet.event is not None
            packet.event.succeed(value=packet)

    def recv_packet(self, packet: Packet):
        # Manage network buffer and call manage_packet
        if len(self.in_queue.queue) == self.queue_capacity:
            return

        with self.in_queue.request() as res:
            self.datacollector.queue_load[self.name].append(
                (self.env.now, len(self.in_queue.queue)))
            self.log("Trying to acquire queue")
            yield res
            self.log("Queue acquired")
            service_time = self.rbg.get_exponential(self.mean_service_time)
            yield self.env.timeout(service_time)

        self.log("Queue released")
        self.manage_packet(packet)
        self.datacollector.queue_load[self.name].append(
            (self.env.now, len(self.in_queue.queue)))

    def new_req(self) -> Request:
        """Generate a new request event"""
        return Request(self.env.event())

    def _transmit(self) -> SimpyProcess[None]:
        """Wait for the transmission delay of a message"""
        transmission_time = self.rbg.get_exponential(
            self.mean_transmission_delay)
        transmission_delay = self.env.timeout(transmission_time)
        yield transmission_delay

    def _send_msg(self, dest: Node, packet: Packet) -> SimpyProcess[None]:
        """Send a packet after waiting for the transmission time

        Args:
            answer_method ([SimpyProcess]): the process to run
            packet (Packet): the packet to send
        """
        assert packet.sender is None
        packet.sender = self
        self.log(f"sending packet {packet}...")
        yield from self._transmit()

        yield self.env.process(dest.recv_packet(packet))

    def send_req(self, dest: Node, packet: Packet) -> Request:
        """Send a packet and bind it to a callback

        Args:
            answer_method (SimpyProcess): the callback method to be called by the receiver
            packet (Packet): the packet to be sent

        Returns:
            Request: the request event that will be triggered by the receiver when it is done
        """
        sent_req = self.new_req()
        packet.event = sent_req
        self.env.process(self._send_msg(dest, packet))
        return sent_req

    def send_resp(self, dest: Node, packet: Packet) -> None:
        """Send the response to an event

        Args:
            recv_req (Request): the event to be processed
            packet (Packet): the packet to send back
        """
        assert packet.event is not None
        self.log(f"sending response...")
        self.env.process(self._send_msg(dest, packet))

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

    @abstractmethod
    def __post_init__(self) -> None:
        super().__post_init__()
        self.ht = dict()
    
    @abstractmethod
    def manage_packet(self, packet: Packet):
        super().manage_packet(packet) 
        if packet.ptype == PacketType.GET_NODE:
            self.get_node(packet)
        elif packet.ptype == PacketType.FIND_VALUE:
            self.find_value(packet)
        elif packet.ptype == PacketType.STORE_VALUE:
            self.store_value(packet)
        elif packet.ptype == PacketType.SET_VALUE:
            self.set_value(packet)
        elif packet.ptype == PacketType.GET_VALUE:
            self.get_value(packet)

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
    def get_node(
        self,
        packet: Packet
    ) -> SimpyProcess:
        """Answer with the node(s) closest to the key among the known ones

        Args:
            packet (Packet): The packet received
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

    def get_value(self, packet: Packet) -> None:
        """Get value associated to a given key in the node's hash table

        Args:
            packet (Packet): The packet containing the asked key
            recv_req (Request): The request event to answer to
        """
        key = packet.data["key"]
        new_packet = Packet(ptype=PacketType.GET_VALUE_REPLY, data=dict(value=self.ht.get(key)), event=packet.event)
        self.send_resp(packet.sender, new_packet)

    def set_value(self, packet: Packet) -> None:
        """Set the value to be associated to a given key in the node's hash table

        Args:
            packet (Packet): The packet containing the asked key
            recv_req (Request): The request event to answer to
        """
        key = packet.data["key"]
        self.ht[key] = packet.data["value"]
        new_packet = Packet(ptype=PacketType.SET_VALUE_REPLY, event=packet.event)
        self.send_resp(packet.sender, new_packet)

    @abstractmethod
    def find_value(self, packet: Packet) -> SimpyProcess[None]:
        """Iteratively find the value associated to a given key

        Args:
            packet (Packet): The packet containing the asked key
            recv_req (Request): The request event to answer to
        """
        pass

    @abstractmethod
    def store_value(self, packet: Packet) -> SimpyProcess[None]:
        """Iteratively store the given value associated to the given key

        Args:
            packet (Packet): The packet containing the key and the value
            recv_req (Request): The request event to answer to
        """
        pass
