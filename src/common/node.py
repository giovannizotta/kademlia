from __future__ import annotations

import hashlib
import logging
from abc import abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from bitstring import BitArray
from simpy.core import Environment
from simpy.events import Process
from simpy.resources.resource import Resource

from common.collector import DataCollector
from common.packet import Message, MessageType, Packet
from common.utils import (
    DHTTimeoutError,
    LocationManager,
    Loggable,
    Request,
    SimpyProcess,
)


@dataclass
class Node(Loggable):
    """Network node"""

    datacollector: DataCollector = field(repr=False)
    location: Tuple[float, float] = field(repr=False)
    mean_service_time: float = field(repr=False, default=0.1)
    max_timeout: float = field(repr=False, default=500.0)
    log_world_size: int = field(repr=False, default=10)
    mean_transmission_delay: float = field(repr=False, default=0.5)
    in_queue: Resource = field(init=False, repr=False)
    queue_capacity: int = field(repr=False, default=100)
    crashed: bool = field(init=False, default=False)

    @abstractmethod
    def __post_init__(self) -> None:
        super().__post_init__()
        self.id = self._compute_key(self.name)
        self.in_queue = Resource(self.env, capacity=1)

    def crash(self) -> None:
        self.crashed = True
        self.datacollector.crashed_time[self.name] = self.env.now

    @abstractmethod
    def manage_packet(self, packet: Packet) -> None:
        if self.crashed:
            return
        msg = packet.message
        if msg.ptype.is_reply():
            assert msg.event is not None
            msg.event.succeed(value=packet)

    @abstractmethod
    def collect_load(self):
        pass

    def recv_packet(self, packet: Packet) -> SimpyProcess[None]:
        # Manage network buffer and call manage_packet
        self.log(f"{self.name} received {packet}")
        if len(self.in_queue.queue) == self.queue_capacity:
            self.log("Queue is full, dropping packet", level=logging.WARNING)
            return

        with self.in_queue.request() as res:
            self.collect_load()
            yield res
            service_time = self.rbg.get_exponential(self.mean_service_time)
            yield self.env.timeout(service_time)
            self.manage_packet(packet)

        self.collect_load()

    def new_req(self) -> Request:
        """Generate a new request event"""
        return Request(self.env.event())

    def _transmit(self, dest: Node) -> SimpyProcess[None]:
        """Wait for the transmission delay of a message"""
        # transmission_time = self.rbg.get_exponential(self.mean_transmission_delay)
        distance = LocationManager.distance(self.location, dest.location)
        # for now, let's assume latency is 10ms every 1000km
        # https://www.oneneck.com/blog/estimating-wan-latency-requirements/
        transmission_time = distance / 100
        transmission_delay = self.env.timeout(transmission_time)
        self.log(f"Transmission delay: {transmission_delay}")
        yield transmission_delay

    def _send_msg(self, dest: Node, msg: Message) -> SimpyProcess[None]:
        """Send a packet after waiting for the transmission time

        Args:
            dest: the node that has to receive the packet
            msg: the message to send
        """
        packet = Packet(self, msg)
        self.log(f"sending packet {packet}...")
        yield from self._transmit(dest)

        yield self.env.process(dest.recv_packet(packet))

    def send_req(self, dest: Node, msg: Message) -> Request:
        """Send a packet to a destination node

        Args:
            dest: the destination node
            msg (Message): the packet to be sent

        Returns:
            Request: the request event that will be triggered on answer
        """
        sent_req = self.new_req()
        msg.event = sent_req
        self.env.process(self._send_msg(dest, msg))
        return sent_req

    def send_resp(self, dest: Node, msg: Message) -> None:
        """Send the response to an event

        Args:
            recv_req (Request): the event to be processed
            msg (Message): the packet to send back
        """
        assert msg.event is not None
        self.log("sending response...")
        self.env.process(self._send_msg(dest, msg))

    def wait_resps(
        self, sent_reqs: Sequence[Request], packets: List[Packet]
    ) -> SimpyProcess[None]:
        """Wait for the responses of the recipients

        Args:
            sent_reqs (Sequence[Request]): the requests to wait for
            packets (List[Packet]): filled with packets received within timeout

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

    def _compute_key(self, key_str: str) -> int:
        digest = hashlib.sha256(bytes(key_str, "utf-8")).hexdigest()
        bindigest = BitArray(hex=digest).bin
        subbin = bindigest[: self.log_world_size]
        return int(BitArray(bin=subbin).uint)


@dataclass
class DHTNode(Node):
    ht: Dict[int, Any] = field(init=False, repr=False)

    @abstractmethod
    def __post_init__(self) -> None:
        super().__post_init__()
        self.ht = dict()

    @abstractmethod
    def manage_packet(self, packet: Packet) -> None:
        super().manage_packet(packet)
        msg = packet.message
        if msg.ptype == MessageType.GET_NODE:
            self.get_node(packet)
        elif msg.ptype == MessageType.FIND_VALUE:
            self.env.process(self.find_value(packet))
        elif msg.ptype == MessageType.STORE_VALUE:
            self.env.process(self.store_value(packet))
        elif msg.ptype == MessageType.SET_VALUE:
            self.set_value(packet)
        elif msg.ptype == MessageType.GET_VALUE:
            self.get_value(packet)

    def collect_load(self):
        self.datacollector.queue_load[self.name].append(
            (self.env.now, len(self.in_queue.queue))
        )

    def change_env(self, env: Environment) -> None:
        self.env = env
        self.in_queue = Resource(self.env, capacity=1)

    @abstractmethod
    def find_node(
        self, key: int | str, ask_to: Optional[DHTNode] = None
    ) -> SimpyProcess[List[Process]]:
        """Iteratively find the closest node(s) to the given key

        Args:
            key (int): The key
            ask_to (Optional[DHTNode], optional): the first node to contact
        """
        pass

    @abstractmethod
    def get_node(self, packet: Packet) -> None:
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

    @staticmethod
    def decide_value(packets: List[Packet]) -> Optional[Any]:
        # give the most popular value
        d: Dict[Any, int] = defaultdict(int)
        for packet in packets:
            if d[packet.message.data["value"]] is not None:
                d[packet.message.data["value"]] += 1
        if d:
            return max(d, key=lambda k: d[k])
        else:
            return None

    def get_value(self, packet: Packet) -> None:
        """Get value associated to a given key in the node's hash table

        Args:
            packet (Packet): The packet containing the asked key
            recv_req (Request): The request event to answer to
        """
        msg = packet.message
        key = msg.data["key"]
        new_msg = Message(
            ptype=MessageType.GET_VALUE_REPLY,
            data=dict(value=self.ht.get(key)),
            event=msg.event,
        )
        self.send_resp(packet.sender, new_msg)

    def set_value(self, packet: Packet) -> None:
        """Set the value to be associated to a given key in the node's hash table

        Args:
            packet (Packet): The packet containing the asked key
            recv_req (Request): The request event to answer to
        """
        msg = packet.message
        key = msg.data["key"]
        self.ht[key] = msg.data["value"]
        new_msg = Message(ptype=MessageType.SET_VALUE_REPLY, event=msg.event)
        self.send_resp(packet.sender, new_msg)

    def ask(self, to: List[DHTNode], data: Dict, ptype: MessageType) -> List[Request]:
        requests = list()
        for node in to:
            if node:
                self.log(f"asking {ptype.name} to {node.name} for {data}")
                new_msg = Message(ptype=ptype, data=data)
                sent_req = self.send_req(node, new_msg)
                requests.append(sent_req)
        return requests

    def unzip_find(
        self, key: int | str, function: Callable
    ) -> SimpyProcess[Tuple[List[DHTNode], int]]:
        processes = yield from self.find_node(key)
        ans = yield function(processes)
        self.log(f"nodes to ask: {ans}")
        best_nodes, hops = zip(*[ans[p] for p in processes])
        return list(best_nodes), max(hops)

    def find_value(self, packet: Packet) -> SimpyProcess[None]:
        self.log(f"Serving {packet}")
        msg = packet.message
        key = msg.data["key"]
        packets: List[Packet] = list()
        hops = -1
        try:
            nodes, hops = yield from self.unzip_find(key, self.env.all_of)
            requests = self.ask(nodes, msg.data, MessageType.GET_VALUE)
            yield from self.wait_resps(requests, packets)
        except DHTTimeoutError:
            if not packets:
                hops = -1

        value = DHTNode.decide_value(packets)
        self.log(f"decided value {value}")
        reply = Message(
            ptype=MessageType.FIND_VALUE_REPLY,
            data=dict(value=value, hops=hops),
            event=msg.event,
        )
        self.send_resp(packet.sender, reply)

    def store_value(self, packet: Packet) -> SimpyProcess[None]:
        self.log(f"Serving {packet}")
        msg = packet.message
        key = msg.data["key"]
        try:
            nodes, hops = yield from self.unzip_find(key, self.env.all_of)
            requests = self.ask(nodes, msg.data, MessageType.SET_VALUE)
            yield from self.wait_resps(requests, [])
        except DHTTimeoutError:
            hops = -1

        reply = Message(
            ptype=MessageType.STORE_VALUE_REPLY, data=dict(hops=hops), event=msg.event
        )
        self.send_resp(packet.sender, reply)
