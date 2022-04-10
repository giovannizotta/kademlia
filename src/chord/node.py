from __future__ import annotations
from common.utils import *
from common.node import DHTNode, Node, Packet, PacketType, Request
from dataclasses import dataclass, field


@dataclass
class ChordNode(DHTNode):
    _pred: Optional[ChordNode] = field(init=False, repr=False)
    _succ: Optional[ChordNode] = field(init=False, repr=False)
    ft: List[ChordNode] = field(init=False, repr=False)

    def __post_init__(self):
        super().__post_init__()
        self.ft: List[ChordNode] = [self] * self.log_world_size

    def manage_packet(self, packet: Packet):
        super().manage_packet(packet)
        if packet.ptype == PacketType.GET_SUCC:
            self.get_successor(packet)
        elif packet.ptype == PacketType.SET_SUCC:
            self.set_successor(packet)
        elif packet.ptype == PacketType.GET_PRED:
            self.get_predecessor(packet)
        elif packet.ptype == PacketType.SET_PRED:
            self.set_predecessor(packet)

    def get_node(
        self,
        packet: Packet,
    ) -> None:
        key = packet.data["key"]
        best_node, _ = self._get_best_node(key)
        new_packet = Packet(ptype=PacketType.GET_NODE_REPLY, data=dict(best_node=best_node, key=key), event=packet.event)
        self.send_resp(cast(Node, packet.sender), new_packet)

    @property
    def succ(self) -> Optional[ChordNode]:
        return self._succ

    @succ.setter
    def succ(self, node: ChordNode) -> None:
        self._succ = node
        self.ft[-1] = node

    @property
    def pred(self) -> Optional[ChordNode]:
        return self._pred

    @pred.setter
    def pred(self, node: ChordNode) -> None:
        self._pred = node

    def _get_best_node(self, key: int) -> Tuple[ChordNode, bool]:
        best_node = min(self.ft, key=lambda node: node._compute_distance(key))
        found = best_node is self
        self.log(
            f"asked for best node for key {key}, it's {best_node if not found else 'me'}")
        return best_node, found

    def _forward(self, key: int, found: bool, best_node: ChordNode):
        if not found:
            self.log(
                f"received answer, looking for: {key} -> forwarding to {best_node}")
            new_packet = Packet(ptype=PacketType.GET_NODE, data=dict(key=key))
            sent_req = self.send_req(best_node, new_packet)
        else:
            self.log(
                f"received answer, looking for: {key} -> found! It's {best_node}")
            sent_req = None
        return best_node, found, sent_req

    def _get_best_node_and_forward(self, key: int, ask_to: Optional[ChordNode]) -> \
            Tuple[ChordNode, bool, Optional[Request]]:
        self.log(f"looking for node with key {key}")
        if ask_to is not None:
            self.log(f"asking to {ask_to}")
            best_node, found = ask_to, False
        else:
            best_node, found = self._get_best_node(key)

        return self._forward(key, found, best_node)

    def _check_best_node_and_forward(self, packet: Packet, best_node: ChordNode) -> \
            Tuple[ChordNode, bool, Optional[Request]]:
        tmp = packet.data["best_node"]
        found = best_node is tmp
        best_node = tmp
        return self._forward(packet.data["key"], found, best_node)

    def find_node(
        self,
        key: int,
        ask_to: Optional[ChordNode] = None
    ) -> SimpyProcess[Tuple[ChordNode, int]]:

        self.log(f"start looking for node holding {key}")

        best_node, found, sent_req = self._get_best_node_and_forward(key, ask_to)
        hops = 0
        while not found:
            hops += 1
            packet = yield from self.wait_resp(sent_req)
            best_node, found, sent_req = self._check_best_node_and_forward(packet, best_node)

        return best_node, hops

    def get_successor(
        self,
        packet: Packet,
    ) -> None:
        self.log("asked what my successor is")
        new_packet = Packet(ptype=PacketType.GET_SUCC_REPLY, data=dict(succ=self.succ), event=packet.event)
        self.send_resp(packet.sender, new_packet)

    def set_successor(
        self,
        packet: Packet,
    ) -> None:
        self.succ = packet.data["succ"]
        self.log(f"asked to change my successor to {self.succ}")
        new_packet = Packet(ptype=PacketType.SET_SUCC_REPLY, event=packet.event)
        self.send_resp(packet.sender, new_packet)

    def get_predecessor(
        self,
        packet: Packet,
    ) -> None:
        self.log("asked what is my predecessor")
        new_packet = Packet(ptype=PacketType.GET_PRED_REPLY, data=dict(succ=self.pred), event=packet.event)
        self.send_resp(packet.sender, new_packet)

    def set_predecessor(
        self,
        packet: Packet,
    ) -> None:
        self.pred = packet.data["pred"]
        self.log(f"asked to change my predecessor to {self.pred}")
        new_packet = Packet(ptype=PacketType.SET_PRED_REPLY, event=packet.event)
        self.send_resp(packet.sender, new_packet)

    def ask_successor(self, to: ChordNode) -> Request:
        self.log(f"asking {to} for it's successor")
        packet = Packet(ptype=PacketType.GET_SUCC)
        sent_req = self.send_req(to, packet)
        return sent_req

    def join_network(self, to: ChordNode) -> SimpyProcess[None]:
        self.log(f"trying to join the network from {to}")
        # ask to to find_node
        node, _ = yield from self.find_node(self.id, ask_to=to)
        # ask node its successor
        sent_req = self.ask(node, Packet(ptype=PacketType.GET_SUCC), PacketType.GET_SUCC)
        # wait for a response
        packet = yield from self.wait_resp(sent_req)
        # serve response
        node_succ = packet.data["succ"]

        # ask them to put me in the ring
        sent_req_pred = self.ask(node, Packet(ptype=PacketType.SET_SUCC, data=dict(succ=self)), PacketType.SET_SUCC)
        sent_req_succ = self.ask(node_succ, Packet(ptype=PacketType.SET_PRED, data=dict(pred=self)), PacketType.SET_PRED)
        # wait for both answers
        yield from self.wait_resps((sent_req_pred, sent_req_succ), [])
        # I do my rewiring
        self.pred = node
        self.succ = node_succ

    def _compute_distance(self, from_key: int) -> int:
        dst = (from_key - self.id)
        return dst % (2**self.log_world_size)

    def _update_ft(self, pos: int, node: ChordNode) -> None:
        self.ft[pos] = node

    def update(self) -> SimpyProcess[None]:
        for x in range(self.log_world_size):
            key = (self.id + 2**x) % (2 ** self.log_world_size)
            node, hops = yield from self.find_node(key)
            self._update_ft(x, node)

    def ask(self, to: ChordNode, packet: Packet, ptype: PacketType) -> Request:
        self.log(f"asking {to} for {ptype.name} for {packet.data}")
        packet = Packet(ptype=ptype, data=packet.data)
        return self.send_req(to, packet)
    
    def find_value(self, packet: Packet) -> SimpyProcess[None]:
        original_sender = packet.sender
        original_event = packet.event
        key = packet.data["key"]
        try:
            node, hops = yield from self.find_node(key)
            sent_req = self.ask(node, packet, PacketType.GET_VALUE)
            packet = yield from self.wait_resp(sent_req)
        except DHTTimeoutError:
            hops = -1
            packet.data["value"] = None

        packet.data["hops"] = hops
        new_packet = Packet(ptype=PacketType.FIND_VALUE_REPLY, data=packet.data, event=original_event)
        self.log(f"Replying to {original_sender} with packet {new_packet}")
        self.send_resp(original_sender, new_packet)

    def store_value(self, packet: Packet) -> SimpyProcess[None]:
        original_sender = packet.sender
        original_event = packet.event
        key = packet.data["key"]
        try:
            node, hops = yield from self.find_node(key)
            self.log(f"found node {node} in {hops} hops")
            sent_req = self.ask(node, packet, PacketType.SET_VALUE)
            packet = yield from self.wait_resp(sent_req)
        except DHTTimeoutError:
            hops = -1
            self.log(f"unable to store the ({key} {packet.data['value']} pair")

        packet.data["hops"] = hops
        new_packet = Packet(ptype=PacketType.STORE_VALUE_REPLY, data=packet.data, event=original_event)
        self.log(f"Replying to {original_sender} with packet {new_packet}")
        self.send_resp(original_sender, new_packet)

    # other methods to implement:
    # - update finger table, when is it called? Just at the beginning or periodically?
    #       - Assume just "gentle" leave or also Crashed -> in the second case must also
    #           keep pred and succ up to date
    #           (https://en.wikipedia.org/wiki/Chord_(peer-to-peer)#Stabilization)
