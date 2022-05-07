from __future__ import annotations

from common.node import DHTNode, Node, Packet, PacketType
from common.utils import *


@dataclass
class ChordNode(DHTNode):
    k: int = field(repr=False, default=0)
    _pred: List[Optional[ChordNode]] = field(init=False, repr=False)
    _succ: List[Optional[ChordNode]] = field(init=False, repr=False)
    ft: List[List[ChordNode]] = field(init=False, repr=False)
    ids: List[int] = field(init=False, repr=False)

    def __post_init__(self):
        super().__post_init__()
        self.ids = [self._compute_key(f"{self.name}_{i}") for i in range(self.k)]
        self.ft = [[self] * self.log_world_size for _ in self.ids]
        self._succ = [None for _ in range(self.k)]
        self._pred = [None for _ in range(self.k)]

    def manage_packet(self, packet: Packet):
        self.log(f"chord, serving {packet}")
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
        index = packet.data["index"]
        best_node, _ = self._get_best_node(key, index)
        new_packet = Packet(ptype=PacketType.GET_NODE_REPLY, data=dict(best_node=best_node, key=key),
                            event=packet.event)
        self.send_resp(cast(Node, packet.sender), new_packet)

    @property
    def succ(self) -> List[Optional[ChordNode]]:
        return self._succ

    @succ.setter
    def succ(self, value: Tuple[int, ChordNode]) -> None:
        i, node = value
        self._succ[i] = node
        self.ft[i][-1] = node

    @property
    def pred(self) -> List[Optional[ChordNode]]:
        return self._pred

    @pred.setter
    def pred(self, value: Tuple[int, ChordNode]) -> None:
        i, node = value
        self._pred[i] = node

    def _get_best_node(self, key: int, index: int) -> Tuple[ChordNode, bool]:
        best_node = min(self.ft[index], key=lambda node: node._compute_distance(key))
        found = best_node is self
        self.log(
            f"asked for best node for key {key}, {index}, it's {best_node if not found else 'me'}")
        return best_node, found

    def _forward(self, key: int, index: int, found: bool, best_node: ChordNode):
        if not found:
            self.log(
                f"received answer, looking for: {key}, {index} -> forwarding to {best_node}")
            new_packet = Packet(ptype=PacketType.GET_NODE, data=dict(key=key, index=index))
            sent_req = self.send_req(best_node, new_packet)
        else:
            self.log(
                f"received answer, looking for: {key}, {index} -> found! It's {best_node}")
            sent_req = None
        return best_node, found, sent_req

    def _get_best_node_and_forward(self, key: int, index: int, ask_to: Optional[ChordNode]) -> \
            Tuple[ChordNode, bool, Optional[Request]]:
        self.log(f"looking for node with key {key} on index {index}")
        if ask_to is not None:
            self.log(f"asking to {ask_to}")
            best_node, found = ask_to, False
        else:
            best_node, found = self._get_best_node(key, index)

        return self._forward(key, index, found, best_node)

    def _check_best_node_and_forward(self, packet: Packet, best_node: ChordNode) -> \
            Tuple[ChordNode, bool, Optional[Request]]:
        tmp = packet.data["best_node"]
        found = best_node is tmp
        best_node = tmp
        return self._forward(packet.data["key"], packet.data["index"], found, best_node)

    def find_node_on_index(self, key: int, index: int, ask_to: Optional[ChordNode] = None) -> SimpyProcess[
        Tuple[Optional[ChordNode], int]]:
        self.log(f"start looking for nodes holding {key} for index {index}")
        best_node, found, sent_req = self._get_best_node_and_forward(key, index, ask_to)
        hops = 0
        while not found:
            hops += 1
            try:
                packet = yield from self.wait_resp(sent_req)
                packet.data["index"] = index
                best_node, found, sent_req = self._check_best_node_and_forward(packet, best_node)
            except DHTTimeoutError:
                self.log(f"find_node_on_index for {key}, {index} timed out.", level=logging.WARNING)
                return None, -1

        return best_node, hops

    def find_node(
            self,
            key: int,
            ask_to: Optional[ChordNode] = None
    ) -> SimpyProcess[List[simpy.Process]]:

        self.log(f"start looking for nodes holding {key}")
        yield self.env.timeout(0)
        return [self.env.process(self.find_node_on_index(key, index, ask_to)) for index in range(self.k)]

    def get_successor(
            self,
            packet: Packet,
    ) -> None:
        self.log("asked what my successor is")
        new_packet = Packet(ptype=PacketType.GET_SUCC_REPLY, data=dict(succ=self.succ[packet.data["index"]]),
                            event=packet.event)
        self.send_resp(packet.sender, new_packet)

    def set_successor(
            self,
            packet: Packet,
    ) -> None:
        self.succ = (packet.data["index"], packet.data["succ"])
        self.log(f"asked to change my successor for index {packet.data['index']} to {self.succ}")
        new_packet = Packet(ptype=PacketType.SET_SUCC_REPLY, event=packet.event)
        self.send_resp(packet.sender, new_packet)

    def get_predecessor(
            self,
            packet: Packet,
    ) -> None:
        self.log("asked what is my predecessor for index {packet.data['index']}")
        new_packet = Packet(ptype=PacketType.GET_PRED_REPLY, data=dict(succ=self.pred[packet.data["index"]]),
                            event=packet.event)
        self.send_resp(packet.sender, new_packet)

    def set_predecessor(
            self,
            packet: Packet,
    ) -> None:
        self.pred = (packet.data["index"], packet.data["pred"])
        self.log(f"asked to change my predecessor for index {packet.data['index']} to {self.pred}")
        new_packet = Packet(ptype=PacketType.SET_PRED_REPLY, event=packet.event)
        self.send_resp(packet.sender, new_packet)

    def ask_successor(self, to: ChordNode, index: int) -> Request:
        self.log(f"asking {to} for it's successor on index {index}")
        packet = Packet(ptype=PacketType.GET_SUCC, data=dict(index=index))
        sent_req = self.send_req(to, packet)
        return sent_req

    def join_network(self, to: ChordNode) -> SimpyProcess[None]:
        for index in range(self.k):
            node, _ = yield from self.find_node_on_index(self.id, index, ask_to=to)
            assert node is not None
            self.log(f"trying to join the network from {to} on index {index}")
            # ask to the node responsible for the key on index
            # ask node its successor
            sent_req = self.ask([node], Packet(ptype=PacketType.GET_SUCC, data=dict(index=index)), PacketType.GET_SUCC)[
                0]
            # wait for a response
            packet = yield from self.wait_resp(sent_req)
            # serve response
            node_succ = packet.data["succ"]

            # ask them to put me in the ring
            sent_req_pred = \
                self.ask([node], Packet(ptype=PacketType.SET_SUCC, data=dict(succ=self, index=index)),
                         PacketType.SET_SUCC)[
                    0]
            sent_req_succ = self.ask([node_succ], Packet(ptype=PacketType.SET_PRED, data=dict(pred=self, index=index)),
                                     PacketType.SET_PRED)[0]
            # wait for both answers
            yield from self.wait_resps((sent_req_pred, sent_req_succ), [])
            # I do my rewiring
            self.pred = (index, node)
            self.succ = (index, node_succ)

    def _compute_distance(self, from_key: int) -> int:
        dst = (from_key - self.id)
        return dst % (2 ** self.log_world_size)

    def _update_ft(self, pos: int, index: int, node: ChordNode) -> None:
        self.ft[index][pos] = node

    def update(self) -> SimpyProcess[None]:
        for x in range(self.log_world_size):
            key = (self.id + 2 ** x) % (2 ** self.log_world_size)
            # TODO: check all_of or any_of
            nodes, _ = yield from self.unzip_find(key, self.env.all_of)
            for index in range(self.k):
                node = nodes[index]
                if node is not None:
                    self._update_ft(x, index, node)

    # def ask(self, to: ChordNode, packet: Packet, ptype: PacketType) -> Request:
    #     self.log(f"asking {to} for {ptype.name} for {packet.data}")
    #     packet = Packet(ptype=ptype, data=packet.data)
    #     return self.send_req(to, packet)
    #
    # def find_value(self, packet: Packet) -> SimpyProcess[None]:
    #     self.log(f"find_value in ChordNode, serving {packet}")
    #     original_sender = packet.sender
    #     original_event = packet.event
    #     key = packet.data["key"]
    #     try:
    #         nodes, hops = yield from self.find_node(key)
    #         sent_req = self.ask(node, packet, PacketType.GET_VALUE)
    #         packet = yield from self.wait_resp(sent_req)
    #     except DHTTimeoutError:
    #         hops = -1
    #         packet.data["value"] = None
    #
    #     packet.data["hops"] = hops
    #     new_packet = Packet(ptype=PacketType.FIND_VALUE_REPLY, data=packet.data, event=original_event)
    #     self.log(f"Replying to {original_sender} with packet {new_packet}")
    #     self.send_resp(original_sender, new_packet)
    #
    # def store_value(self, packet: Packet) -> SimpyProcess[None]:
    #     self.log(f"Serving {packet}")
    #     original_sender = packet.sender
    #     original_event = packet.event
    #     key = packet.data["key"]
    #     try:
    #         node, hops = yield from self.find_node(key)
    #         self.log(f"found node {node} in {hops} hops")
    #         sent_req = self.ask(node, packet, PacketType.SET_VALUE)
    #         packet = yield from self.wait_resp(sent_req)
    #     except DHTTimeoutError:
    #         hops = -1
    #         self.log(f"unable to store the ({key} {packet.data['value']} pair")
    #
    #     packet.data["hops"] = hops
    #     new_packet = Packet(ptype=PacketType.STORE_VALUE_REPLY, data=packet.data, event=original_event)
    #     self.log(f"Replying to {original_sender} with packet {new_packet}")
    #     self.send_resp(original_sender, new_packet)

    # other methods to implement:
    # - update finger table, when is it called? Just at the beginning or periodically?
    #       - Assume just "gentle" leave or also Crashed -> in the second case must also
    #           keep pred and succ up to date
    #           (https://en.wikipedia.org/wiki/Chord_(peer-to-peer)#Stabilization)
