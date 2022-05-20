from __future__ import annotations

from common.node import DHTNode, Node, Packet, PacketType
from common.utils import *
from math import log2


@dataclass
class ChordNode(DHTNode):
    k: int = field(repr=False, default=0)
    _pred: List[Optional[ChordNode]] = field(init=False, repr=False)
    _succ: List[Optional[ChordNode]] = field(init=False, repr=False)
    ft: List[List[ChordNode]] = field(init=False, repr=False)
    ids: List[int] = field(init=False, repr=False)

    STABILIZE_PERIOD: ClassVar[float] = 200
    STABILIZE_STDDEV: ClassVar[float] = 20
    STABILIZE_MINCAP: ClassVar[float] = 100
    INBETWEEN_FINGER_PERIOD: ClassVar[float] = 10
    INBETWEEN_FINGER_STDDEV: ClassVar[float] = 2
    INBETWEEN_FINGER_MINCAP: ClassVar[float] = 3
    UPDATE_FINGER_PERIOD: ClassVar[float] = 200
    UPDATE_FINGER_STDDEV: ClassVar[float] = 20
    UPDATE_FINGER_MINCAP: ClassVar[float] = 100

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
        elif packet.ptype == PacketType.NOTIFY:
            self.notify(packet)

    def change_env(self, env: simpy.Environment) -> None:
        super().change_env(env)
        self.env.process(self.stabilize())
        self.env.process(self.fix_fingers())

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
        #self.ft[i][self.get_index_for(node.ids[i], i)] = node
        self.ft[i][-1] = node

    @property
    def pred(self) -> List[Optional[ChordNode]]:
        return self._pred

    @pred.setter
    def pred(self, value: Tuple[int, ChordNode]) -> None:
        i, node = value
        self._pred[i] = node

    def get_index_for(self, key: int, index: int) -> int:
        dst = self._compute_distance(key, index)
        return dst if dst == 0 else int(log2(dst))

    def _get_best_node(self, key: int, index: int) -> Tuple[ChordNode, bool]:
        best_node = min(self.ft[index], key=lambda node: node._compute_distance(key, index))
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

    def find_node_on_index(self, key: int | str, index: int, ask_to: Optional[ChordNode] = None) -> SimpyProcess[
        Tuple[Optional[ChordNode], int]]:
        # use different hash functions for each index by hashing key+index
        key_hash = self._compute_key(key + str(index)) if type(key) == str else key
        self.log(f"start looking for nodes holding {key} for index {index}")
        best_node, found, sent_req = self._get_best_node_and_forward(key_hash, index, ask_to)
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
        self.log(f"found node for key {key} on index {index}: {best_node}")
        return best_node, hops

    def find_node(
            self,
            key: int | str,
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
        index = packet.data["index"]
        new_packet = Packet(ptype=PacketType.GET_PRED_REPLY, data=dict(pred=self.pred[index]),
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

    def send_notify(self, index: int) -> None:
        succ = self.succ[index]
        assert succ is not None
        self.send_req(succ, Packet(ptype=PacketType.NOTIFY, data=dict(index=index, pred=self)))

    def notify(self, packet: Packet) -> None:
        p = packet.data["pred"]
        index = packet.data["index"]
        if self.pred[index] is None or p in (self, self.pred[index]):
            self.pred[index] = p

    def stabilize(self) -> SimpyProcess[None]:
        while True:
            yield self.env.timeout(self.rbg.get_normal(self.STABILIZE_PERIOD, self.STABILIZE_STDDEV, self.STABILIZE_MINCAP))
            for index in range(self.k):
                self.env.process(self.stabilize_on_index(index))


    def stabilize_on_index(self, index: int) -> SimpyProcess[None]:
        succ = self.succ[index]
        sent_req = self.ask([succ], Packet(ptype=PacketType.GET_PRED, data=dict(index=index)), PacketType.GET_PRED)[0]
        try:
            packet = yield from self.wait_resp(sent_req)
            x = packet.data["pred"]
            if x in (self, succ):
                self.succ[index] = x
            
            self.send_notify(index)
        except DHTTimeoutError:
            self.log(f"Timeout on stabilize on index {index}", level=logging.WARNING)


    def fix_finger_on_index(self, finger_index: int, index: int) -> SimpyProcess[None]:
        key = (self.ids[index] + 2 ** finger_index) % (2 ** self.log_world_size)
        node, _ = yield from self.find_node_on_index(key, index)
        if node is not None:
            self._update_ft(finger_index, index, node)


    def fix_fingers(self) -> SimpyProcess[None]:
        while True:
            yield self.env.timeout(self.rbg.get_normal(self.UPDATE_FINGER_PERIOD, self.UPDATE_FINGER_STDDEV, self.UPDATE_FINGER_MINCAP))
            for finger_index in range(self.log_world_size):
                for index in range(self.k):
                    self.env.process(self.fix_finger_on_index(finger_index, index))
                yield self.env.timeout(self.rbg.get_normal(self.INBETWEEN_FINGER_PERIOD, self.INBETWEEN_FINGER_STDDEV, self.INBETWEEN_FINGER_MINCAP))

    def join_network(self, to: ChordNode) -> SimpyProcess[None]:
        for index in range(self.k):
            self.log(f"trying to join the network from {to} on index {index}")
            node, _ = yield from self.find_node_on_index(self.ids[index], index, ask_to=to)
            assert node is not None
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
                         PacketType.SET_SUCC)[0]
            sent_req_succ = self.ask([node_succ], Packet(ptype=PacketType.SET_PRED, data=dict(pred=self, index=index)),
                                     PacketType.SET_PRED)[0]
            # wait for both answers
            yield from self.wait_resps((sent_req_pred, sent_req_succ), [])
            # I do my rewiring
            self.pred = (index, node)
            self.succ = (index, node_succ)


    def _compute_distance(self, from_key: int, index: int) -> int:
        dst = (from_key - self.ids[index])
        return dst % (2 ** self.log_world_size)

    def _update_ft(self, pos: int, index: int, node: ChordNode) -> None:
        self.ft[index][pos] = node

    def update(self) -> SimpyProcess[None]:
        for index, id_ in enumerate(self.ids):
            for x in range(self.log_world_size):
                key = (id_ + 2 ** x) % (2 ** self.log_world_size)
                node, _ = yield from self.find_node_on_index(key, index)
                if node is not None:
                    self._update_ft(x, index, node)

    # other methods to implement:
    # - update finger table, when is it called? Just at the beginning or periodically?
    #       - Assume just "gentle" leave or also Crashed -> in the second case must also
    #           keep pred and succ up to date
    #           (https://en.wikipedia.org/wiki/Chord_(peer-to-peer)#Stabilization)
