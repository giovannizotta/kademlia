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
        if packet.ptype == PacketType.FIND_NODE:
            pass
        elif packet.ptype == PacketType.ASK_SUCC:
            pass
        elif packet.ptype == PacketType.SET_SUCC:
            pass
        elif packet.ptype == PacketType.SET_PRED:
            pass

    def _check_best_node(self, packet: Packet, best_node: ChordNode) -> \
            Tuple[ChordNode, bool, Optional[Request]]:
        tmp = packet.data["best_node"]
        found = best_node is tmp
        best_node = tmp
        if not found:
            self.log(
                f"received answer, packet: {packet} -> forwarding to {best_node}")
            sent_req = self.send_req(best_node, packet)
        else:
            self.log(
                f"received answer, packet: {packet} -> found! It's {best_node}")
            sent_req = None
        return best_node, found, sent_req

    def get_node(
        self,
        packet: Packet,
    ) -> None:
        key = packet.data["key"]
        best_node, _ = self._get_best_node(key)
        packet.data["best_node"] = best_node
        self.send_resp(recv_req, packet)

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

    def _get_best_node_and_forward(self, key: int, packet: Packet, ask_to: Optional[ChordNode]) -> \
            Tuple[ChordNode, bool, Optional[Request]]:
        self.log(f"looking for node with key {key}")
        if ask_to is not None:
            self.log(f"asking to {ask_to}")
            best_node, found = ask_to, False
        else:
            best_node, found = self._get_best_node(key)

        if not found:
            sent_req = self.send_req(best_node, packet)
        else:
            sent_req = None
        return best_node, found, sent_req

    def find_node(
        self,
        key: int,
        ask_to: Optional[DHTNode] = None
    ) -> SimpyProcess[Tuple[Optional[DHTNode], int]]:
        self.log(f"start looking for node holding {key}")
        packet = Packet(ptype=PacketType.FIND_NODE, data=dict(key=key))
        best_node, found, sent_req = self._get_best_node_and_forward(
            key, packet, ask_to)
        hops = 0
        while not found:
            hops += 1
            packet = yield from self.wait_resp(sent_req)
            best_node, found, sent_req = yield from self._check_best_node(packet, best_node)

        return best_node, hops

    def get_successor(
        self,
        packet: Packet,
        recv_req: Request,
    ) -> None:
        self.log("asked what my successor is")
        packet.data["succ"] = self.succ
        self.send_resp(recv_req, packet)

    def set_successor(
        self,
        packet: Packet,
        recv_req: Request,
    ) -> None:
        self.succ = packet.data["succ"]
        self.log(f"asked to change my successor to {self.succ}")
        self.send_resp(recv_req, packet)

    def get_predecessor(
        self,
        packet: Packet,
        recv_req: Request,
    ) -> None:
        self.log("asked what is my predecessor")
        packet.data["pred"] = self.pred
        self.send_resp(recv_req, packet)

    def set_predecessor(
        self,
        packet: Packet,
        recv_req: Request,
    ) -> None:
        self.pred = packet.data["pred"]
        self.log(f"asked to change my predecessor to {self.pred}")
        self.send_resp(recv_req, packet)

    def _read_succ_resp(self, packet: Packet) -> Optional[ChordNode]:
        self.log(f"got answer for node's succ, it's {packet.data['succ']}")
        succ: Optional[ChordNode] = packet.data["succ"]
        return succ

    def ask_successor(self, to: ChordNode) -> Request:
        self.log(f"asking {to} for it's successor")
        packet = Packet(ptype=PacketType.ASK_SUCC)
        sent_req = self.send_req(to, packet)
        return sent_req

    def _ask_set_pred_succ(self, pred: ChordNode, succ: ChordNode) -> Tuple[Request, Request]:
        self.log(f"asking {pred} to set me as its successor")
        self.log(f"asking {succ} to set me as its predecessor")
        packet_pred = Packet(ptype=PacketType.SET_PRED, data=dict(succ=self))
        packet_succ = Packet(ptype=PacketType.SET_SUCC, data=dict(pred=self))
        sent_req_pred = self.send_req(pred, packet_pred)
        sent_req_succ = self.send_req(succ, packet_succ)
        return sent_req_pred, sent_req_succ

    def _set_pred_succ(self, pred: ChordNode, succ: ChordNode) -> None:
        self.log(f"setting my succ to {succ}")
        self.log(f"setting my pred to {pred}")
        self.pred = pred
        self.succ = succ

    def join_network(self, to: DHTNode) -> SimpyProcess[None]:
        self.log(f"trying to join the network from {to}")
        # ask to to find_node
        node, hops = yield from self.find_node(self.id, ask_to=to)
        # ask node its successor
        sent_req = self.ask_successor(node)
        # wait for a response
        packet = yield from self.wait_resp(sent_req)
        # serve response
        node_succ = yield from self._read_succ_resp(packet)

        # ask them to put me in the ring
        sent_req_pred, sent_req_succ = self._ask_set_pred_succ(node, node_succ)
        # wait for both answers
        yield from self.wait_resps((sent_req_pred, sent_req_succ), [])
        # I do my rewiring
        yield from self._set_pred_succ(node, node_succ)

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

    def ask_value(self, to: ChordNode, packet: Packet) -> Request:
        self.log(f"asking {to} for the key {packet.data['key']}")
        sent_req = self.send_req(to, packet)
        return sent_req

    def reply_find_value(self, recv_req: Request, packet: Packet, hops: int) -> None:
        packet.data["hops"] = hops
        self.send_resp(recv_req, packet)

    def find_value(self, packet: Packet, recv_req: Request) -> SimpyProcess[None]:
        key = packet.data["key"]
        try:
            node, hops = yield from self.find_node(key)
            sent_req = self.ask_value(node, packet)
            packet = yield from self.wait_resp(sent_req)
        except DHTTimeoutError:
            hops = -1
            packet.data["value"] = None

        self.reply_find_value(recv_req, packet, hops)

    def ask_set_value(self, to: ChordNode, packet: Packet) -> Request:
        self.log(
            f"asking {to} to set the value {packet.data['value']} for the key {packet.data['key']}")
        sent_req = self.send_req(to, packet)
        return sent_req

    def reply_store_value(self, recv_req: Request, packet: Packet, hops: int) -> None:
        packet.data["hops"] = hops
        self.send_resp(recv_req, packet)

    def store_value(self, packet: Packet, recv_req: Request) -> SimpyProcess[None]:
        key = packet.data["key"]
        try:
            node, hops = yield from self.find_node(key)
            sent_req = self.ask_set_value(node, packet)
            packet = yield from self.wait_resp(sent_req)
        except DHTTimeoutError:
            hops = -1
            self.log(f"unable to store the ({key} {packet.data['value']} pair")

        self.reply_store_value(recv_req, packet, hops)

    # other methods to implement:
    # - update finger table, when is it called? Just at the beginning or periodically?
    #       - Assume just "gentle" leave or also Crashed -> in the second case must also
    #           keep pred and succ up to date
    #           (https://en.wikipedia.org/wiki/Chord_(peer-to-peer)#Stabilization)
