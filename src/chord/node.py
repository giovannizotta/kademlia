from __future__ import annotations
from common.utils import *
from common.node import Node, packet_service
from dataclasses import dataclass, field
from common.packet import Packet


@dataclass
class ChordNode(Node):
    _pred: Optional[ChordNode] = field(init=False, repr=False)
    _succ: Optional[ChordNode] = field(init=False, repr=False)
    ft: List[ChordNode] = field(init=False, repr=False)

    def __post_init__(self):
        super().__post_init__()
        self.ft: List[ChordNode] = [self] * self.log_world_size

    @property
    def succ(self) -> ChordNode:
        return self._succ

    @succ.setter
    def succ(self, node: ChordNode) -> None:
        self._succ = node
        self.ft[1] = node

    @property
    def pred(self) -> ChordNode:
        return self._pred

    @pred.setter
    def pred(self, node: ChordNode) -> None:
        self._pred = node

    def _get_best_node(self, key: int) -> Tuple[ChordNode, bool]:
        best_node = min(self.ft, key=lambda node: ChordNode._compute_distance(
            node.id, key, self.log_world_size))
        found = best_node is self
        self.log(f"asked for best node for key {key}, it's {best_node if not found else 'me'}")
        return best_node, found

    @packet_service
    def _get_best_node_and_forward(self, key: int, packet: Packet, ask_to: Optional[ChordNode] = None) -> Tuple[ChordNode, bool]:
        self.log(f"looking for node with key {key}")
        if ask_to is not None:
            self.log(f"asking to {ask_to}")
            best_node, found = ask_to, False
        else:
            best_node, found = self._get_best_node(key)
            
        if not found:
            sent_req = self.send_req(best_node.on_find_node_request, packet)
        else:
            sent_req = None
        return best_node, found, sent_req

    

    @packet_service
    def _check_best_node(self, packet: Packet, best_node: ChordNode, timeout: simpy.Event) -> Tuple[ChordNode, bool, simpy.Event]:
        if timeout.processed:
            self.log("TIMEOUT")
            return self, True, timeout
        tmp = packet.data["best_node"]
        found = best_node is tmp
        if not found:
            self.log(f"received answer, forwarding to {best_node}")
            sent_req = self.send_req(best_node.on_find_node_request, packet)
        else:
            self.log(f"received answer, found! It's {best_node}")
            sent_req = None
        return tmp, found, sent_req

    def find_node_request(
        self,
        packet: Packet,
        recv_req: simpy.Event
    ) -> SimpyProcess[ChordNode]:
        key = packet.data["key"]
        best_node = yield from self.find_node(key)
        packet.data["best_node"] = best_node
        self.send_resp(recv_req)

    @packet_service
    def on_find_node_request(
        self,
        packet: Packet,
        recv_req: simpy.Event
    ) -> SimpyProcess:
        key = packet.data["key"]
        best_node, _ = self._get_best_node(key)
        packet.data["best_node"] = best_node
        self.send_resp(recv_req)

    def find_node(
        self,
        key: int,
        ask_to: Optional[ChordNode] = None
    ) -> SimpyProcess[ChordNode]:
        self.log(f"start looking for node holding {key}")
        packet = Packet(data=dict(key=key))
        best_node, found, sent_req = yield from self._get_best_node_and_forward(key, packet, ask_to)
        while not found:
            timeout = yield from self.wait_resp(sent_req)
            best_node, found, sent_req = yield from self._check_best_node(packet, best_node, timeout)

        return best_node

    @packet_service
    def get_successor(
        self,
        packet: Packet,
        recv_req: simpy.Event,
    ) -> SimpyProcess:
        self.log("asked what is my successor")
        packet.data["succ"] = self.succ
        self.send_resp(recv_req)

    @packet_service
    def set_successor(
        self,
        packet: Packet,
        recv_req: simpy.Event,
    ) -> SimpyProcess:
        self.succ = packet.data["succ"]
        self.log(f"asked to change my successor to {self.succ}")
        self.send_resp(recv_req)

    @packet_service
    def get_predecessor(
        self,
        packet: Packet,
        recv_req: simpy.Event,
    ) -> SimpyProcess:
        self.log("asked what is my predecessor")
        packet.data["pred"] = self.pred
        self.send_resp(recv_req)

    @packet_service
    def set_predecessor(
        self,
        packet: Packet,
        recv_req: simpy.Event,
    ) -> SimpyProcess:
        self.pred = packet.data["pred"]
        self.log(f"asked to change my predecessor to {self.pred}")
        self.send_resp(recv_req)

    @packet_service
    def _read_succ_resp(self, packet: Packet, timeout: simpy.Event) -> ChordNode:
        if timeout.processed:
            self.log("TIMEOUT")
            return None
        self.log(f"got answer for node's succ, it's {packet.data['succ']}")
        return packet.data["succ"]

    @packet_service
    def ask_successor(self, to: ChordNode, packet:Packet) -> SimpyProcess[ChordNode]:
        self.log(f"asking {to} for it's successor")
        sent_req = self.send_req(to.get_successor, packet)
        return sent_req

    @packet_service
    def _ask_set_pred_succ(self, pred: ChordNode, succ: ChordNode) -> None:
        self.log(f"asking {pred} to set me as its successor")
        self.log(f"asking {succ} to set me as its predecessor")
        packet_pred = Packet(data=dict(succ=self))
        packet_succ = Packet(data=dict(pred=self))
        sent_req_pred = self.send_req(pred.set_successor, packet_pred)
        sent_req_succ = self.send_req(succ.set_predecessor, packet_succ)
        return sent_req_pred, sent_req_succ

    @packet_service
    def _set_pred_succ(self, pred: ChordNode, succ: ChordNode, timeout: simpy.Event) -> None:
        if timeout.processed:
            self.log("TIMEOUT")
            return
        self.log(f"setting my succ to {succ}")
        self.log(f"setting my pred to {pred}")
        self.pred = pred
        self.succ = succ

    def join_network(self, to: Node) -> SimpyProcess:
        self.log(f"trying to join the network from {to}")
        # ask to to find_node
        node = yield from self.find_node(self.id, ask_to=to)
        # ask node its successor
        packet = Packet()
        sent_req = yield from self.ask_successor(node, packet)
        # wait for a response (or timeout)
        timeout = yield from self.wait_resp(sent_req)
        # serve response
        node_succ = yield from self._read_succ_resp(packet, timeout)

        # ask them to put me in the ring
        sent_req_pred, sent_req_succ = yield from self._ask_set_pred_succ(node, node_succ)
        # wait for both answers
        timeout = yield from self.wait_resp(sent_req_pred & sent_req_succ)
        # I do my rewiring
        yield from self._set_pred_succ(node, node_succ, timeout)

    @staticmethod
    def _compute_distance(key1: int, key2: int, log_world_size: int) -> int:
        """Compute the distance from key1 to key 2"""
        return (key2 - key1) % (2**log_world_size - 1)

    @packet_service
    def _update_ft(self, pos, node) -> None:
        self.ft[pos] = node

    def update(self) -> SimpyProcess:
        for x in range(self.log_world_size):
            key = (self.id * 2**x) % (2 ** self.log_world_size)
            node = yield from self.find_node(key)
            yield from self._update_ft(x, node)

    # other methods to implement:
    # - update finger table, when is it called? Just at the beginning or periodically?
    #       - Assume just "gentle" leave or also Crashed -> in the second case must also
    #           keep pred and succ up to date
    #           (https://en.wikipedia.org/wiki/Chord_(peer-to-peer)#Stabilization)