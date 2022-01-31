from __future__ import annotations
from common.utils import *
from common.node import DHTNode, packet_service
from dataclasses import dataclass, field, replace
from common.packet import Packet


@dataclass
class ChordNode(DHTNode):
    _pred: Optional[ChordNode] = field(init=False, repr=False)
    _succ: Optional[ChordNode] = field(init=False, repr=False)
    ft: List[ChordNode] = field(init=False, repr=False)

    def __post_init__(self):
        super().__post_init__()
        self.ft: List[ChordNode] = [self] * self.log_world_size

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
        """Get the best node for a given key from the finger table

        Args:
            key (int): the key to be searched

        Returns:
            Tuple[ChordNode, bool]: best_node, true if the node is self
        """
        best_node = min(self.ft, key=lambda node: ChordNode._compute_distance(
            node.id, key, self.log_world_size))
        found = best_node is self
        self.log(
            f"asked for best node for key {key}, it's {best_node if not found else 'me'}")
        return best_node, found

    @packet_service
    def _get_best_node_and_forward(self, key: int, packet: Packet, ask_to: Optional[ChordNode]) -> \
            Tuple[ChordNode, bool, Optional[simpy.Event]]:
        """Iteratively search for the best node for a given key. 

        Args:
            key (int): the key to be searched
            packet (Packet): the packet to be sent
            ask_to (Optional[ChordNode], optional): Optional hint of a node to ask to. Defaults to None.

        Returns:
            Tuple[ChordNode, bool]: best_node, found or not, sent_request 
        """
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
    def _check_best_node(self, packet: Packet, best_node: ChordNode) -> \
            Tuple[ChordNode, bool, Optional[simpy.Event]]:
        tmp = packet.data["best_node"]
        found = best_node is tmp
        best_node = tmp
        if not found:
            self.log(
                f"received answer, packet: {packet} -> forwarding to {best_node}")
            sent_req = self.send_req(best_node.on_find_node_request, packet)
        else:
            self.log(
                f"received answer, packet: {packet} -> found! It's {best_node}")
            sent_req = None
        return best_node, found, sent_req

    # def find_node_request(
    #     self,
    #     packet: Packet,
    #     recv_req: simpy.Event
    # ) -> SimpyProcess[ChordNode]:
    #     """Serve a find_node request for the given key.

    #     Args:
    #         packet (Packet): the packet
    #         recv_req (simpy.Event): the event to be triggered by the successful response
    #     """
    #     key = packet.data["key"]
    #     best_node = yield from self.find_node(key)
    #     packet.data["best_node"] = best_node
    #     self.send_resp(recv_req)

    @packet_service
    def on_find_node_request(
        self,
        packet: Packet,
        recv_req: simpy.Event
    ) -> None:
        """Get the best node in the finger table of the node for the given key.

        Args:
            packet (Packet): the packet
            recv_req (simpy.Event): the event to be triggered by the successful response
        """
        key = packet.data["key"]
        best_node, _ = self._get_best_node(key)
        packet.data["best_node"] = best_node
        self.send_resp(recv_req, packet)

    def find_node(
        self,
        key: int,
        ask_to: Optional[DHTNode] = None
    ) -> SimpyProcess[Optional[DHTNode]]:
        """Execute the iterative search to find the best node for the given key.

        Args:
            key (int): the key to be searched for
            ask_to (Optional[ChordNode], optional): first node to contact. Defaults to None.
        """
        self.log(f"start looking for node holding {key}")
        packet = Packet(data=dict(key=key))
        best_node, found, sent_req = yield from self._get_best_node_and_forward(key, packet, ask_to)
        while not found:
            packet = yield from self.wait_resp(sent_req)
            best_node, found, sent_req = yield from self._check_best_node(packet, best_node)

        return best_node

    @packet_service
    def get_successor(
        self,
        packet: Packet,
        recv_req: simpy.Event,
    ) -> None:
        self.log("asked what my successor is")
        packet.data["succ"] = self.succ
        self.send_resp(recv_req, packet)

    @packet_service
    def set_successor(
        self,
        packet: Packet,
        recv_req: simpy.Event,
    ) -> None:
        self.succ = packet.data["succ"]
        self.log(f"asked to change my successor to {self.succ}")
        self.send_resp(recv_req, packet)

    @packet_service
    def get_predecessor(
        self,
        packet: Packet,
        recv_req: simpy.Event,
    ) -> None:
        self.log("asked what is my predecessor")
        packet.data["pred"] = self.pred
        self.send_resp(recv_req, packet)

    @packet_service
    def set_predecessor(
        self,
        packet: Packet,
        recv_req: simpy.Event,
    ) -> None:
        self.pred = packet.data["pred"]
        self.log(f"asked to change my predecessor to {self.pred}")
        self.send_resp(recv_req, packet)

    @packet_service
    def _read_succ_resp(self, packet: Packet) -> Optional[ChordNode]:
        self.log(f"got answer for node's succ, it's {packet.data['succ']}")
        succ: Optional[ChordNode] = packet.data["succ"]
        return succ

    @packet_service
    def ask_successor(self, to: ChordNode, packet: Packet) -> simpy.Event:
        self.log(f"asking {to} for it's successor")
        sent_req = self.send_req(to.get_successor, packet)
        return sent_req

    @packet_service
    def _ask_set_pred_succ(self, pred: ChordNode, succ: ChordNode) -> Tuple[simpy.Event, simpy.Event]:
        self.log(f"asking {pred} to set me as its successor")
        self.log(f"asking {succ} to set me as its predecessor")
        packet_pred = Packet(data=dict(succ=self))
        packet_succ = Packet(data=dict(pred=self))
        sent_req_pred = self.send_req(pred.set_successor, packet_pred)
        sent_req_succ = self.send_req(succ.set_predecessor, packet_succ)
        return sent_req_pred, sent_req_succ

    @packet_service
    def _set_pred_succ(self, pred: ChordNode, succ: ChordNode) -> None:
        self.log(f"setting my succ to {succ}")
        self.log(f"setting my pred to {pred}")
        self.pred = pred
        self.succ = succ

    def join_network(self, to: DHTNode) -> SimpyProcess[None]:
        self.log(f"trying to join the network from {to}")
        # ask to to find_node
        node = yield from self.find_node(self.id, ask_to=to)
        # ask node its successor
        packet = Packet()
        sent_req = yield from self.ask_successor(node, packet)
        # wait for a response
        packet = yield from self.wait_resp(sent_req)
        # serve response
        node_succ = yield from self._read_succ_resp(packet)

        # ask them to put me in the ring
        sent_req_pred, sent_req_succ = yield from self._ask_set_pred_succ(node, node_succ)
        # wait for both answers
        _ = yield from self.wait_resps((sent_req_pred, sent_req_succ))
        # I do my rewiring
        yield from self._set_pred_succ(node, node_succ)

    @classmethod
    def _compute_distance(cls, key1: int, key2: int, log_world_size: int) -> int:
        """Compute the distance from key1 to key 2"""
        dst = (key2 - key1)
        ws: int = 2**log_world_size
        return dst % ws

    @packet_service
    def _update_ft(self, pos: int, node: ChordNode) -> None:
        self.ft[pos] = node

    def update(self) -> SimpyProcess[None]:
        for x in range(self.log_world_size):
            key = (self.id + 2**x) % (2 ** self.log_world_size)
            node = yield from self.find_node(key)
            yield from self._update_ft(x, node)

    @packet_service
    def get_value(self, packet: Packet, recv_req: simpy.Event) -> None:
        """Get value associated to a given key in the node's hash table"""
        key = packet.data["key"]
        packet.data["value"] = self.ht.get(key)
        self.send_resp(recv_req, packet)

    @packet_service
    def ask_value(self, to: ChordNode, packet: Packet) -> simpy.Event:
        self.log(f"asking {to} for the key {packet.data['key']}")
        sent_req = self.send_req(to.get_value, packet)
        return sent_req

    @packet_service
    def reply_find_value(self, recv_req: simpy.Event, packet: Packet) -> None:
        self.send_resp(recv_req, packet)

    def find_value(self, packet: Packet, recv_req: simpy.Event) -> SimpyProcess[None]:
        """Find the value associated to a given key"""
        key = packet.data["key"]
        try:
            node = yield from self.find_node(key)
            sent_req = yield from self.ask_value(node, packet)
            packet = yield from self.wait_resp(sent_req)
        except DHTTimeoutError:
            packet.data["value"] = None

        yield from self.reply_find_value(recv_req, packet)

    @packet_service
    def set_value(self, packet: Packet, recv_req: simpy.Event) -> None:
        """Set the value to be associated to a given key in the node's hash table"""
        key = packet.data["key"]
        self.ht[key] = packet.data["value"]
        self.send_resp(recv_req, packet)

    @packet_service
    def ask_set_value(self, to: ChordNode, packet: Packet) -> simpy.Event:
        self.log(
            f"asking {to} to set the value {packet.data['value']} for the key {packet.data['key']}")
        sent_req = self.send_req(to.set_value, packet)
        return sent_req

    @packet_service
    def reply_store_value(self, recv_req: simpy.Event, packet: Packet) -> None:
        self.send_resp(recv_req, packet)

    def store_value(self, packet: Packet, recv_req: simpy.Event) -> SimpyProcess[None]:
        """Store the value to be associated to a given key"""
        key = packet.data["key"]
        try:
            node = yield from self.find_node(key)
            sent_req = yield from self.ask_set_value(node, packet)
            packet = yield from self.wait_resp(sent_req)
        except DHTTimeoutError:
            self.log(f"unable to store the ({key} {packet.data['value']} pair")

        yield from self.reply_store_value(recv_req, packet)

    # other methods to implement:
    # - update finger table, when is it called? Just at the beginning or periodically?
    #       - Assume just "gentle" leave or also Crashed -> in the second case must also
    #           keep pred and succ up to date
    #           (https://en.wikipedia.org/wiki/Chord_(peer-to-peer)#Stabilization)
