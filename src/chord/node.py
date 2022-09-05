from __future__ import annotations

import logging
from dataclasses import dataclass, field
from math import log2
from typing import Iterable, List, Optional, SupportsIndex, Tuple, Union

from simpy.core import Environment
from simpy.events import Process

from common.node import DHTNode
from common.packet import Message, MessageType, Packet
from common.utils import DHTTimeoutError, Request, SimpyProcess


class _SuccView(List[Optional["ChordNode"]]):
    def __init__(
            self, node: ChordNode, iterable: Iterable[Optional["ChordNode"]]
    ) -> None:
        super().__init__(iterable)
        self.node = node

    def __setitem__(self, i: SupportsIndex, node: ChordNode) -> None:
        super().__setitem__(i, node)
        # self.ft[i][self._get_index_for(node.ids[i], i)] = node
        self.node.ft[i][-1] = node


@dataclass
class ChordNode(DHTNode):
    k: int = field(repr=False)

    _pred: List[Optional[ChordNode]] = field(init=False, repr=False)
    _succ: _SuccView = field(init=False, repr=False)
    ft: List[List[ChordNode]] = field(init=False, repr=False)
    ids: List[int] = field(init=False, repr=False)

    stabilize_period: float = field(repr=False)
    stabilize_stddev: float = field(repr=False)
    stabilize_mincap: float = field(repr=False)
    update_finger_period: float = field(repr=False)
    update_finger_stddev: float = field(repr=False)
    update_finger_mincap: float = field(repr=False)

    def __post_init__(self):
        super().__post_init__()
        self.ids = [self._compute_key(f"{self.name}_{i}") for i in range(self.k)]
        self.ft = [[self] * self.log_world_size for _ in self.ids]
        self._succ = _SuccView(self, [None for _ in range(self.k)])
        self._pred = [None for _ in range(self.k)]

    def manage_packet(self, packet: Packet) -> None:
        self.log(f"chord, serving {packet}")
        super().manage_packet(packet)
        msg = packet.message
        if msg.ptype == MessageType.GET_SUCC:
            self.get_successor(packet)
        elif msg.ptype == MessageType.SET_SUCC:
            self.set_successor(packet)
        elif msg.ptype == MessageType.GET_PRED:
            self.get_predecessor(packet)
        elif msg.ptype == MessageType.SET_PRED:
            self.set_predecessor(packet)
        elif msg.ptype == MessageType.NOTIFY:
            self.notify(packet)

    def change_env(self, env: Environment) -> None:
        super().change_env(env)
        self.env.process(self.stabilize())
        self.env.process(self.fix_fingers())

    def get_node(
            self,
            packet: Packet,
    ) -> None:
        msg = packet.message
        key = msg.data["key"]
        index = msg.data["index"]
        best_node, _ = self._get_best_node(key, index)
        reply = Message(
            ptype=MessageType.GET_NODE_REPLY,
            data=dict(best_node=best_node, key=key, index=index),
            event=msg.event,
        )
        self.send_resp(packet.sender, reply)

    @property
    def succ(self) -> _SuccView:
        return self._succ

    # @succ.setter
    # def succ(self, value: Tuple[int, ChordNode]) -> None:
    #     i, node = value
    #     self._succ[i] = node
    #     # self.ft[i][self._get_index_for(node.ids[i], i)] = node
    #     self.ft[i][-1] = node

    @property
    def pred(self) -> List[Optional[ChordNode]]:
        return self._pred

    @pred.setter
    def pred(self, value: Tuple[int, ChordNode]) -> None:
        i, node = value
        self._pred[i] = node

    def _get_index_for(self, key: int, index: int) -> int:
        dst = self._compute_distance(key, index)
        return dst if dst == 0 else int(log2(dst))

    def _get_best_node(self, key: int, index: int) -> Tuple[ChordNode, bool]:
        best_node = min(
            self.ft[index], key=lambda node: node._compute_distance(key, index)
        )
        found = best_node is self
        self.log(
            f"asked for best node for key {key}, {index}, "
            f"it's {best_node if not found else 'me'}"
        )
        return best_node, found

    def _forward(
            self, key: int, index: int, found: bool, best_node: ChordNode
    ) -> Tuple[ChordNode, bool, Optional[Request]]:
        if not found:
            self.log(
                f"received answer, looking for: {key}, {index} "
                f"-> forwarding to {best_node}"
            )
            msg = Message(ptype=MessageType.GET_NODE, data=dict(key=key, index=index))
            sent_req = self.send_req(best_node, msg)
        else:
            self.log(
                f"received answer, looking for: {key}, {index}"
                f"-> found! It's {best_node}"
            )
            sent_req = None
        return best_node, found, sent_req

    def _get_best_node_and_forward(
            self, key: int, index: int, ask_to: Optional[ChordNode]
    ) -> Tuple[ChordNode, bool, Optional[Request]]:
        self.log(f"looking for node with key {key} on index {index}")
        if ask_to is not None and ask_to is not self:
            self.log(f"asking to {ask_to}")
            best_node, found = ask_to, False
        else:
            best_node, found = self._get_best_node(key, index)

        return self._forward(key, index, found, best_node)

    def _check_best_node_and_forward(
            self, packet: Packet, best_node: ChordNode
    ) -> Tuple[ChordNode, bool, Optional[Request]]:
        msg = packet.message
        tmp = msg.data["best_node"]
        found = best_node is tmp
        best_node = tmp
        return self._forward(msg.data["key"], msg.data["index"], found, best_node)

    def find_node_on_index(
            self, key: Union[int, str], index: int, ask_to: Optional[ChordNode] = None
    ) -> SimpyProcess[Tuple[Optional[ChordNode], int]]:
        # use different hash functions for each index by hashing key+index
        key_hash = self._compute_key(key + str(index)) if isinstance(key, str) else key
        self.log(f"start looking for nodes holding {key} for index {index}")
        best_node, found, sent_req = self._get_best_node_and_forward(
            key_hash, index, ask_to
        )
        hops = 0
        while not found:
            assert sent_req is not None
            hops += 1
            try:
                reply = yield from self.wait_resp(sent_req)
                best_node, found, sent_req = self._check_best_node_and_forward(
                    reply, best_node
                )
            except DHTTimeoutError:
                self.log(
                    f"find_node_on_index for {key}, {index} timed out.",
                    level=logging.WARNING,
                )
                self.purge(best_node)
                return None, -1
        self.log(f"found node for key {key} on index {index}: {best_node}")
        return best_node, hops

    def find_node(
            self, key: Union[int, str], ask_to: Optional[ChordNode] = None
    ) -> SimpyProcess[List[Process]]:

        self.log(f"start looking for nodes holding {key}")
        yield self.env.timeout(0)
        return [
            self.env.process(self.find_node_on_index(key, index, ask_to))
            for index in range(self.k)
        ]

    def get_successor(self, packet: Packet) -> None:
        self.log("asked what my successor is")
        msg = packet.message
        reply = Message(
            ptype=MessageType.GET_SUCC_REPLY,
            data=dict(succ=self.succ[msg.data["index"]]),
            event=msg.event,
        )
        self.send_resp(packet.sender, reply)

    def set_successor(self, packet: Packet) -> None:
        msg = packet.message
        self.succ[msg.data["index"]] = msg.data["succ"]
        self.log(
            "asked to change my successor for index "
            f"{msg.data['index']} to {self.succ}"
        )
        reply = Message(ptype=MessageType.SET_SUCC_REPLY, event=msg.event)
        self.send_resp(packet.sender, reply)

    def get_predecessor(
            self,
            packet: Packet,
    ) -> None:
        msg = packet.message
        self.log(f"asked what is my predecessor for index {msg.data['index']}")
        index = msg.data["index"]
        reply = Message(
            ptype=MessageType.GET_PRED_REPLY,
            data=dict(pred=self.pred[index]),
            event=msg.event,
        )
        self.send_resp(packet.sender, reply)

    def set_predecessor(
            self,
            packet: Packet,
    ) -> None:
        msg = packet.message
        self.pred = (msg.data["index"], msg.data["pred"])
        self.log(
            "asked to change my predecessor for index "
            f"{msg.data['index']} to {self.pred}"
        )
        reply = Message(ptype=MessageType.SET_PRED_REPLY, event=msg.event)
        self.send_resp(packet.sender, reply)

    def ask_successor(self, to: ChordNode, index: int) -> Request:
        self.log(f"asking {to} for it's successor on index {index}")
        packet = Message(ptype=MessageType.GET_SUCC, data=dict(index=index))
        sent_req = self.send_req(to, packet)
        return sent_req

    def send_notify(self, index: int) -> None:
        succ = self.succ[index]
        assert succ is not None
        self.send_req(
            succ,
            Message(ptype=MessageType.NOTIFY, data=dict(index=index, pred=self)),
        )

    def notify(self, packet: Packet) -> None:
        msg = packet.message
        p = msg.data["pred"]
        index = msg.data["index"]
        if self.pred[index] is None or p in (self, self.pred[index]):
            self.pred[index] = p

    def stabilize(self) -> SimpyProcess[None]:
        while True:
            yield self.env.timeout(
                self.rbg.get_normal(
                    self.stabilize_period,
                    self.stabilize_stddev,
                    self.stabilize_mincap,
                )
            )
            self.log("stabilizing")
            for index in range(self.k):
                self.env.process(self.stabilize_on_index(index))

    def stabilize_on_index(self, index: int) -> SimpyProcess[None]:
        succ = self.succ[index]
        assert succ is not None
        sent_req = self.ask(
            [succ],
            dict(index=index),
            MessageType.GET_PRED,
        ).pop()
        try:
            reply = yield from self.wait_resp(sent_req)
            x = reply.message.data["pred"]
            if x in (self, succ):
                self.succ[index] = x

            self.send_notify(index)
        except DHTTimeoutError:
            self.log(f"Timeout on stabilize on index {index}", level=logging.WARNING)
            self.purge(succ)
            joined = yield from self.join_network_on_index(self, index)
            if not joined:
                self.log("failed to join after stabilize", level=logging.ERROR)

    def fix_finger_on_index(self, finger_index: int, index: int) -> SimpyProcess[None]:
        key = (self.ids[index] + 2 ** finger_index) % (2 ** self.log_world_size)
        self.log(f"Updating finger {finger_index} on index {index}")
        node, _ = yield from self.find_node_on_index(key, index)
        if node is not None:
            self._update_ft(finger_index, index, node)

    def fix_fingers(self) -> SimpyProcess[None]:
        while True:
            yield self.env.timeout(
                self.rbg.get_normal(
                    self.update_finger_period,
                    self.update_finger_stddev,
                    self.update_finger_mincap,
                )
            )
            self.log("fixing fingers")
            for finger_index in range(self.log_world_size):
                for index in range(self.k):
                    self.env.process(self.fix_finger_on_index(finger_index, index))

    def join_network_on_index(self, to: ChordNode, index: int) -> SimpyProcess[bool]:
        self.log(f"trying to join the network from {to} on index {index}")
        try:
            # ask to the node responsible for the key on index
            node, _ = yield from self.find_node_on_index(self.ids[index], index, ask_to=to)
            if node is None:
                raise DHTTimeoutError()
            # ask node its successor
            sent_req = self.ask(
                [node],
                dict(index=index),
                MessageType.GET_SUCC,
            ).pop()
            # wait for a response
            reply = yield from self.wait_resp(sent_req)
            # serve response
            node_succ = reply.message.data["succ"]
            if node_succ is None:
                return False

            # ask them to put me in the ring
            sent_req_pred = self.ask(
                [node],
                dict(succ=self, index=index),
                MessageType.SET_SUCC,
            ).pop()
            sent_req_succ = self.ask(
                [node_succ],
                dict(pred=self, index=index),
                MessageType.SET_PRED,
            ).pop()
            # wait for both answers
            yield from self.wait_resps((sent_req_pred, sent_req_succ), [])
            # I do my rewiring
            self.pred = (index, node)
            self.succ[index] = node_succ
            return True
        except DHTTimeoutError:
            self.log(f"Timeout on join on index {index}", level=logging.WARNING)
            return False

    def join_network(self, to: ChordNode) -> SimpyProcess[bool]:
        processes = []
        for index in range(self.k):
            processes.append(self.env.process(self.join_network_on_index(to, index)))

        outcomes = yield self.env.all_of(processes)
        return any(outcomes.values())

    def _compute_distance(self, from_key: int, index: int) -> int:
        dst = from_key - self.ids[index]
        return int(dst % (2 ** self.log_world_size))

    def _update_ft(self, pos: int, index: int, node: ChordNode) -> None:
        self.ft[index][pos] = node

    def update(self) -> SimpyProcess[None]:
        for index, id_ in enumerate(self.ids):
            for x in range(self.log_world_size):
                key = (id_ + 2 ** x) % (2 ** self.log_world_size)
                node, _ = yield from self.find_node_on_index(key, index)
                if node is not None:
                    self._update_ft(x, index, node)

    def purge(self, node: ChordNode) -> None:
        for index in range(self.k):
            for finger_index in range(self.log_world_size):
                if self.ft[index][finger_index] == node:
                    self.ft[index][finger_index] = self