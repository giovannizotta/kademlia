import types
from functools import partial
from typing import Type

import pytest

from chord.node import ChordNode
from common.node import DHTNode, Node
from common.packet import Message, MessageType, Packet
from kad.node import KadNode


@pytest.fixture(scope="module")
def cls() -> Type[Node]:
    class NonAbstractNode(Node):
        pass

    NonAbstractNode.__abstractmethods__ = set()
    return NonAbstractNode


@pytest.fixture
def conf_two(cls, env, dc):
    location1, location2 = (0, 0), (1, 1)
    timeout = 10
    log_world_size = 160
    queue_capacity = 100
    mean_service_time = 0.1
    n1 = cls(env, dc, location1, timeout, log_world_size, queue_capacity, mean_service_time)
    n2 = cls(env, dc, location2, timeout, log_world_size, queue_capacity, mean_service_time)
    return n1, n2, env


class TestPacket:
    def test_id_increment(self, conf_two):
        """
        Test packet ids are assigned incrementally
        """
        n1, n2, _ = conf_two
        p1 = Packet(n1, Message(MessageType.GET_NODE, data=dict()))
        p2 = Packet(n2, Message(MessageType.FIND_NODE, data=dict()))
        assert p1.id == 0
        assert p2.id == 1


class TestNode:

    def test_send_recv_discard(self, conf_two):
        """
        Test limited size buffers.
        When sending 4 messages with a buffer of size 2:
        - one is served
        - two are buffered
        - one is discarded
        """
        n1, n2, env = conf_two
        n2.queue_capacity = 2
        n1.mean_transmission_delay = 0.001

        p1 = Message(MessageType.GET_NODE, data=dict())
        p2 = Message(MessageType.GET_NODE, data=dict())
        p3 = Message(MessageType.GET_NODE, data=dict())
        p4 = Message(MessageType.GET_NODE, data=dict())
        # replace manage packet with a stub function that stores the received packets
        recv_packets = []

        def stub(_: Node, packet: Packet) -> None:
            recv_packets.append(packet)

        n2.manage_packet = types.MethodType(stub, n2)

        n1.send_req(n2, p1)  # immediately served
        n1.send_req(n2, p2)  # queued
        n1.send_req(n2, p3)  # queued
        n1.send_req(n2, p4)  # discarded

        env.run()
        assert len(recv_packets) == 3

    def test_send_recv_reply(self, conf_two):
        """
        Test send-reply mechanism.
        """
        n1, n2, env = conf_two
        p1 = Message(MessageType.GET_NODE, data=dict())
        p2 = Message(MessageType.GET_NODE, data=dict())

        def echo(self: Node, packet: Packet) -> None:
            resp = Message(
                MessageType.GET_NODE_REPLY,
                data=packet.message.data,
                event=packet.message.event,
            )
            self.send_resp(packet.sender, resp)

        n2.manage_packet = types.MethodType(echo, n2)

        sent_reqs = [n1.send_req(n2, p1), n1.send_req(n2, p2)]
        packets = []

        def proc():
            yield from n1.wait_resps(sent_reqs, packets)

        env.process(proc())
        env.run()
        assert len(packets) == 2
        assert all(p.sender is n2 for p in packets)


class TestDHTNode:

    @pytest.fixture(params=[
        partial(ChordNode, k=1, stabilize_period=100, stabilize_stddev=1, stabilize_mincap=10, update_finger_period=100,
                update_finger_stddev=1, update_finger_mincap=100),
        partial(KadNode, alpha=1, k=1),
    ])
    def dht_cls(self, request) -> Type[DHTNode]:
        return request.param

    @pytest.fixture
    def conf_two(self, dht_cls, env, dc):
        location1, location2 = (0, 0), (1, 1)
        timeout = 10
        log_world_size = 160
        queue_capacity = 100
        mean_service_time = 0.1
        n1 = dht_cls(env, dc, location1, timeout, log_world_size, queue_capacity, mean_service_time)
        n2 = dht_cls(env, dc, location2, timeout, log_world_size, queue_capacity, mean_service_time)
        return n1, n2, env

    @pytest.fixture()
    def key(self) -> str:
        return "K"

    @pytest.fixture()
    def value(self) -> str:
        return "V"

    def test_set_value(self, conf_two, key, value):
        """
        Test set value request.
        """
        n1, n2, env = conf_two
        p = Message(MessageType.SET_VALUE, data=dict(key=key, value=value))
        n1.send_req(n2, p)
        env.run()
        assert n2.ht[key] == value

    def test_get_value(self, conf_two, key, value):
        """
        Test get value request.
        """
        n1, n2, env = conf_two
        n2.ht[key] = value
        p = Message(MessageType.GET_VALUE, data=dict(key=key))
        req = n1.send_req(n2, p)
        ans = env.run(req).message.data.get("value")
        assert ans == value
