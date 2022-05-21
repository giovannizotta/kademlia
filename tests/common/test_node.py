from common.node import *
from common.utils import *


class TestPacket:
    def test_id_increment(self):
        p1 = Packet(PacketType.GET_NODE, data=dict())
        p2 = Packet(PacketType.FIND_VALUE, data=dict())
        assert p1.id == 0
        assert p2.id == 1


class TestNode:
    class NonAbstractNode(Node):
        pass

    NonAbstractNode.__abstractmethods__ = set()

    env = simpy.Environment()
    dc = DataCollector()

    def test_init(self):
        n = "N1"
        mt = 1.0
        lws = 2
        mtd = 3.0
        qc = 10
        mst = 2
        n1 = self.NonAbstractNode(
            self.env,
            n,
            datacollector=self.dc,
            max_timeout=mt,
            log_world_size=lws,
            mean_transmission_delay=mtd,
            queue_capacity=qc,
            mean_service_time=mst
        )
        assert n1.env is self.env
        assert n1.name == n
        assert n1.datacollector is self.dc
        assert n1.max_timeout == mt
        assert n1.log_world_size == lws
        assert n1.mean_transmission_delay == mtd
        assert len(n1.in_queue.queue) == 0
        assert n1.queue_capacity == qc
        assert n1.mean_service_time == mst
        assert n1.id == n1._compute_key(n1.name)
