from chord.node import *
from common.utils import *


class TestChordNode:
    def test_init(self, env, dc):
        mt = 1.0
        lws = 2
        mtd = 3.0
        qc = 10
        mst = 2
        k = 7
        sp = 30
        ss = 40
        sm = 50
        ufp = 500
        ufs = 600
        ufm = 700
        n1 = ChordNode(
            env,
            datacollector=dc,
            max_timeout=mt,
            log_world_size=lws,
            mean_transmission_delay=mtd,
            queue_capacity=qc,
            mean_service_time=mst,
            k=k,
            STABILIZE_PERIOD=sp,
            STABILIZE_STDDEV=ss,
            STABILIZE_MINCAP=sm,
            UPDATE_FINGER_PERIOD=ufp,
            UPDATE_FINGER_STDDEV=ufs,
            UPDATE_FINGER_MINCAP=ufm
        )
        assert n1.env is env
        assert n1.name == "ChordNode_000"
        assert n1.datacollector is dc
        assert n1.max_timeout == mt
        assert n1.log_world_size == lws
        assert n1.mean_transmission_delay == mtd
        assert len(n1.in_queue.queue) == 0
        assert n1.queue_capacity == qc
        assert n1.mean_service_time == mst
        assert n1.id == n1._compute_key(n1.name)
        assert n1.STABILIZE_PERIOD == sp
        assert n1.STABILIZE_STDDEV == ss
        assert n1.STABILIZE_MINCAP == sm
        assert n1.UPDATE_FINGER_PERIOD == ufp
        assert n1.UPDATE_FINGER_STDDEV == ufs
        assert n1.UPDATE_FINGER_MINCAP == ufm
        assert len(n1.ids) == k
        assert all(id == n1._compute_key(f"{n}_{i}")
                   for i, id in enumerate(n1.ids))
        assert len(n1.ft) == k
        assert all(len(ft_id) == lws for ft_id in n1.ft)
        assert len(n1.succ) == k
        assert len(n1.pred) == k
