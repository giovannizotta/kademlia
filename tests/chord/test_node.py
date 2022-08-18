from chord.node import ChordNode


class TestChordNode:
    def test_init(self, env, dc):
        loc = (0, 0)
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
            dc,
            location=loc,
            max_timeout=mt,
            log_world_size=lws,
            queue_capacity=qc,
            mean_service_time=mst,
            k=k,
            stabilize_period=sp,
            stabilize_stddev=ss,
            stabilize_mincap=sm,
            update_finger_period=ufp,
            update_finger_stddev=ufs,
            update_finger_mincap=ufm,
        )
        assert n1.env is env
        assert n1.name == f"ChordNode_{0:05d}"
        assert n1.datacollector is dc
        assert n1.max_timeout == mt
        assert n1.log_world_size == lws
        assert len(n1.in_queue.queue) == 0
        assert n1.queue_capacity == qc
        assert n1.mean_service_time == mst
        assert n1.id == n1._compute_key(n1.name)
        assert n1.stabilize_period == sp
        assert n1.stabilize_stddev == ss
        assert n1.stabilize_mincap == sm
        assert n1.update_finger_period == ufp
        assert n1.update_finger_stddev == ufs
        assert n1.update_finger_mincap == ufm
        assert len(n1.ids) == k
        assert all(
            id == n1._compute_key(f"{n1.name}_{i}") for i, id in enumerate(n1.ids)
        )
        assert len(n1.ft) == k
        assert all(len(ft_id) == lws for ft_id in n1.ft)
        assert len(n1.succ) == k
        assert len(n1.pred) == k
