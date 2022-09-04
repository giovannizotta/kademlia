import simpy

from kad.node import KadNode
import pytest

class TestKadNode:
    @pytest.fixture
    def conf_five(self, env, dc):
        locations = [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4)]
        timeout = 10
        log_world_size = 160
        queue_capacity = 100
        mean_service_time = 0.1
        n1 = KadNode(env, dc, locations[0], timeout, log_world_size, queue_capacity, mean_service_time, 2, 5)
        n2 = KadNode(env, dc, locations[1], timeout, log_world_size, queue_capacity, mean_service_time, 2, 5)
        n3 = KadNode(env, dc, locations[2], timeout, log_world_size, queue_capacity, mean_service_time, 2, 5)
        n4 = KadNode(env, dc, locations[3], timeout, log_world_size, queue_capacity, mean_service_time, 2, 5)
        n5 = KadNode(env, dc, locations[4], timeout, log_world_size, queue_capacity, mean_service_time, 2, 5)
        return n1, n2, n3, n4, n5, env

    def test_find_node_bounded_parallelism(self, conf_five):
        # n1: candidates: {n2, n3, n4}
        # n1 ---get-nodes--> n2, n3
        # n2 ---reply---> {n3, n4}        | n1 update candidates: {n1, n2, n3, n4}     | n1 ---get-nodes--> n4
        # n3 ---reply---> {n2, n5}        | n1 update candidates: {n2, n3, n4, n5}     | n1 ---get-nodes--> n5
        # n4  does not reply              |                                            |
        # n5 ---reply---> {n1, n2}        | n1 update candidates: {n1, n2, n3, n4, n5} |
        n1: KadNode
        n2: KadNode
        n3: KadNode
        n4: KadNode
        n5: KadNode
        env: simpy.Environment
        n1, n2, n3, n4, n5, env = conf_five

        n4.crash()
        n1.update_bucket(n2)
        n1.update_bucket(n3)
        n1.update_bucket(n4)
        n2.update_bucket(n3)
        n2.update_bucket(n4)
        n3.update_bucket(n2)
        n3.update_bucket(n5)
        n5.update_bucket(n1)
        n5.update_bucket(n2)
        candidates = []
        def run_find():
            tmp, hops = yield from n1.unzip_find("asd", env.all_of)
            candidates.extend(tmp)
        env.process(run_find())
        env.run()
        assert set(candidates) == {n1, n2, n3, n4, n5}