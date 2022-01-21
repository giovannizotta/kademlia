from common.utils import *
from common.node import Node


class ChordNode(Node):
    def __init__(
        self,
        env: simpy.Environment,
        _id: int,
        serve_mean: float = 0.8,
        timeout: int = 5,
        neigh: Node = None,
    ):
        super().__init__(env, _id, serve_mean, timeout, neigh)
