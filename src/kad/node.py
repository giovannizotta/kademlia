from kad.rbg import RandomBatchGenerator as RBG
from kad.utils import *


class Node:
    def __init__(self, env: simpy.Environment, _id: int, serve_mean: float = 0.8):
        self.env = env
        self.id = _id
        self.serve_mean = serve_mean
        self.rbg = RBG()

    def serve_request(self, packet: int) -> SimpyProcess:
        """Serve a request for the given packet"""
        to = self.rbg.get_exponential(self.serve_mean)
        yield self.env.timeout(to)
        print(f"Node {self.id} serve packet {packet}")
