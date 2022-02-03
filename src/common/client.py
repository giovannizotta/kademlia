from common.node import DHTNode, Node, Packet
from common.utils import *


class Client(Node):
    def __post_init__(self) -> None:
        super().__post_init__()

    def find_value(self, ask_to: DHTNode, key: str) -> SimpyProcess[None]:
        self.log(f"Start looking for DHT[{key}], asking to {ask_to}", level=logging.INFO)
        key_hash = self._compute_key(key, self.log_world_size)
        packet = Packet(data=dict(key=key_hash))
        sent_req = self.send_req(ask_to.find_value, packet)
        try:
            packet = yield from self.wait_resp(sent_req)
            value = packet.data["value"]
            self.log(f"Received value: DHT[{key}] = {value}", level=logging.INFO)
        except DHTTimeoutError:
            self.log(
                f"Request for find key {key} timed out", level=logging.WARNING)

    def store_value(self, ask_to: DHTNode, key: str, value: int) -> SimpyProcess[None]:
        self.log(f"Storing DHT[{key}] = {value}, asking to {ask_to}", level=logging.INFO)
        key_hash = self._compute_key(key, self.log_world_size)
        packet = Packet(data=dict(key=key_hash, value=value))
        sent_req = self.send_req(ask_to.store_value, packet)
        try:
            packet = yield from self.wait_resp(sent_req)
            self.log(f"Stored value: DHT[{key}] = {value}", level=logging.INFO)
        except DHTTimeoutError:
            self.log(
                f"Request for  store key {key} : value {value} timed out", level=logging.WARNING)