from common.node import DHTNode, Node, Packet, PacketType
from common.utils import *


class Client(Node):
    def __post_init__(self) -> None:
        super().__post_init__()
        self.max_timeout = self.max_timeout * 6

    def manage_packet(self, packet: Packet) -> None:
        return super().manage_packet(packet)
        
    def find_value(self, ask_to: DHTNode, key: str) -> SimpyProcess[None]:
        """Perform a find_value request and wait for the response"""
        self.log(
            f"Start looking for DHT[{key}], asking to {ask_to}", level=logging.INFO)
        before = self.env.now
        key_hash = self._compute_key(key, self.log_world_size)
        packet = Packet(ptype=PacketType.FIND_VALUE, data=dict(key=key_hash))
        sent_req = self.send_req(ask_to, packet)
        try:
            packet = yield from self.wait_resp(sent_req)
            value = packet.data["value"]
            hops = packet.data["hops"]
            if hops == -1:
                # the node responded but the process timed out
                raise DHTTimeoutError()
            self.log(
                f"Received value: DHT[{key}] = {value}", level=logging.INFO)
            after = self.env.now
            self.datacollector.client_requests.append((after - before, hops))
        except DHTTimeoutError:
            self.datacollector.timed_out_requests += 1
            self.log(
                f"Request for find key {key} timed out", level=logging.WARNING)

    def store_value(self, ask_to: DHTNode, key: str, value: int) -> SimpyProcess[None]:
        """Perform a store_value request and wait for the response"""
        self.log(
            f"Storing DHT[{key}] = {value}, asking to {ask_to}", level=logging.INFO)
        before = self.env.now
        key_hash = self._compute_key(key, self.log_world_size)
        packet = Packet(ptype=PacketType.STORE_VALUE,
                        data=dict(key=key_hash, value=value))
        sent_req = self.send_req(ask_to, packet)
        try:
            packet = yield from self.wait_resp(sent_req)
            hops = packet.data["hops"]
            if hops == -1:
                # the node responded but the process timed out
                raise DHTTimeoutError()
            self.log(f"Stored value: DHT[{key}] = {value}", level=logging.INFO)
            after = self.env.now
            self.datacollector.client_requests.append((after - before, hops))
        except DHTTimeoutError:
            self.datacollector.timed_out_requests += 1
            self.log(
                f"Request for  store key {key} : value {value} timed out", level=logging.WARNING)
