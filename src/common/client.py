import logging

from common.node import DHTNode, Node
from common.packet import Message, MessageType, Packet
from common.utils import DHTTimeoutError, SimpyProcess


class Client(Node):

    def __post_init__(self) -> None:
        super().__post_init__()

    def collect_load(self):
        pass

    def manage_packet(self, packet: Packet) -> None:
        return super().manage_packet(packet)

    def find_value(self, ask_to: DHTNode, key: str) -> SimpyProcess[None]:
        """Perform a find_value request and wait for the response"""
        self.log(
            f"Start looking for DHT[{key}], asking to {ask_to}", level=logging.INFO
        )
        before = self.env.now
        msg = Message(ptype=MessageType.FIND_VALUE, data=dict(key=key))
        sent_req = self.send_req(ask_to, msg)
        try:
            reply: Packet = yield from self.wait_resp(sent_req)
            value = reply.message.data["value"]
            hops = reply.message.data["hops"]
            if hops == -1:
                # the node responded but the process timed out
                raise DHTTimeoutError()
            self.log(f"Received value: DHT[{key}] = {value}", level=logging.INFO)
            after = self.env.now
            self.datacollector.client_requests.append((self.env.now, after - before, hops))
            self.datacollector.returned_value.append((self.env.now, key, value))
        except DHTTimeoutError:
            self.datacollector.timed_out_requests.append(self.env.now)
            self.log(f"Request for find key {key} timed out", level=logging.WARNING)

    def store_value(self, ask_to: DHTNode, key: str, value: int) -> SimpyProcess[None]:
        """Perform a store_value request and wait for the response"""
        self.log(
            f"Storing DHT[{key}] = {value}, asking to {ask_to}", level=logging.INFO
        )
        before = self.env.now
        msg = Message(ptype=MessageType.STORE_VALUE, data=dict(key=key, value=value))
        sent_req = self.send_req(ask_to, msg)
        try:
            reply = yield from self.wait_resp(sent_req)
            hops = reply.message.data["hops"]
            if hops == -1:
                # the node responded but the process timed out
                raise DHTTimeoutError()
            self.log(f"Stored value: DHT[{key}] = {value}", level=logging.INFO)
            after = self.env.now
            self.datacollector.client_requests.append((self.env.now, after - before, hops))
            self.datacollector.true_value.append((self.env.now, key, value))
        except DHTTimeoutError:
            self.datacollector.timed_out_requests.append(self.env.now)
            self.log(
                f"Request for  store key {key} : value {value} timed out",
                level=logging.WARNING,
            )
