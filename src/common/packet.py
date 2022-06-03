from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Optional

from simpy.events import Event

if TYPE_CHECKING:
    from common.node import Node


class PacketType(Enum):
    FIND_NODE = auto()
    SET_PRED = auto()
    SET_PRED_REPLY = auto()
    SET_SUCC = auto()
    SET_SUCC_REPLY = auto()
    GET_SUCC = auto()
    GET_SUCC_REPLY = auto()
    GET_PRED = auto()
    GET_PRED_REPLY = auto()
    FIND_VALUE_REPLY = auto()
    STORE_VALUE_REPLY = auto()
    GET_NODE = auto()
    GET_NODE_REPLY = auto()
    FIND_VALUE = auto()
    STORE_VALUE = auto()
    GET_VALUE = auto()
    SET_VALUE = auto()
    GET_VALUE_REPLY = auto()
    SET_VALUE_REPLY = auto()
    NOTIFY = auto()

    def is_reply(self):
        return "REPLY" in self.name


@dataclass
class Packet:
    ptype: PacketType
    id: int = field(init=False)
    data: Dict[str, Any] = field(default_factory=dict)
    sender: Optional[Node] = field(init=False, default=None)
    event: Optional[Event] = field(default=None)

    instances: ClassVar[int] = 0

    def __post_init__(self) -> None:
        self.id = Packet.instances
        Packet.instances += 1
