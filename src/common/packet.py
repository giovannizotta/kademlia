from dataclasses import dataclass, field
from gettext import install
from typing import *

@dataclass
class Packet():
    instances: ClassVar[int] = 0
    id: int = field(init=False)
    data: Dict = field(default_factory=dict)
    
    def __post_init__(self):
        self.id = Packet.instances
        Packet.instances += 1
    
    
