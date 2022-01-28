from dataclasses import dataclass, field
from gettext import install
from typing import *

@dataclass
class Packet():
    id: int = field(init=False)
    data: Dict = field(default_factory=dict)
    
    instances: ClassVar[int] = 0
    
    def __post_init__(self):
        self.id = Packet.instances
        Packet.instances += 1
    
    
