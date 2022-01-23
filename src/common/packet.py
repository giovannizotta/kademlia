from dataclasses import dataclass, field
from typing import *

@dataclass
class Packet():
    id: int = 0
    data: Dict = field(default_factory=dict)
    
