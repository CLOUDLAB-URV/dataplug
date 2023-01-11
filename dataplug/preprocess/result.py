from dataclasses import dataclass
from typing import BinaryIO, Dict, Any


@dataclass
class Metadata:
    metadata: BinaryIO = None
    object_body: BinaryIO = None
    attributes: Dict[str, Any] = None
