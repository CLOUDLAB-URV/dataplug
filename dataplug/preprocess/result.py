from dataclasses import dataclass
from typing import BinaryIO, Dict, Any, AnyStr


@dataclass
class PreprocessingResult:
    metadata: BinaryIO = None
    object_body: BinaryIO = None
    attributes: Dict[AnyStr, Any] = None
