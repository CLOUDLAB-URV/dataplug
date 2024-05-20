from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import BinaryIO, Dict, List, Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class PreprocessingMetadata:
    metadata: Optional[BinaryIO | bytes] = None
    metadata_file_path: Optional[str] = None
    attributes: Optional[Dict[str, Any]] = None
