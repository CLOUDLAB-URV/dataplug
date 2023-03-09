from __future__ import annotations

from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .cloudobject import CloudObject


class CloudObjectSlice:
    def __init__(self, range_0=None, range_1=None):
        self.range_0: Optional[int] = range_0
        self.range_1: Optional[int] = range_1
        self.cloud_object: Optional[CloudObject] = None

    def get(self):
        raise NotImplementedError()
