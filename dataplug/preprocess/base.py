from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Union, Optional

if TYPE_CHECKING:
    from ..cloudobject import CloudObject
    from .stubs import BatchPreprocessor, MapReducePreprocessor


class PreprocessorBackendBase(ABC):
    def do_preprocess(self,
                      preprocessor: Union[BatchPreprocessor, MapReducePreprocessor],
                      cloud_object: CloudObject,
                      chunk_size: Optional[int] = None,
                      num_workers: Optional[int] = None,
                      *args, **kwargs):
        raise NotImplementedError()
