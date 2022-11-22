from __future__ import annotations

from typing import TYPE_CHECKING, Union, Optional

from ..backendbase import PreprocessorBackendBase
from ..preprocessor import BatchPreprocessor, MapReducePreprocessor

if TYPE_CHECKING:
    from ...cloudobject import CloudObject
else:
    CloudObject = object


class LithopsPreprocessor(PreprocessorBackendBase):
    def __init__(self, lithops_config=None):
        self.lithops_config = lithops_config

    def do_preprocess(self,
                      preprocessor: Union[BatchPreprocessor, MapReducePreprocessor],
                      cloud_object: CloudObject,
                      chunk_size: Optional[int] = None,
                      num_workers: Optional[int] = None,
                      *args, **kwargs):
        raise NotImplementedError()
