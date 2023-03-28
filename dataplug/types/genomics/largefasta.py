from __future__ import annotations

import io
import logging
import pickle
import re
import time
from typing import TYPE_CHECKING

import pandas as pd

from ...cloudobject import CloudDataType

from ...preprocessing.preprocessor import MapReducePreprocessor, PreprocessingMetadata
from .fasta import FASTAPreprocessor

if TYPE_CHECKING:
    from typing import List
    from ...cloudobject import CloudObject

logger = logging.getLogger(__name__)


class LargeFASTAPreprocessor(FASTAPreprocessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def map(
            self, cloud_object: CloudObject, mapper_id: int, map_chunk_size: int, num_mappers: int
    ) -> PreprocessingMetadata:
        ...

    def reduce(
            self, map_results: List[PreprocessingMetadata], cloud_object: CloudObject, n_mappers: int
    ) -> PreprocessingMetadata:
        ...


@CloudDataType(preprocessor=FASTAPreprocessor)
class LargeFASTA:
    def __init__(self, cloud_object):
        self.cloud_object = cloud_object


