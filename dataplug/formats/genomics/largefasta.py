from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from dataplug.core.cloudobject import CloudDataFormatTemplate
from .fasta import FASTAPreprocessor
from ...preprocessing.preprocessor import PreprocessingMetadata

if TYPE_CHECKING:
    from typing import List
    from dataplug.core.cloudobject import CloudObject

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


@CloudDataFormatTemplate(preprocessor=FASTAPreprocessor)
class LargeFASTA:
    def __init__(self, cloud_object):
        self.cloud_object = cloud_object
