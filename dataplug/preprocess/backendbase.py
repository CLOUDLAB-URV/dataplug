from __future__ import annotations

from typing import TYPE_CHECKING, Union, Optional

if TYPE_CHECKING:
    from dataplug.cloudobject import CloudObject
    from dataplug.preprocess.preprocessor import BatchPreprocessor, MapReducePreprocessor


class PreprocessorBackendBase:
    def preprocess_batch(self, preprocessor: BatchPreprocessor, cloud_object: CloudObject):
        raise NotImplementedError()

    def preprocess_map_reduce(self, preprocessor: MapReducePreprocessor, cloud_object: CloudObject):
        raise NotImplementedError()
