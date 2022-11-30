from __future__ import annotations

from abc import ABC
from typing import TYPE_CHECKING, Union, Optional

if TYPE_CHECKING:
    from ..cloudobject import CloudObject
    from .preprocessor import MetadataPreprocessor, BatchPreprocessor, MapReducePreprocessor


class PreprocessorBackendBase:
    def preprocess_metadata(self, preprocessor: MetadataPreprocessor, cloud_object: CloudObject):
        raise NotImplementedError()

    def preprocess_batch(self, preprocessor: BatchPreprocessor, cloud_object: CloudObject):
        raise NotImplementedError()

    def preprocess_map_reduce(self, preprocessor: MapReducePreprocessor, cloud_object: CloudObject):
        raise NotImplementedError()
