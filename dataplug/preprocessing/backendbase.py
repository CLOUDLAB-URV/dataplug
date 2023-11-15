from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dataplug.cloudobject import CloudObject
    from .preprocessor import (
        BatchPreprocessor,
        MapReducePreprocessor,
    )


class PreprocessorBackendBase:
    def run_batch_job(self, preprocessor: BatchPreprocessor, cloud_object: CloudObject):
        raise NotImplementedError()

    def run_mapreduce_job(self, preprocessor: MapReducePreprocessor, cloud_object: CloudObject):
        raise NotImplementedError()
