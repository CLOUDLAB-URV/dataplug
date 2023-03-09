from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..cloudobject import CloudObject
    from .preprocessor import (
        BatchPreprocessor,
        MapReducePreprocessor,
    )


class PreprocessingJobFuture:
    def __init__(self, job_id: str):
        self.job_id: str = job_id

    def check_result(self) -> bool:
        raise NotImplementedError()


class PreprocessorBackendBase:
    def setup(self):
        raise NotImplementedError()

    def submit_batch_job(self, preprocessor: BatchPreprocessor, cloud_object: CloudObject) -> PreprocessingJobFuture:
        raise NotImplementedError()

    def submit_mapreduce_job(self, preprocessor: MapReducePreprocessor, cloud_object: CloudObject):
        raise NotImplementedError()
