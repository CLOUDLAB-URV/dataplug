from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ..handler import batch_job_handler, map_job_handler, reduce_job_handler
from ..backendbase import PreprocessorBackendBase, PreprocessingJobFuture

if TYPE_CHECKING:
    from ...cloudobject import CloudObject
    from ..preprocessor import BatchPreprocessor, MapReducePreprocessor
else:
    CloudObject = object

logger = logging.getLogger(__name__)


class DummyPreprocessingJobFuture(PreprocessingJobFuture):
    def check_result(self):
        # Do nothing, if preprocessing did not raise Exception, assume preprocessing job succeeded
        return True


class DummyPreprocessor(PreprocessorBackendBase):
    def setup(self, *args, **kwargs):
        logger.info("Initializing DummyPreprocessor")

    def submit_batch_job(self, preprocessor: BatchPreprocessor, cloud_object: CloudObject) -> PreprocessingJobFuture:
        logger.info("Submit batch job on DummyPreprocessor for object %s", cloud_object)
        # Call batch job handler directly, it will block here and perform the preprocessing synchronously
        batch_job_handler(preprocessor, cloud_object)
        return DummyPreprocessingJobFuture(job_id="")

    def submit_mapreduce_job(self, preprocessor: MapReducePreprocessor, cloud_object: CloudObject):
        map_results = []
        for mapper_id in range(preprocessor.num_mappers):
            # Same as batch job
            map_result = map_job_handler(preprocessor, cloud_object, mapper_id)
            map_results.append(map_result)

        reduce_job_handler(preprocessor, cloud_object, map_results)
        return DummyPreprocessingJobFuture(job_id="")
