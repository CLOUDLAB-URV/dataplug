from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Union, Optional

import lithops

from dataplug.preprocess.backendbase import PreprocessorBackendBase, PreprocessingJobFuture
from dataplug.preprocess.preprocessor import BatchPreprocessor, MapReducePreprocessor
from dataplug.preprocess.handler import batch_job_handler, map_job_handler, reduce_job_handler

if TYPE_CHECKING:
    from ...cloudobject import CloudObject
else:
    CloudObject = object

logger = logging.getLogger(__name__)


class LithopsPreprocessingJobFuture(PreprocessingJobFuture):
    def __init__(self, lithops_fexec, lithops_futures, *args, **kwargs):
        self.lithops_fexec = lithops_fexec
        self.lithops_futures = lithops_futures
        super().__init__(*args, **kwargs)

    def check_result(self) -> bool:
        self.lithops_fexec.get_result(self.lithops_futures)
        return True


def lithops_map_wrapper(mapper_id, preprocessor, cloud_object):
    map_result = map_job_handler(preprocessor, cloud_object, mapper_id)
    return map_result


def lithops_reduce_wrapper(results, preprocessor, cloud_object):
    reduce_job_handler(preprocessor, cloud_object, results)


class LithopsPreprocessor(PreprocessorBackendBase):
    fexec: lithops.FunctionExecutor

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def setup(self, *args, **kwargs):
        logger.info("Initializing LithopsPreprocessor")
        self.fexec = lithops.FunctionExecutor(*args, **kwargs)

    def submit_batch_job(self, preprocessor: BatchPreprocessor, cloud_object: CloudObject) -> PreprocessingJobFuture:
        logger.info("Submit batch job on LithopsPreprocessor for object %s", cloud_object)
        fut = self.fexec.call_async(batch_job_handler, (preprocessor, cloud_object))
        return LithopsPreprocessingJobFuture(job_id="", lithops_fexec=self.fexec, lithops_futures=fut)

    def submit_mapreduce_job(self, preprocessor: MapReducePreprocessor, cloud_object: CloudObject):
        fut = self.fexec.map_reduce(
            lithops_map_wrapper,
            range(preprocessor.num_mappers),
            lithops_reduce_wrapper,
            extra_args=(preprocessor, cloud_object),
            extra_args_reduce=(preprocessor, cloud_object),
        )
        return LithopsPreprocessingJobFuture(job_id="", lithops_fexec=self.fexec, lithops_futures=fut)
