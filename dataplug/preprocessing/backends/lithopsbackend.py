from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

try:
    import lithops
except ModuleNotFoundError:
    pass

from ..backendbase import PreprocessorBackendBase
from ..preprocessor import batch_job_handler, map_job_handler, reduce_job_handler

if TYPE_CHECKING:
    from dataplug.cloudobject import CloudObject
    from ..preprocessor import BatchPreprocessor, MapReducePreprocessor
else:
    CloudObject = object

logger = logging.getLogger(__name__)


def lithops_map_wrapper(mapper_id, preprocessor, cloud_object):
    map_result = map_job_handler(preprocessor, cloud_object, mapper_id)
    return map_result


def lithops_reduce_wrapper(results, preprocessor, cloud_object):
    reduce_job_handler(preprocessor, cloud_object, results)


class LithopsPreprocessor(PreprocessorBackendBase):
    fexec: lithops.FunctionExecutor

    def __init__(self, export_stats=False, lithops_kwargs=None, *args, **kwargs):
        try:
            import lithops
        except ModuleNotFoundError as e:
            logger.error("You need to install 'lithops' before using the LithopsPreprocessor backend")
            raise e

        self.export_stats = export_stats
        self.lithops_kwargs = lithops_kwargs or {}
        super().__init__(*args, **kwargs)

    def run_batch_job(self, preprocessor: BatchPreprocessor, cloud_object: CloudObject) -> PreprocessingJobFuture:
        logger.info("Submit batch job on LithopsPreprocessor for object %s", cloud_object)
        fut = self.fexec.call_async(batch_job_handler, (preprocessor, cloud_object))
        return fut.get_result()

    def run_mapreduce_job(self, preprocessor: MapReducePreprocessor, cloud_object: CloudObject):
        fut = self.fexec.map_reduce(
            lithops_map_wrapper,
            range(preprocessor.num_mappers),
            lithops_reduce_wrapper,
            extra_args=(preprocessor, cloud_object),
            extra_args_reduce=(preprocessor, cloud_object),
        )
        return fut.get_result()
