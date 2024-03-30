from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

try:
    import lithops
except ModuleNotFoundError:
    pass

from ..handler import batch_job_handler, map_job_handler, reduce_job_handler
from ..backendbase import PreprocessorBackendBase, PreprocessingJobFuture

if TYPE_CHECKING:
    from dataplug.core.cloudobject import CloudObject
    from ..preprocessor import BatchPreprocessor, MapReducePreprocessor
else:
    CloudObject = object

logger = logging.getLogger(__name__)


class LithopsPreprocessingJobFuture(PreprocessingJobFuture):
    def __init__(self, lithops_fexec, lithops_futures, export_stats=False, *args, **kwargs):
        self.lithops_fexec = lithops_fexec
        self.lithops_futures = lithops_futures
        self.export_stats = export_stats
        super().__init__(*args, **kwargs)

    def check_result(self) -> bool:
        self.lithops_fexec.get_result(self.lithops_futures)
        if self.export_stats:
            filename = self.lithops_fexec.executor_id + "_stats.json"
            with open(filename, "w") as output_file:
                logger.info("Saving Lithops execution stats to %s", filename)
                json.dump([f.stats for f in self.lithops_futures], output_file, indent=4)
        return True


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
        except ModuleNotFoundError:
            logger.error("Module 'lithops' not installed!")
        self.export_stats = export_stats
        self.lithops_kwargs = lithops_kwargs or {}
        super().__init__(*args, **kwargs)

    def setup(self):
        logger.info("Initializing LithopsPreprocessor")
        self.fexec = lithops.FunctionExecutor(**self.lithops_kwargs)

    def submit_batch_job(self, preprocessor: BatchPreprocessor, cloud_object: CloudObject) -> PreprocessingJobFuture:
        logger.info("Submit batch job on LithopsPreprocessor for object %s", cloud_object)
        fut = self.fexec.call_async(batch_job_handler, (preprocessor, cloud_object))
        return LithopsPreprocessingJobFuture(
            job_id=self.fexec.executor_id,
            lithops_fexec=self.fexec,
            lithops_futures=fut,
            export_stats=self.export_stats,
        )

    def submit_mapreduce_job(self, preprocessor: MapReducePreprocessor, cloud_object: CloudObject):
        fut = self.fexec.map_reduce(
            lithops_map_wrapper,
            range(preprocessor.num_mappers),
            lithops_reduce_wrapper,
            extra_args=(preprocessor, cloud_object),
            extra_args_reduce=(preprocessor, cloud_object),
        )
        return LithopsPreprocessingJobFuture(
            job_id=self.fexec.executor_id,
            lithops_fexec=self.fexec,
            lithops_futures=fut,
            export_stats=self.export_stats,
        )
