from __future__ import annotations

import logging
import dill
from typing import TYPE_CHECKING
import multiprocessing

from ..backendbase import PreprocessorBackendBase
from ..preprocessor import batch_job_handler, map_job_handler, reduce_job_handler

if TYPE_CHECKING:
    from typing import Optional
    from dataplug.cloudobject import CloudObject
    from ..preprocessor import BatchPreprocessor, MapReducePreprocessor
else:
    CloudObject = object

logger = logging.getLogger(__name__)


def _map_reduce_sequential(preprocessor, cloud_object):
    map_results = []
    for mapper_id in range(preprocessor.num_mappers):
        map_result = map_job_handler(preprocessor, cloud_object, mapper_id)
        map_results.append(map_result)

    reduce_job_handler(preprocessor, cloud_object, map_results)


def _job_wrapper(f, payload):
    p = dill.loads(payload)
    result = f(*p)
    return dill.dumps(result)


class LocalPreprocessor(PreprocessorBackendBase):
    def __init__(self, use_threads: bool = False, debug: bool = False, max_workers: Optional[int] = None):
        self.use_threads = use_threads
        self.debug = debug
        self.max_workers = max_workers or multiprocessing.cpu_count()

    def run_batch_job(self, preprocessor: BatchPreprocessor, cloud_object: CloudObject):
        logger.info("Running batch preprocessing job for object %s", cloud_object)
        # Call batch job handler directly, it will block here and perform the preprocessing synchronously
        batch_job_handler(preprocessor, cloud_object)

    def run_mapreduce_job(self, preprocessor: MapReducePreprocessor, cloud_object: CloudObject):
        return _map_reduce_sequential(preprocessor, cloud_object)
