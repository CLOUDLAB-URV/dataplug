import logging
import math
from typing import TYPE_CHECKING, Union, Optional

from ..base import PreprocessorBackendBase
from ..stubs import BatchPreprocessor, MapReducePreprocessor, PreprocesserMetadata

if TYPE_CHECKING:
    from ...cloudobject import CloudObject
else:
    CloudObject = object

logger = logging.getLogger(__name__)


class LocalPreprocessor(PreprocessorBackendBase):
    def do_preprocess(self,
                      preprocessor: Union[BatchPreprocessor, MapReducePreprocessor],
                      cloud_object: CloudObject,
                      chunk_size: Optional[int] = None,
                      num_workers: Optional[int] = None):
        if issubclass(preprocessor, BatchPreprocessor):
            self._do_local_batch(cloud_object, preprocessor)
        elif issubclass(preprocessor, MapReducePreprocessor):
            self._do_local_mapreduce(cloud_object, preprocessor)

    @staticmethod
    def _do_local_batch(cloud_object, preprocessor):
        get_res = cloud_object.s3.get_object(Bucket=cloud_object.path.bucket, Key=cloud_object.path.key)
        logger.debug(get_res)
        obj_size = get_res['ContentLength']

        meta = PreprocesserMetadata(
            s3=cloud_object.s3,
            obj_path=cloud_object.path,
            meta_path=cloud_object.meta_path,
            worker_id=1,
            chunk_size=obj_size,
            obj_size=obj_size,
            partitions=1
        )

        result = preprocessor.preprocess(data_stream=get_res['Body'], meta=meta)

        try:
            body, meta = result
        except TypeError:
            raise Exception(f'Preprocessing result is {result}')

        if body is None or meta is None:
            raise Exception('Preprocessing result is {}'.format((body, meta)))

        cloud_object.s3.upload_fileobj(
            Fileobj=body,
            Bucket=cloud_object.path.bucket,
            Key=cloud_object.path.key,
            ExtraArgs={'Metadata': meta}
        )

        if hasattr(body, 'close'):
            body.close()

    @staticmethod
    def _do_local_mapreduce(cloud_object, preprocessor, chunk_size, num_workers):
        head_res = cloud_object.s3.head_object(Bucket=cloud_object.path.bucket, Key=cloud_object.path.key)
        print(head_res)
        obj_size = head_res['ContentLength']

        if chunk_size is not None and num_workers is not None:
            raise Exception('Both chunk_size and num_workers is not allowed')
        elif chunk_size is not None and num_workers is None:
            iterations = math.ceil(obj_size / chunk_size)
        elif chunk_size is None and num_workers is not None:
            iterations = num_workers
            chunk_size = round(obj_size / num_workers)
        else:
            raise Exception('At least chunk_size or num_workers parameter is required')

        map_results = []
        for i in range(iterations):
            r0 = i * chunk_size
            r1 = ((i * chunk_size) + chunk_size)
            r1 = r1 if r1 <= obj_size else obj_size
            get_res = cloud_object.s3.get_object(Bucket=cloud_object.path.bucket, Key=cloud_object.path.key,
                                                 Range=f'bytes={r0}-{r1}')

            meta = PreprocesserMetadata(
                s3=cloud_object.s3,
                obj_path=cloud_object.path,
                meta_path=cloud_object.meta_path,
                worker_id=i,
                chunk_size=chunk_size,
                obj_size=obj_size,
                partitions=iterations
            )

            result = preprocessor.map(data_stream=get_res['Body'], meta=meta)
            map_results.append(result)

            reduce_result, meta = preprocessor.__preprocesser.reduce(map_results, cloud_object.s3)
