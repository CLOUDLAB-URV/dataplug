from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING, Union, Optional
from boto3.s3.transfer import TransferConfig
import smart_open

from ..backendbase import PreprocessorBackendBase
from ..preprocessor import BatchPreprocessor, MapReducePreprocessor
from ...util import dump_attributes

if TYPE_CHECKING:
    from ...cloudobject import CloudObject
else:
    CloudObject = object

logger = logging.getLogger(__name__)


class DummyPreprocessor(PreprocessorBackendBase):
    def preprocess_batch(self, preprocessor: BatchPreprocessor, cloud_object: CloudObject):
        obj_uri = cloud_object.path.as_uri()
        client = cloud_object.s3._new_client()
        stream = smart_open.open(obj_uri, 'rb', transport_params={'client': client})
        preprocess_result = preprocessor.preprocess(stream, cloud_object)

        if preprocess_result.attributes is not None:
            attrs_dict = dump_attributes(preprocess_result.attributes)
        else:
            attrs_dict = {}

        if preprocess_result.metadata is None:
            preprocess_result.metadata = io.BytesIO(b"")

        if hasattr(stream, 'read'):
            cloud_object.s3.upload_fileobj(
                Fileobj=preprocess_result.metadata,
                Bucket=cloud_object.meta_path.bucket,
                Key=cloud_object.path.key,
                ExtraArgs={'Metadata': attrs_dict},
                Config=TransferConfig(use_threads=True, max_concurrency=256)
            )
        else:
            cloud_object.s3.put_object(
                Body=preprocess_result.metadata,
                Bucket=cloud_object.meta_path.bucket,
                Key=cloud_object.path.key,
                Metadata=attrs_dict
            )

        if hasattr(stream, 'close'):
            stream.close()

    def preprocess_map_reduce(self, preprocessor: MapReducePreprocessor, cloud_object: CloudObject):
        raise NotImplementedError()
        # head_res = cloud_object.s3.head_object(Bucket=cloud_object.path.bucket, Key=cloud_object.path.key)
        # print(head_res)
        # obj_size = head_res['ContentLength']
        #
        # if chunk_size is not None and num_workers is not None:
        #     raise Exception('Both chunk_size and num_workers is not allowed')
        # elif chunk_size is not None and num_workers is None:
        #     iterations = math.ceil(obj_size / chunk_size)
        # elif chunk_size is None and num_workers is not None:
        #     iterations = num_workers
        #     chunk_size = round(obj_size / num_workers)
        # else:
        #     raise Exception('At least chunk_size or num_workers parameter is required')
        #
        # map_results = []
        # for i in range(iterations):
        #     r0 = i * chunk_size
        #     r1 = ((i * chunk_size) + chunk_size)
        #     r1 = r1 if r1 <= obj_size else obj_size
        #     get_res = cloud_object.s3.get_object(Bucket=cloud_object.path.bucket, Key=cloud_object.path.key,
        #                                          Range=f'bytes={r0}-{r1}')
        #
        #     meta = PreprocessorMetadata(
        #         s3=cloud_object.s3,
        #         obj_path=cloud_object.path,
        #         meta_path=cloud_object.meta_path,
        #         worker_id=i,
        #         chunk_size=chunk_size,
        #         obj_size=obj_size,
        #         partitions=iterations
        #     )
        #
        #     result = preprocessor.map(data_stream=get_res['Body'], meta=meta)
        #     map_results.append(result)
        #
        #     reduce_result, meta = preprocessor.__preprocesser.reduce(map_results, cloud_object.s3)
