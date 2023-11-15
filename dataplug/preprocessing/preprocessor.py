from __future__ import annotations

import pickle
from typing import TYPE_CHECKING

from boto3.s3.transfer import TransferConfig

from ..util import force_delete_path

if TYPE_CHECKING:
    from dataplug.cloudobject import CloudObject
    from typing import List

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import BinaryIO, Dict, List, Any, Optional
    from dataplug.cloudobject import CloudObject

logger = logging.getLogger(__name__)


class BatchPreprocessor:
    """
    The BatchPreprocessor is used to preprocess a CloudObject in a single batch job, e.g. a single function call that
    preprocesses the entire object.
    """

    def __init__(self, *args, **kwargs):
        pass

    def preprocess(self, cloud_object: CloudObject) -> PreprocessingMetadata:
        """
        Preprocess function for batch preprocessing
        :param cloud_object: CloudObject instance to be preprocessed
        :return: tuple of preprocessed object as a bytearray and metadata attributes
        """
        raise NotImplementedError()


class MapReducePreprocessor:
    """
    The MapReducePreprocessor is used to preprocess a CloudObject as a map-reduce workflow. The 'map' function is called
    in parallel for each chunk of the object, and the 'reduce' function is called once with each mapper result as input.
    """

    def __init__(self, num_mappers: Optional[int] = None, map_chunk_size: Optional[int] = None, *args, **kwargs):
        self.num_mappers = num_mappers
        self.map_chunk_size = map_chunk_size

    def map(
        self,
        cloud_object: CloudObject,
        mapper_id: int,
        map_chunk_size: int,
        num_mappers: int,
    ) -> PreprocessingMetadata:
        """
        Map function for Map-Reduce preprocessor
        :param cloud_object: CloudObject instance to be preprocessed
        :param mapper_id: indicates the mapper ID in the map-reduce workflow
        :param map_chunk_size: chunk size in bytes preprocessed in this mapper function
        :param num_mappers: total number of mappers in the map-reduce workflow
        :return: mapper result as a bytearray
        """
        raise NotImplementedError()

    def reduce(
        self, map_results: List[PreprocessingMetadata], cloud_object: CloudObject, n_mappers: int
    ) -> PreprocessingMetadata:
        """
        Reduce function for Map-Reduce preprocessor
        :param cloud_object: CloudObject instance to be preprocessed
        :param map_results: List of mapper results
        :param n_mappers: total number of mappers in the map-reduce workflow
        :return: tuple of reduced result (preprocessed object) as a bytearray and metadata attributes
        """
        raise NotImplementedError()


@dataclass
class PreprocessingMetadata:
    metadata: Optional[BinaryIO | bytes] = None
    object_body: Optional[BinaryIO | bytes] = None
    object_file_path: Optional[str] = None
    attributes: Optional[Dict[str, Any]] = None


def checkout_preprocessing_output(preprocess_meta: PreprocessingMetadata, cloud_object: CloudObject):
    if all((preprocess_meta.object_body, preprocess_meta.object_file_path)):
        raise Exception("Choose one for object preprocessing result: object_body or object_file_path")

    # Upload object body to meta bucket with the same key as original (prevent to overwrite)
    if preprocess_meta.object_body is not None:
        if hasattr(preprocess_meta.object_body, "read"):
            cloud_object.storage.upload_fileobj(
                Fileobj=preprocess_meta.object_body,
                Bucket=cloud_object.meta_path.bucket,
                Key=cloud_object.path.key,
                ExtraArgs={"Metadata": {"dataplug": "dev"}},
                Config=TransferConfig(use_threads=True, max_concurrency=256),
            )
        else:
            cloud_object.storage.put_object(
                Body=preprocess_meta.object_body,
                Bucket=cloud_object.meta_path.bucket,
                Key=cloud_object.path.key,
                Metadata={"dataplug": "dev"},
            )
    if preprocess_meta.object_file_path is not None:
        cloud_object.storage.upload_file(
            Filename=preprocess_meta.object_file_path,
            Bucket=cloud_object.meta_path.bucket,
            Key=cloud_object.path.key,
            ExtraArgs={"Metadata": {"dataplug": "dev"}},
            Config=TransferConfig(use_threads=True, max_concurrency=256),
        )
        force_delete_path(preprocess_meta.object_file_path)

    # Upload attributes to meta bucket
    if preprocess_meta.attributes is not None:
        attrs_bin = pickle.dumps(preprocess_meta.attributes)
        cloud_object.storage.put_object(
            Body=attrs_bin,
            Bucket=cloud_object._attrs_path.bucket,
            Key=cloud_object._attrs_path.key,
            Metadata={"dataplug": "dev"},
        )

    # Upload metadata object to meta bucket
    if preprocess_meta.metadata is not None:
        if hasattr(preprocess_meta.metadata, "read"):
            cloud_object.storage.upload_fileobj(
                Fileobj=preprocess_meta.metadata,
                Bucket=cloud_object.meta_path.bucket,
                Key=cloud_object.meta_path.key,
                ExtraArgs={"Metadata": {"dataplug": "dev"}},
                Config=TransferConfig(use_threads=True, max_concurrency=256),
            )
        else:
            cloud_object.storage.put_object(
                Body=preprocess_meta.metadata,
                Bucket=cloud_object.meta_path.bucket,
                Key=cloud_object.meta_path.key,
                Metadata={"dataplug": "dev"},
            )

        if hasattr(preprocess_meta.metadata, "close"):
            preprocess_meta.metadata.close()


def batch_job_handler(preprocessor: BatchPreprocessor, cloud_object: CloudObject):
    preprocess_result = preprocessor.preprocess(cloud_object)
    checkout_preprocessing_output(preprocess_result, cloud_object)


def map_job_handler(
    preprocessor: MapReducePreprocessor, cloud_object: CloudObject, mapper_id: int
) -> PreprocessingMetadata:
    result = preprocessor.map(cloud_object, mapper_id, preprocessor.map_chunk_size, preprocessor.num_mappers)
    return result


def reduce_job_handler(
    preprocessor: MapReducePreprocessor, cloud_object: CloudObject, map_results: List[PreprocessingMetadata]
) -> PreprocessingMetadata:
    preprocess_result = preprocessor.reduce(map_results, cloud_object, n_mappers=len(map_results))
    checkout_preprocessing_output(preprocess_result, cloud_object)
