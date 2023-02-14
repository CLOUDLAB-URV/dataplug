from __future__ import annotations

import math
import pickle
from typing import TYPE_CHECKING, List

from boto3.s3.transfer import TransferConfig

from .preprocessor import BatchPreprocessor, MapReducePreprocessor, PreprocessingMetadata
from ..util import force_delete_path
from ..version import __version__

if TYPE_CHECKING:
    from ..cloudobject import CloudObject


def batch_job_handler(preprocessor: BatchPreprocessor, cloud_object: CloudObject):
    # Call preprocess code
    preprocess_result = preprocessor.preprocess(cloud_object)

    if all((preprocess_result.object_body, preprocess_result.object_file_path)):
        raise Exception("Choose one for object preprocessing result: object_body or object_file_path")

    # Upload object body to meta bucket with the same key as original (prevent to overwrite)
    if preprocess_result.object_body is not None:
        if hasattr(preprocess_result.object_body, "read"):
            cloud_object.s3.upload_fileobj(
                Fileobj=preprocess_result.object_body,
                Bucket=cloud_object.meta_path.bucket,
                Key=cloud_object.path.key,
                ExtraArgs={"Metadata": {"dataplug": __version__}},
                Config=TransferConfig(use_threads=True, max_concurrency=256),
            )
        else:
            cloud_object.s3.put_object(
                Body=preprocess_result.object_body,
                Bucket=cloud_object.meta_path.bucket,
                Key=cloud_object.path.key,
                Metadata={"dataplug": __version__},
            )
    if preprocess_result.object_file_path is not None:
        cloud_object.s3.upload_file(
            Filename=preprocess_result.object_file_path,
            Bucket=cloud_object.meta_path.bucket,
            Key=cloud_object.path.key,
            ExtraArgs={"Metadata": {"dataplug": __version__}},
            Config=TransferConfig(use_threads=True, max_concurrency=256),
        )
        force_delete_path(preprocess_result.object_file_path)

    # Upload attributes to meta bucket
    if preprocess_result.attributes is not None:
        attrs_bin = pickle.dumps(preprocess_result.attributes)
        cloud_object.s3.put_object(
            Body=attrs_bin,
            Bucket=cloud_object._attrs_path.bucket,
            Key=cloud_object._attrs_path.key,
            Metadata={"dataplug": __version__},
        )

    # Upload metadata object to meta bucket
    if preprocess_result.metadata is not None:
        if hasattr(preprocess_result.metadata, "read"):
            cloud_object.s3.upload_fileobj(
                Fileobj=preprocess_result.metadata,
                Bucket=cloud_object.meta_path.bucket,
                Key=cloud_object.meta_path.key,
                ExtraArgs={"Metadata": {"dataplug": __version__}},
                Config=TransferConfig(use_threads=True, max_concurrency=256),
            )
        else:
            cloud_object.s3.put_object(
                Body=preprocess_result.metadata,
                Bucket=cloud_object.meta_path.bucket,
                Key=cloud_object.meta_path.key,
                Metadata={"dataplug": __version__},
            )

        if hasattr(preprocess_result.metadata, "close"):
            preprocess_result.metadata.close()


def map_job_handler(preprocessor: MapReducePreprocessor, cloud_object: CloudObject, mapper_id: int):
    # Call map process code
    result = preprocessor.map(cloud_object, mapper_id, preprocessor.map_chunk_size, preprocessor.num_mappers)
    return result


def reduce_job_handler(
    preprocessor: MapReducePreprocessor, cloud_object: CloudObject, map_results: List[PreprocessingMetadata]
):
    # Call reduce process code
    preprocessor.reduce(map_results, cloud_object, n_mappers=len(map_results))
