import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Tuple, BinaryIO, Dict, List, Type
from abc import ABC

import boto3

from ..storage import PureS3Path
from ..cloudobject import CloudObject

logger = logging.getLogger(__name__)


# @dataclass
# class PreprocessorMetadata:
#     """
#     Data Class structure containing the metadata used by the preprocessor class
#     """
#     s3: boto3.client
#     obj_path: PureS3Path
#     meta_path: PureS3Path
#     worker_id: int
#     chunk_size: int
#     obj_size: int
#     partitions: int = 1


class MetadataPreprocessor:
    """
    MetadataPreprocessor is used to extract metadata from an object without requiring to overwrite it
    """

    def __init__(self, *args, **kwargs):
        pass

    def extract_metadata(self, cloud_object: CloudObject) -> Dict[str, str]:
        raise NotImplementedError()


class BatchPreprocessor:
    def __init__(self, *args, **kwargs):
        pass

    def preprocess(self, data_stream: BinaryIO, meta: PreprocessorMetadata) -> Tuple[BinaryIO, Dict[str, str]]:
        pass


class MapReducePreprocessor:
    def __init__(self, *args, **kwargs):
        pass

    def map(self, data_stream: BinaryIO, meta: PreprocessorMetadata) -> BinaryIO:
        raise NotImplementedError()

    def reduce(self, results: List[BinaryIO], meta: PreprocessorMetadata) -> Tuple[BinaryIO, Dict[str, str]]:
        raise NotImplementedError()
