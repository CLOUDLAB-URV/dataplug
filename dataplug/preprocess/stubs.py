from dataclasses import dataclass
from typing import TYPE_CHECKING, Tuple, BinaryIO, Dict, List

import boto3

from ..storage import PureS3Path


@dataclass
class PreprocessorMetadata:
    """
    Data Class structure containing the metadata used by the preprocessor class
    """
    s3: boto3.client
    obj_path: PureS3Path
    meta_path: PureS3Path
    worker_id: int
    chunk_size: int
    obj_size: int
    partitions: int = 1


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
