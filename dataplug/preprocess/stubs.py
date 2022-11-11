from dataclasses import dataclass

import boto3

from ..storage import PureS3Path


class BatchPreprocessor:
    def __init__(self):
        pass

    @staticmethod
    def preprocess(data_stream, meta):
        pass


class MapReducePreprocessor:
    def __init__(self):
        pass

    @staticmethod
    def map(data_stream, meta):
        raise NotImplementedError()

    @staticmethod
    def reduce(results, meta):
        raise NotImplementedError()


@dataclass
class PreprocesserMetadata:
    """
    Data Class structure containing the metadata used by the preprocesser class
    """
    s3: boto3.client
    obj_path: PureS3Path
    meta_path: PureS3Path
    worker_id: int
    chunk_size: int
    obj_size: int
    partitions: int = 1
