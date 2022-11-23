from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Tuple, BinaryIO, Dict, List, Type, ByteString

if TYPE_CHECKING:
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

    def extract_metadata(self, data_stream: BinaryIO, cloud_object: CloudObject) -> Dict[str, str]:
        """
        Metadata extractor preprocessor function
        :param data_stream: object body stream to be preprocessed
        :param cloud_object: CloudObject instance to be preprocessed
        :return: metadata attributes
        """
        raise NotImplementedError()


class BatchPreprocessor:
    def __init__(self, *args, **kwargs):
        pass

    def preprocess(self, data_stream: BinaryIO, cloud_object: CloudObject) -> Tuple[ByteString, Dict[str, str]]:
        """
        Preprocess function for batch preprocessing
        :param data_stream: object body stream to be preprocessed
        :param cloud_object: CloudObject instance to be preprocessed
        :return: tuple of preprocessed object as a bytearray and metadata attributes
        """
        pass


class MapReducePreprocessor:
    def __init__(self, *args, **kwargs):
        pass

    def map(self, data_stream: BinaryIO, cloud_object: CloudObject,
            mapper: int, chunk_size: int, n_mappers: int) -> ByteString:
        """
        Map function for Map-Reduce preprocessor
        :param data_stream: object body stream to be preprocessed
        :param cloud_object: CloudObject instance to be preprocessed
        :param mapper: indicates the mapper ID in the map-reduce workflow
        :param chunk_size: chunk size in bytes preprocessed in this mapper function
        :param n_mappers: total number of mappers in the map-reduce workflow
        :return: mapper result as a bytearray
        """
        raise NotImplementedError()

    def reduce(self, results: List[BinaryIO], cloud_object: CloudObject,
               n_mappers: int) -> Tuple[ByteString, Dict[str, str]]:
        """
        Reduce function for Map-Reduce preprocessor
        :param cloud_object: CloudObject instance to be preprocessed
        :param results: List of mapper results
        :param n_mappers: total number of mappers in the map-reduce workflow
        :return: tuple of reduced result (preprocessed object) as a bytearray and metadata attributes
        """
        raise NotImplementedError()
