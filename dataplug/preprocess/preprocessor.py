from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Tuple, BinaryIO, Dict, List, Type, ByteString, Union

if TYPE_CHECKING:
    from dataplug.cloudobject import CloudObject
    from dataplug.preprocess.result import Metadata

logger = logging.getLogger(__name__)


class BatchPreprocessor:
    def __init__(self, *args, **kwargs):
        pass

    def preprocess(self, data_stream: BinaryIO, cloud_object: CloudObject) -> Metadata:
        """
        Preprocess function for batch preprocessing
        :param data_stream: object body stream to be preprocessed
        :param cloud_object: CloudObject instance to be preprocessed
        :return: tuple of preprocessed object as a bytearray and metadata attributes
        """
        raise NotImplementedError()


class MapReducePreprocessor:
    def __init__(self, *args, **kwargs):
        pass

    def map(self, data_stream: BinaryIO, cloud_object: CloudObject,
            mapper: int, chunk_size: int, n_mappers: int) -> Metadata:
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

    def reduce(self, results: List[BinaryIO], cloud_object: CloudObject, n_mappers: int) -> Metadata:
        """
        Reduce function for Map-Reduce preprocessor
        :param cloud_object: CloudObject instance to be preprocessed
        :param results: List of mapper results
        :param n_mappers: total number of mappers in the map-reduce workflow
        :return: tuple of reduced result (preprocessed object) as a bytearray and metadata attributes
        """
        raise NotImplementedError()
