from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typing import BinaryIO, Dict, List, Any, Optional
    from dataplug.cloudobject import CloudObject

logger = logging.getLogger(__name__)


class BatchPreprocessor:
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
    object_body: Optional[BinaryIO] = None
    object_file_path: Optional[str] = None
    attributes: Optional[Dict[str, Any]] = None
