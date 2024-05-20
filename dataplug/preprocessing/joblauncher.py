from __future__ import annotations

import logging

from typing import TYPE_CHECKING

import botocore

if TYPE_CHECKING:
    from dataplug.cloudobject import CloudObject
    from .preprocessor import (
        BatchPreprocessor,
        MapReducePreprocessor,
    )

logger = logging.getLogger(__name__)


def preprocess_cloudobject(cloud_object: CloudObject):


    # FIXME implement this properly
    if issubclass(self._format_cls.preprocessor, BatchPreprocessor):
        batch_preprocessor: BatchPreprocessor = self._format_cls.preprocessor(*args, **kwargs)
        preprocessor_backend.setup()
        future = preprocessor_backend.submit_batch_job(batch_preprocessor, self)
        return future
    elif issubclass(self._format_cls.preprocessor, MapReducePreprocessor):
        mapreduce_preprocessor: MapReducePreprocessor = self._format_cls.preprocessor(*args, **kwargs)

        # Check mapreduce parameters
        if mapreduce_preprocessor.map_chunk_size is not None and mapreduce_preprocessor.num_mappers is not None:
            raise Exception('Setting both "map_chunk_size" and "num_mappers" is not allowed')

        if mapreduce_preprocessor.map_chunk_size is not None and mapreduce_preprocessor.num_mappers is None:
            # Calculate number of mappers from mapper chunk size
            mapreduce_preprocessor.num_mappers = math.ceil(self.size / mapreduce_preprocessor.map_chunk_size)
        elif mapreduce_preprocessor.map_chunk_size is None and mapreduce_preprocessor.num_mappers is not None:
            # Calculate mappers chunk size from number of mappers
            mapreduce_preprocessor.map_chunk_size = round(self.size / mapreduce_preprocessor.num_mappers)
        else:
            raise Exception(
                f'At least "map_chunk_size" or "num_mappers" parameter is required for {MapReducePreprocessor.__class__.__name__}'
            )

        preprocessor_backend.setup()
        future = preprocessor_backend.submit_mapreduce_job(mapreduce_preprocessor, self)
        return future
    else:
        raise Exception("This object cannot be preprocessed")
