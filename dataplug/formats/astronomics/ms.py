from __future__ import annotations

import io
import logging
from math import ceil
from typing import TYPE_CHECKING

import pandas as pd

from ...entities import CloudDataFormat, CloudObjectSlice, PartitioningStrategy
from ...preprocessing.metadata import PreprocessingMetadata

if TYPE_CHECKING:
    from typing import List
    from ...cloudobject import CloudObject

logger = logging.getLogger(__name__)


def preprocess_ms(cloud_object: CloudObject) -> PreprocessingMetadata:
    return PreprocessingMetadata(
        attributes={
            # Here you must mut the key-value attributed pairs that were defined in the CloudDataFormat class
        },
        metadata=...,  # Here you can return any binary metadata that you want to store. You must take care of serialization.
        metadata_file_path=... # Or you can return a file path to the metadata file
    )

@CloudDataFormat(preprocessing_function=preprocess_ms)
class MS:
    attrb1: List[str]
    attrb2: List[str]

class MSSLice(CloudObjectSlice):
     def get(self):
        # Here you can consult your metadata generated for this format
        metadata = self.cloud_object.s3.get_object(
                Bucket=self.cloud_object.meta_path.bucket,
                Key=self.cloud_object.meta_path.bucket
        )["Body"]
        
        # Perform the necessary HTTP GET operations to get the data
        chunk = self.cloud_object.s3.get_object(
                Bucket=self.cloud_object.path.bucket,
                Key=self.cloud_object.path.bucket,
                Range=f"bytes={self.range_0}-{self.range_1}"
        )["Body"]
        
        # And finally return the actual chunked data        
        return chunk

@PartitioningStrategy(dataformat=MS)
def newformat_partitioning_strategy(cloud_object: CloudObject, num_chunks: int):
    slices = []
    for i in range(num_chunks):
        # Here you put the necessary logic for defining the byte ranges required to read a chunk of the data
        range_0 = ...
        range_1 = ...
        slice = MSSLice(range_0, range_1)
        slices.append(slice)
    return slices
