from __future__ import annotations

import io
import logging
import os
import tarfile
import re
from math import ceil
from typing import TYPE_CHECKING
from casacore.tables import table

import pandas as pd

from ...entities import CloudDataFormat, CloudObjectSlice, PartitioningStrategy
from ...preprocessing.metadata import PreprocessingMetadata

if TYPE_CHECKING:
    from typing import List
    from ...cloudobject import CloudObject

logger = logging.getLogger(__name__)

def _create_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

def _calculate_block_size(data_type, shape):
    type_sizes = {
        "double": 8,
        "float": 4,
        "Complex": 8,
        "Bool": 1,
        "Int": 4,
    }
    num_elements = 1
    for dim in shape:
        num_elements *= dim
    if data_type == "Bool":
        return ceil((num_elements * type_sizes[data_type]) / 8)
    return num_elements * type_sizes[data_type]


def _retrieve_ms_from_s3(client, bucket_name, ms_name,base_dir, local_metadata_path="template.ms", criterion="_TSM0"):
    
    local_metadata_path = os.path.join(base_dir, local_metadata_path)

    response = client.list_objects_v2(bucket_name, ms_name)

    if 'Contents' not in response:
        print(f"No se encontraron contenidos en el bucket {bucket_name} con el prefijo {ms_name}")  #debug, maybe throw exception?
        return []
    empty_files_info = []
    
    for obj in response['Contents']:
            key = obj['Key']
            size = obj['Size']

            relative_path = os.path.relpath(key, ms_name)

            local_file_path = os.path.join(local_metadata_path, relative_path)
            local_dir = os.path.dirname(local_file_path)
            if not os.path.exists(local_dir):
                os.makedirs(local_dir)

            if key.endswith(criterion):
                with open(local_file_path, 'wb') as f:
                    pass
                empty_files_info.append({"name": relative_path, "size": size})
            else:
                client.download_file(bucket_name, key, local_file_path)
    
    tar_path = local_metadata_path + ".tar"

    _create_tarfile(tar_path,local_metadata_path)    

    return empty_files_info, tar_path


def _analyze_tiled_columns(ms_path):
    ms = table(ms_path, readonly=True)
    structure = ms.showstructure()
    ms.close()

    blocks = structure.strip().split("\n\n")
    tiled_columns = []
    total_rows = None

    for block in blocks:
        if "TiledColumnStMan" in block:
            column_metadata = {
                "filename": None,
                "type": None,
                "shape": None,
                "bucketsize": None
            }

            match_file = re.search(r"file=(\S+)", block)
            if match_file:
                column_metadata["filename"] = match_file.group(1)

            match_data = re.search(r"(\b(?:double|float|Complex|Bool|Int)\b)\s+.*?shape=\[(.*?)\]", block)
            if match_data:
                column_metadata["type"] = match_data.group(1)
                column_metadata["shape"] = [int(x) for x in match_data.group(2).split(",")]

            match_bucketsize = re.search(r"bucketsize=(\d+)", block)
            if match_bucketsize:
                column_metadata["bucketsize"] = int(match_bucketsize.group(1))

            if all(key in column_metadata for key in ("filename", "type", "shape", "bucketsize")):
                column_metadata["block_size"] = _calculate_block_size(
                    column_metadata["type"], column_metadata["shape"]
                )
                tiled_columns.append(column_metadata)

        match_rows = re.search(r"(\d+)\s*rows", block)
        if match_rows:
            total_rows = int(match_rows.group(1))

    return tiled_columns, total_rows

def preprocess_ms(cloud_object: CloudObject) -> PreprocessingMetadata:
    s3_client = cloud_object.storage()
    bucket_name = cloud_object.path.bucket
    ms_name = cloud_object.path.key
    criterion = "_TSM0"

    clean_ms_name = ms_name.replace('/', '_')
    base_dir = f"metadata_{clean_ms_name}"

    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    empty_files_info, metadata_path = _retrieve_ms_from_s3(        #creates template and returns some metadata
        client=s3_client,
        bucket_name=bucket_name,
        ms_name=ms_name,
        base_dir=base_dir,
        local_metadata_path="template.ms",
        criterion=criterion
    )

    tiled_metadata, total_rows = _analyze_tiled_columns("template.ms")

    mutables_list = []

    for column in tiled_metadata:
        column_filename = column["filename"] + criterion
        for empty_file in empty_files_info:
            if os.path.basename(empty_file["name"]) == column_filename:
                    new_mutable = Mutable(empty_file["name"],f"{ms_name}/",empty_file["size"],column["bucketsize"],column["block_size"])
                    mutables_list.append(new_mutable)

    return PreprocessingMetadata(
        attributes={
            "total_rows":total_rows,
            "mutable_files":mutables_list,
            "ms_name":ms_name  
        },
        metadata_file_path=metadata_path
    )

class Mutable:
    file_name: str
    ms_path: str
    real_size: int
    bucketsize: int
    block_size: int

@CloudDataFormat(preprocessing_function=preprocess_ms)
class MS:
    ms_name: str
    mutable_files: List[Mutable]
    total_rows: int

class MSSLice(CloudObjectSlice):
     def get(self):
        # Here you can consult your metadata generated for this format, in here we need to have caution 
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

@PartitioningStrategy(dataformat=MS)    #here we could define 2 partiton strategies, one that where you specify how many rows you want per file and other that you specify how many resulting files
                                        #you will obtaain?
def newformat_partitioning_strategy(cloud_object: CloudObject, num_chunks: int):
    slices = [] #get in heres
    for i in range(num_chunks):
        # Here you put the necessary logic for defining the byte ranges required to read a chunk of the data
        range_0 = ...
        range_1 = ...
        slice = MSSLice(range_0, range_1)
        slices.append(slice)
    return slices
