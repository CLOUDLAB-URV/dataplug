from __future__ import annotations

import logging
import os
import shutil
import tarfile
import re

from math import ceil
from typing import TYPE_CHECKING

from casacore.tables import table               #Should we always import or use a try?

from ...entities import CloudDataFormat, CloudObjectSlice, PartitioningStrategy
from ...preprocessing.metadata import PreprocessingMetadata

if TYPE_CHECKING:
    from typing import List
    from ...cloudobject import CloudObject

logger = logging.getLogger(__name__)

def _create_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

def _calculate_block_size(data_type, shape):    #These are not directly accessible from casacore so we need to hardcode them, they may change
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

# For now, we define the default criterion "_TSM0", but in the future, more formats may be used. 
def _retrieve_ms_from_s3(client, bucket_name, ms_name, base_dir, local_metadata_path="template.ms", criterion="_TSM0"):
    
    local_metadata_path = os.path.join(base_dir, local_metadata_path)

    response = client.list_objects_v2(Bucket=bucket_name,Prefix=ms_name)

    if 'Contents' not in response:
        print(f"WARNING: No content in: {bucket_name} with the following name: {ms_name}")  #DEBUG?
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

    return empty_files_info, local_metadata_path

def _analyze_tiled_columns(ms_path):
    ms = table(ms_path, readonly=True)
    structure = ms.showstructure()
    ms.close()

    blocks = structure.strip().split("\n\n")
    tiled_columns = []
    total_rows = None
    
    # As stated before, if more definitions of .ms files appear it may be of interest to not hardcode the type of column
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
    s3_client = cloud_object.storage
    bucket_name = cloud_object.path.bucket
    ms_name = cloud_object.path.key
    criterion = "_TSM0"

    clean_ms_name = ms_name.replace('/', '_')
    base_dir = f"metadata_{clean_ms_name}"

    if not os.path.exists(base_dir):
        os.makedirs(base_dir)

    # This both creates the attributes and a .tar file that's the template for later processing
    empty_files_info, metadata_path = _retrieve_ms_from_s3(
        client=s3_client,
        bucket_name=bucket_name,
        ms_name=ms_name,
        base_dir=base_dir,
        local_metadata_path="template.ms",
        criterion=criterion
    )

    tiled_metadata, total_rows = _analyze_tiled_columns(f"{base_dir}/template.ms")
    mutables_list = []

    for column in tiled_metadata:
        column_filename = column["filename"] + criterion
        for empty_file in empty_files_info:
            if os.path.basename(empty_file["name"]) == column_filename:
                new_mutable = {
                    "file_name": empty_file["name"],
                    "ms_path": f"{ms_name}/",
                    "real_size": empty_file["size"],
                    "bucketsize": column["bucketsize"],
                    "block_size": column["block_size"]
                }
                mutables_list.append(new_mutable)

    return PreprocessingMetadata(
        attributes={
            "total_rows": total_rows,
            "mutable_files": mutables_list,
            "ms_name": ms_name
        },
        metadata_file_path=metadata_path + ".tar"
    )

@CloudDataFormat(preprocessing_function=preprocess_ms)
class MS:
    ms_name: str
    mutable_files: List[dict]
    total_rows: int

def _clone_template(template_path, output_path):
    if not os.path.exists(template_path):
        raise FileNotFoundError(f"The template {template_path} does not exist.")

    if os.path.exists(output_path):
        if os.path.isdir(output_path):
            shutil.rmtree(output_path)
        else:
            os.remove(output_path)

    for root, dirs, files in os.walk(template_path):
        for file_name in files:
            src_file_path = os.path.join(root, file_name)
            dest_file_path = os.path.join(output_path, os.path.relpath(src_file_path, template_path))
            os.makedirs(os.path.dirname(dest_file_path), exist_ok=True)
            with open(src_file_path, 'rb') as src_file, open(dest_file_path, 'wb') as dest_file:
                dest_file.write(src_file.read())

    print(f"Cloned {template_path} to {output_path}")

def _copy_byte_range(s3, bucket, ms_name, metadata, output_path, starting_row, end_row):
    for mutable in metadata:
        file_name = mutable["file_name"]
        block_size = mutable["block_size"]
        bucketsize = mutable["bucketsize"]
        real_size = mutable["real_size"]

        key = f"{ms_name}/{file_name}"
        start_byte = block_size * starting_row
        end_byte = block_size * end_row
        requested_length = end_byte - start_byte

        actual_end = min(end_byte, real_size)
        if start_byte < real_size:
            s3_range = f"bytes={start_byte}-{actual_end - 1}"
            try:
                try:
                    response = s3.get_object(Bucket=bucket, Key=key, Range=s3_range)
                except s3.exceptions.NoSuchKey:
                    print(f"Error: The object {key} does not exist.")                       #Debug?
                    file_data = b""
                except s3.exceptions.InvalidRange:
                    print(f"Error: The range {s3_range} is invalid for the object {key}.")  #Debug?
                    file_data = b""
                file_data = response["Body"].read()
            except Exception as e:
                print(f"Error retrieving {key} with the range {s3_range}: {e}")
                raise
        else:
            file_data = b""
            print("Error with the range")

        if requested_length % bucketsize == 0:
            padded_length = requested_length
        else:
            padded_length = ((requested_length // bucketsize) + 1) * bucketsize

        current_length = len(file_data)
        padding_needed = padded_length - current_length if current_length < padded_length else 0

        target_file_path = os.path.join(output_path, file_name)
        os.makedirs(os.path.dirname(target_file_path), exist_ok=True)
        with open(target_file_path, "wb") as fout:
            fout.write(file_data)
            if padding_needed > 0:
                fout.write(b'\x00' * padding_needed)

        print(f"Copied {current_length} bytes {key} to {target_file_path} with {padding_needed} empty bytes for padding")

# Per actual definition, returning path to processed slice is a desirable outcome. 
def _cleanup_ms(input_ms_path, output_ms_path, num_rows):                      
    if not os.path.exists(input_ms_path):
        return f"Error: MeasurementSet '{input_ms_path}' not found."

    try:
        ms = table(input_ms_path)
        
        selection = ms.selectrows(list(range(0, num_rows))) 
        selection.copy(output_ms_path, deep=True) 
        
        ms.close()
        #This return, if not checked, will not be displayed. Maybe return nothing?
        return f"Measurement Set processed correctly, stored in: {output_ms_path}"
    
    except Exception as e:
        return f"Error MS: {str(e)}"

class MSSLice(CloudObjectSlice):   
    def get(self):
        # Maybe paths could be handled in a cleanlier way? Or redefine where data is created/stored?
        ms_name = self.cloud_object.path.key
        clean_ms_name = ms_name.replace('/', '_')
        metadata_dir = f"metadata_{clean_ms_name}"
        template_path = metadata_dir + "/template.ms"

        if not os.path.exists(metadata_dir):
            meta_key = self.cloud_object.meta_path.key
            meta_bucket = self.cloud_object.meta_path.bucket

            self.cloud_object.storage.download_file(meta_bucket,meta_key,metadata_dir+"/template.ms.tar")

            with tarfile.open(template_path+".tar","r") as tar:
                tar.extractall(path=template_path)
        
        total_rows = self.range_1 - self.range_0
        slice_number = self.range_0 // total_rows

        if (self.range_0 % slice_number):
            slice_number = slice_number + 1

        sliced_outcome = f"temp/slice_{slice_number}.ms"            #DEBUG, should probably delete folder later
        cleaned_sliced_path = f"output/slice_{slice_number}.ms"  
        
        _clone_template(template_path,sliced_outcome)
        _copy_byte_range(
            s3=self.cloud_object.storage,
            bucket=self.cloud_object.path.bucket,
            ms_name=ms_name,
            metadata=self.cloud_object["mutable_files"],
            output_path=sliced_outcome,
            starting_row=self.range_0,
            end_row=self.range_1
        )
        if not os.path.exists("output"):
            os.makedirs("output")
        error = _cleanup_ms(sliced_outcome, cleaned_sliced_path,total_rows)
        print(error)                                                #Debug

        # Finally returning the path to the file so you can programatically pass it to casacore for further processing. 
        chunk = cleaned_sliced_path 

        return chunk

# Decide number of resulting chunks (Most optimal)
@PartitioningStrategy(dataformat=MS)
def ms_partitioning_strategy(cloud_object: CloudObject, num_chunks: int):
    
    total_rows=cloud_object.get_attribute("total_rows")
    slices = []
    
    rows_per_chunk = total_rows // num_chunks
    remainder = total_rows % num_chunks
    start = 0

    for i in range(num_chunks):
        chunk_size = rows_per_chunk + (1 if i < remainder else 0)
        end = start + chunk_size - 1
        slice = MSSLice(start, end)
        slices.append(slice)
        start = end + 1

    return slices

# Decide number of resulting rows per chunk (Most flexible)
@PartitioningStrategy(dataformat=MS)
def ms_partitioning_strategy_rowsize(cloud_object: CloudObject, row_size: int):

    total_rows = cloud_object.get_attribute("total_rows")
    slices = []
    start = 0
    
    while start < total_rows:
        end = min(start + row_size - 1, total_rows - 1)
        slice = MSSLice(start, end)
        slices.append(slice)
        start = end + 1
        
    return slices

# Decide if we could use a third partitioning strategy, based maybe on default bucket sizes? Or dinamically with metadata usage?