from __future__ import annotations

import io
import logging
import re
from math import ceil
from typing import TYPE_CHECKING

from ...entities import CloudDataFormat, CloudObjectSlice, PartitioningStrategy
from ...preprocessing.metadata import PreprocessingMetadata

if TYPE_CHECKING:
    from typing import Dict, List, Union
    from ...cloudobject import CloudObject

logger = logging.getLogger(__name__)


def preprocess_vcf(cloud_object: CloudObject) -> PreprocessingMetadata:
    header = []
    header_metadata = {}
    with cloud_object.open("r") as f:
        line = f.readline().strip()
        assert line.startswith(
            "##fileformat=VCF"
        ), "VCF file does not start with the correct header"
        key, value = line.replace("##", "").split("=")
        header_metadata[key] = value
        header.append(line)

        line = f.readline().strip()
        while line.startswith("##"):
            header.append(line)
            key, value = line.replace("##", "").split("=", 1)
            if "<" in value and ">" in value:
                # Value is a dictionary with the format <key1=value1,key2=value2,...>
                value = value.strip("<").strip(">")
                matches = re.findall(r'(\w+)=(".*?"|\w+)', value)
                decoded_dict = {key: value.strip('"') for key, value in matches}
                if key not in header_metadata:
                    header_metadata[key] = []
                header_metadata[key].append(decoded_dict)
            else:
                # Value is a simple key-value pair (or custom metadata)
                header_metadata[key] = value
            line = f.readline().strip()

        assert line.startswith("#CHROM"), "VCF file does not have the correct header"
        columns = line.replace("#", "").split("\t")
        header.append(line)

        body_offset = f.tell()  # Save the current position to read the rest of the file

    header = "\n".join(header).encode("utf-8")

    # print(header_metadata)
    # print(columns)
    # print(header)

    return PreprocessingMetadata(
        attributes={
            "columns": columns,
            "vcf_attributes": header_metadata,
            "body_offset": body_offset,
        },
        metadata=header,
    )


def preprocess_vcf_gz(cloud_object: CloudObject) -> PreprocessingMetadata:
    raise NotImplementedError("Preprocessing for VCF GZ files is not implemented yet")


@CloudDataFormat(preprocessing_function=preprocess_vcf)
class VCF:
    columns: List[str]
    vcf_attributes: Dict[str, Union[str, List[str], Dict[str, str]]]
    body_offset: int


class VCFSlice(CloudObjectSlice):
    def __init__(self, chunk_id, num_chunks, padding, *args, **kwargs):
        self.chunk_id = chunk_id
        self.num_chunks = num_chunks
        self.padding = padding
        super().__init__(*args, **kwargs)

    def get(self):
        res = self.cloud_object.storage.get_object(
            Bucket=self.cloud_object.path.bucket,
            Key=self.cloud_object.path.key,
            Range=f"bytes={self.range_0}-{self.range_1}",
        )
        vcf_body = res["Body"].read().decode("utf-8")
        buff = io.StringIO(vcf_body)
        # logger.info(f"Getting slice {self.chunk_id}. Range is {self.range_0}-{self.range_1}")

        head_offset = 0
        if self.chunk_id != 0:
            # Check if the previous character is a newline for chunks in the middle
            first = buff.read(1)
            if first != "\n":
                # If not a newline, it means we are in the middle of a line
                # We truncate the line and read the next one
                # The truncated line will be read by the previous chunk
                buff.readline()
            # Set the head offset to skip a partial line (included if the first char is a newline)
            head_offset = buff.tell()

        buff.seek(0, io.SEEK_END)
        tail_offset = buff.tell()

        if self.chunk_id != self.num_chunks - 1:
            buff.seek(tail_offset - 1)
            last = buff.read(1)
            retries = 0
            while last != "\n":
                last = buff.read(1)
                if not last:
                    # Expand the buffer
                    # + - 1 because ranges are both inclusive
                    r0 = self.range_1 + (self.padding * retries) + 1
                    r1 = r0 + self.padding - 1
                    retries += 1
                    # logger.info(f"File: {self.cloud_object.path.key} Range: {r0}-{r1}")
                    res = self.cloud_object.storage.get_object(
                        Bucket=self.cloud_object.path.bucket,
                        Key=self.cloud_object.path.key,
                        Range=f"bytes={r0}-{r1}",
                    )
                    vcf_body = res["Body"].read().decode("utf-8")
                    buff.seek(0, io.SEEK_END)
                    pos = buff.tell()
                    buff.write(vcf_body)
                    buff.seek(pos)
                    last = buff.read(1)

            tail_offset = buff.tell()

        vcf_body = buff.getvalue()[head_offset:tail_offset]

        # Get the VCF header
        res = self.cloud_object.storage.get_object(
            Bucket=self.cloud_object.meta_path.bucket,
            Key=self.cloud_object.meta_path.key,
        )
        vcf_header = res["Body"].read().decode("utf-8")

        return vcf_header + "\n" + vcf_body


@PartitioningStrategy(dataformat=VCF)
def partition_num_chunks(
    cloud_object: CloudObject, num_chunks: int, padding=256
) -> List[VCFSlice]:
    """
    This partition strategy chunks VCF data in a fixed number of chunks
    """
    chunk_size = ceil((cloud_object.size - cloud_object["body_offset"]) / num_chunks)

    slices = []
    for i in range(num_chunks):
        r0 = (chunk_size * i) + cloud_object["body_offset"]
        r1 = r0 + chunk_size - 1  # one less because ranges are inclusive
        # Read one extra byte from the previous chunk, we will check if it is a newline
        r0 = r0 - 1 if i != 0 else r0
        r1 = cloud_object.size if r1 > cloud_object.size else r1
        data_slice = VCFSlice(
            range_0=r0, range_1=r1, chunk_id=i, num_chunks=num_chunks, padding=padding
        )
        slices.append(data_slice)

    return slices
