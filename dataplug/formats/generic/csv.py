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


def preprocess_csv(cloud_object: CloudObject, separator=",") -> PreprocessingMetadata:
    print(f"Preprocessing CSV file {cloud_object.path.key} (separator {separator})")
    top = []
    with cloud_object.open("r") as f:
        for i in range(20):
            top.append(f.readline().strip())
    df = pd.read_csv(io.StringIO("\n".join(top)), sep=separator)
    print(df.columns)
    print(df.dtypes)
    print(df.head(5))

    return PreprocessingMetadata(
        attributes={
            "columns": df.columns.tolist(),
            "dtypes": df.dtypes.tolist(),
        }
    )


@CloudDataFormat(preprocessing_function=preprocess_csv)
class CSV:
    columns: List[str]
    dtypes: List[str]


class CSVSlice(CloudObjectSlice):
    def __init__(self, chunk_id, num_chunks, padding, *args, **kwargs):
        self.chunk_id = chunk_id
        self.num_chunks = num_chunks
        self.padding = padding
        super().__init__(*args, **kwargs)

    def get(self):
        res = self.cloud_object.storage.get_object(
            Bucket=self.cloud_object.path.bucket, Key=self.cloud_object.path.key,
            Range=f"bytes={self.range_0}-{self.range_1}"
        )
        body = res["Body"].read().decode("utf-8")
        buff = io.StringIO(body)

        head_offset = 0
        if self.chunk_id != 0:
            # Check if the previous character is a newline for chunks in the middle
            first = buff.read(1)
            if first != "\n":
                # If not a newline, it means we are in the middle of a line
                # We truncate the line and read the next one
                # The truncated line will be read by the previous chunk
                buff.readline()
                head_offset = buff.tell()

        buff.seek(0, io.SEEK_END)
        tail_offset = buff.tell()

        if self.chunk_id != self.num_chunks - 1:
            buff.seek(tail_offset - self.padding)
            tail_offset = buff.tell()

            buff.seek(tail_offset - 1)
            last = buff.read(1)
            retries = 0
            while last != "\n":
                last = buff.read(1)
                if not last:
                    # Expand the buffer
                    retries += 1
                    r0 = self.range_1 + (self.padding * retries)
                    r1 = r0 + self.padding
                    res = self.cloud_object.storage.get_object(
                        Bucket=self.cloud_object.path.bucket, Key=self.cloud_object.path.key,
                        Range=f"bytes={r0}-{r1}"
                    )
                    vcf_body = res["Body"].read().decode("utf-8")
                    buff.write(vcf_body)
                    last = buff.read(1)

            tail_offset = buff.tell()

        full_body = buff.getvalue()[head_offset:tail_offset]

        if self.range_0 != 0:
            # Add the columns header if it is not the first chunk
            header = ",".join(self.cloud_object.attributes.columns) + "\n"
            full_body = header + full_body

        return full_body

    def get_as_pandas(self):
        import pandas as pd
        return pd.read_csv(io.StringIO(self.get()))


@PartitioningStrategy(dataformat=CSV)
def partition_chunk_size(cloud_object: CloudObject, chunk_size: int, padding=256) -> List[CSVSlice]:
    """
    This partition strategy chunks CSV data by a fixed size
    """
    assert chunk_size <= cloud_object.size, "Chunk size must be smaller than the file size"
    num_chunks = ceil(cloud_object.size / chunk_size)

    slices = []
    for i in range(num_chunks):
        r0 = chunk_size * i
        r0 = r0 - 1 if r0 > 0 else r0  # Read one extra byte from the previous chunk, we will check if it is a newline
        r1 = (chunk_size * i) + chunk_size
        r1 = cloud_object.size if r1 > cloud_object.size else r1 + padding
        data_slice = CSVSlice(range_0=r0, range_1=r1, chunk_id=i, num_chunks=num_chunks, padding=padding)
        slices.append(data_slice)

    return slices


@PartitioningStrategy(dataformat=CSV)
def partition_num_chunks(cloud_object: CloudObject, num_chunks: int, padding=256) -> List[CSVSlice]:
    """
    This partition strategy chunks CSV data in a fixed number of chunks
    """
    chunk_size = ceil(cloud_object.size / num_chunks)

    slices = []
    for i in range(num_chunks):
        r0 = chunk_size * i
        r0 = r0 - 1 if r0 > 0 else r0  # Read one extra byte from the previous chunk, we will check if it is a newline
        r1 = (chunk_size * i) + chunk_size
        r1 = cloud_object.size if r1 > cloud_object.size else r1 + padding  # Add padding to read the last line
        data_slice = CSVSlice(range_0=r0, range_1=r1, chunk_id=i, num_chunks=num_chunks, padding=padding)
        slices.append(data_slice)

    return slices
