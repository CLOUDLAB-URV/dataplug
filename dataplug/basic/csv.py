import logging
from math import ceil
from typing import BinaryIO, List, Tuple, Union, ByteString, Dict
import pandas as pd

from .. import CloudObject
from ..cloudobject import CloudDataType
from ..dataslice import CloudObjectSlice
from ..preprocess import BatchPreprocessor
import io

logger = logging.getLogger(__name__)


class CSVPreprocessor(BatchPreprocessor):
    def extract_metadata(self, data_stream: BinaryIO, cloud_object: CloudObject):
        head = ''.join(data_stream.readline().decode('utf-8') for _ in range(25))
        df = pd.read_csv(io.StringIO(head))
        return None, None, {'columns': df.columns.values.tolist(), 'dtypes': df.dtypes.tolist()}


@CloudDataType(preprocessor=CSVPreprocessor)
class CSV:
    header: int


class CSVSlice(CloudObjectSlice):
    def __init__(self, threshold, *args, **kwargs):
        self.threshold = threshold
        self.first = False
        self.last = False
        self.header = ""
        super().__init__(*args, **kwargs)

    def get(self):
        "Return the slice as a string"
        r0 = self.range_0 - 1 if not self.first else self.range_0
        r1 = self.range_1 + self.threshold if not self.last else self.range_1
        res = self.s3.get_object(Bucket=self.obj_path.bucket, Key=self.obj_path.key, Range=f'bytes={r0}-{r1}')
        retval = res['Body'].read().decode('utf-8')

        first_row_start_pos = 0
        last_row_end_pos = self.range_1 - self.range_0
        # find the nearest first row start position
        if not self.first:
            while retval[first_row_start_pos] != '\n':
                first_row_start_pos += 1

        # find the nearest last row end position within the threshold
        if not self.last:
            while retval[last_row_end_pos] != '\n':
                last_row_end_pos += 1

        # store the header of the first slice as an attribute
        if self.first:
            self.header = retval[first_row_start_pos:last_row_end_pos].split("\n")[0] + '\n'

        return retval[first_row_start_pos:last_row_end_pos]

    def generator_csv(self, read_size):
        "Return the slice as a generator"
        r0 = self.range_0 - 1 if not self.first else self.range_0
        r1 = self.range_1 + self.threshold if not self.last else self.range_1
        res = self.s3.get_object(Bucket=self.obj_path.bucket, Key=self.obj_path.key, Range=f'bytes={r0}-{r1}')
        last_row_end_pos = self.range_1 - self.range_0

        total_bytes_read = read_size
        buffer = b''
        b_new_line = b'\n'

        # find the nearest first row start position, discard the first partitial row
        if not self.first:
            chars = b''
            while chars != b_new_line:
                chars = res['Body'].read(1)
                total_bytes_read += 1
        total_bytes_read = total_bytes_read - 1

        # yield the n-2 reads
        while total_bytes_read <= last_row_end_pos:
            buffer = buffer + res['Body'].read(read_size)
            for line in buffer.splitlines(keepends=True):
                if len(buffer.split(b_new_line, 1)) > 1:
                    yield line
                    buffer = buffer.split(b_new_line, 1)[1]
            total_bytes_read += read_size

        # yield the n-1 read (rows left until last_row_end_pos)
        if total_bytes_read > last_row_end_pos:
            last_read_size = last_row_end_pos - (total_bytes_read - read_size)
            buffer = buffer + res['Body'].read(last_read_size)
            for line in buffer.splitlines(keepends=True):
                if len(buffer.split(b_new_line, 1)) > 1:
                    yield line
                    buffer = buffer.split(b_new_line, 1)[1]

        # If the buffer has contents in it, there is one partial line that
        # has been omited, read until a \n has been found (within the threshold) and yield it

        if len(buffer) > 0:
            next_el = res['Body'].read(1)
            counter = 0
            while next_el != b_new_line and counter <= self.threshold:
                buffer = buffer + next_el
                next_el = res['Body'].read(1)
                counter += 1

            if not self.first:
                yield buffer + b_new_line
            else:
                yield buffer

    def as_pandas(self):
        "Return the slice as a pandas dataframe"
        if not self.first:
            dataframe = pd.read_csv(io.StringIO(self.get()), sep=',')
        else:
            dataframe = pd.read_csv(self.header + io.StringIO(self.get()), sep=',')
        return dataframe


def whole_line_csv_strategy(cloud_object: CSV, num_chunks: int, threshold: int = 32) -> List[CSVSlice]:
    """
    This partition strategy chunks csv files by number of chunks avoiding to cut rows in half
    """
    chunk_sz = ceil(cloud_object.size / num_chunks)

    slices = []
    for i in range(num_chunks):
        r0 = chunk_sz * i
        r1 = (chunk_sz * i) + chunk_sz
        r1 = cloud_object.size if r1 > cloud_object.size else r1
        data_slice = CSVSlice(range_0=r0, range_1=r1, threshold=threshold)
        data_slice.first = True if i == 0 else False
        slices.append(data_slice)

    slices[-1].last = True

    return slices
