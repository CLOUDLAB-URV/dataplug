import logging
from math import ceil
from typing import BinaryIO, List
import pandas as pd
from ..cloudobject import CloudDataType
from ..dataslice import CloudObjectSlice
import io

logger = logging.getLogger(__name__)


@CloudDataType()
class CSV:
    pass

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
        last_row_end_pos = self.range_1-self.range_0 
        #find the nearest first row start position
        if not self.first:
            while retval[first_row_start_pos] != '\n':
                 first_row_start_pos += 1

        #find the nearest last row end position within the threshold
        if not self.last:
            while retval[last_row_end_pos] != '\n':
                last_row_end_pos += 1
        
        #store the header of the first slice as an attribute
        if self.first:
            self.header = retval[first_row_start_pos:last_row_end_pos].split("\n")[0] + '\n'
        
        return retval[first_row_start_pos:last_row_end_pos]

    def generator_csv(self):
        "Return the slice as a generator"
        for line in self.get().splitlines():
            yield line

    def as_pandas(self):
        "Return the slice as a pandas dataframe"
        if not self.first:
            dataframe = pd.read_csv(io.StringIO(self.get()), sep=',')
        else:
            dataframe = pd.read_csv(self.header + io.StringIO(self.get()), sep=',')
        return dataframe

    
def whole_line_csv_strategy(cloud_object: CSV, num_chunks: int,threshold: int = 32) -> List[CSVSlice]:
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

