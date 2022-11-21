import logging
from math import ceil
from typing import BinaryIO, List

from ..cloudobject import CloudDataType
from ..dataslice import CloudObjectSlice

logger = logging.getLogger(__name__)


@CloudDataType()
class CSV:
    pass

class CSVSlice(CloudObjectSlice):
    def __init__(self, threshold, *args, **kwargs):
        self.threshold = threshold
        self.first = False
        self.last = False
        super().__init__(*args, **kwargs)

    def get(self):
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
        
        return retval[first_row_start_pos:last_row_end_pos]

        

        


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
        slices.append(data_slice)
    
    slices[0].first = True
    slices[-1].last = True

    return slices
