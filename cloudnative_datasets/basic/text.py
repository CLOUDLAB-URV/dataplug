import logging
import os
import re
import io
import subprocess
import tempfile
from math import ceil
from typing import BinaryIO, List

import pandas as pd

from ..cobase import CloudObjectWrapper
from ..cochunkbase import CloudObjectSlice

logger = logging.getLogger(__name__)


@CloudObjectWrapper()
class UTF8Text:
    pass


class UTF8TextSlice(CloudObjectSlice):
    def __init__(self, padding, *args, **kwargs):
        self.padding = padding
        self.first = False
        self.last = False
        super().__init__(*args, **kwargs)

    def get(self):
        r0 = self.range_0 - 1 if not self.first else self.range_0
        r1 = self.range_1 + self.padding if not self.last else self.range_1

        res = self.s3.get_object(Bucket=self.obj_path.bucket, Key=self.obj_path.key, Range=f'bytes={r0}-{r1}')
        body = res['Body'].read().decode('utf-8')

        s0 = 0
        if not self.first:
            # trim cut words in first slice
            while body[s0] != ' ' and body[s0] != '\n':
                s0 += 1
            s0 += 1

        s1 = self.range_1 - self.range_0
        if not self.last:
            # add cut words for slices in the middle using padding
            pad_count = 1
            c = body[s1]
            while c != ' ' and c != '\n':
                s1 += 1
                if s1 == len(body):
                    r0 = self.padding * pad_count
                    r1 = (self.padding * pad_count) + self.padding
                    r1 = self.obj_size if r1 > self.obj_size else r1
                    res = self.s3.get_object(Bucket=self.obj_path.bucket, Key=self.obj_path.key,
                                             Range=f'bytes={r0}-{r1}')
                    body += res['Body'].decode('utf-8')
                    pad_count += 1
                c = body[s1]

        return body[s0:s1]


def whole_words_strategy(cloud_object: UTF8Text, num_chunks: int, padding: int = 32) -> List[UTF8TextSlice]:
    """
    This partition strategy chunks raw text by number of chunks avoiding to cut words in half
    """
    chunk_sz = ceil(cloud_object.size / num_chunks)

    slices = []
    for i in range(num_chunks):
        r0 = chunk_sz * i
        r0 = r0 + 1 if r0 > 0 else r0
        r1 = (chunk_sz * i) + chunk_sz
        r1 = cloud_object.size if r1 > cloud_object.size else r1
        slice = UTF8TextSlice(range_0=r0, range_1=r1, padding=padding)
        slices.append(slice)
    slices[0].first = True
    slices[-1].last = True

    return slices
