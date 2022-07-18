import logging
import os
import re
import io
import subprocess
import tempfile
from typing import BinaryIO

import pandas as pd

from ..cloudobjectbase import CloudObjectBase


logger = logging.getLogger(__name__)

GZTOOL_PATH = '/home/lab144/.local/bin/gztool'
CHUNK_SIZE = 65536

RE_WINDOWS = re.compile(r'#\d+: @ \d+ / \d+ L\d+ \( \d+ @\d+ \)')
RE_NUMS = re.compile(r'\d+')
RE_NLINES = re.compile(r'Number of lines\s+:\s+\d+')


class GZippedText(CloudObjectBase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def preprocess(self, object_stream: BinaryIO):
        r, w = os.pipe()

        tmp_file_name = tempfile.mktemp()
        # try:
        #     os.remove(tmp_file_name)
        # except FileNotFoundError:
        #     pass
        index_proc = subprocess.Popen([GZTOOL_PATH, '-i', '-x', '-s', '10', '-I', tmp_file_name],
                                      stdin=r, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        pipe = os.fdopen(w, 'wb')

        chunk = object_stream.read(CHUNK_SIZE)
        while chunk != b"":
            pipe.write(chunk)
            chunk = object_stream.read(CHUNK_SIZE)
        pipe.flush()
        pipe.close()
        object_stream.close()

        stdout, stderr = index_proc.communicate()
        logger.debug(stdout.decode('utf-8'))
        logger.debug(stderr.decode('utf-8'))
        assert index_proc.returncode == 0

        with open(tmp_file_name, 'rb') as index_f:
            output = subprocess.check_output([GZTOOL_PATH, '-ell'], input=index_f.read()).decode('utf-8')
            logger.debug(output)
        os.remove(tmp_file_name)

        total_lines = RE_NUMS.findall(RE_NLINES.findall(output).pop()).pop()

        lines = []
        for f in RE_WINDOWS.finditer(output):
            nums = [int(n) for n in RE_NUMS.findall(f.group())]
            lines.append(nums)

        df = pd.DataFrame(lines, columns=['window', 'compressed_byte', 'uncompressed_byte',
                                          'line_number', 'window_size', 'window_offset'])
        df.set_index(['window'], inplace=True)

        out_stream = io.BytesIO()
        df.to_parquet(out_stream, engine='pyarrow')
        return out_stream.getvalue(), {'total_lines': total_lines}

    def partition_even_lines(self, lines_per_chunk):
        # TODO
        # make use of index to get the ranges of the chunks partitioned by number of lines per chunk
        # return list of range tuples [(chunk0-range0, chunk-0range1), ...]
        pass
