import logging
import os
import re
import io
import subprocess
import tempfile
from math import ceil
from typing import BinaryIO

import pandas as pd

from ..cobase import CloudObjectWrapper
from ..cochunkbase import CloudObjectChunk
from ..preprocessers import AsyncPreprocesser

logger = logging.getLogger(__name__)

# GZTOOL_PATH = '/home/lab144/.local/bin/gztool'
GZTOOL_PATH = '/home/aitor-pc/.local/bin/gztool'
CHUNK_SIZE = 65536

RE_WINDOWS = re.compile(r'#\d+: @ \d+ / \d+ L\d+ \( \d+ @\d+ \)')
RE_NUMS = re.compile(r'\d+')
RE_NLINES = re.compile(r'Number of lines\s+:\s+\d+')


class GZipTextAsyncPreprocesser(AsyncPreprocesser):
    def __init__(self):
        super().__init__()

    @staticmethod
    def preprocess(data_stream, meta):
        tmp_index_file_name = tempfile.mktemp()
        try:
            os.remove(tmp_index_file_name)
        except FileNotFoundError:
            pass

        # Create index
        index_proc = subprocess.Popen([GZTOOL_PATH, '-i', '-x', '-s', '1', '-I', tmp_index_file_name],
                                      stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        chunk = data_stream.read(CHUNK_SIZE)
        while chunk != b"":
            index_proc.stdin.write(chunk)
            chunk = data_stream.read(CHUNK_SIZE)
        # data_stream.close()

        stdout, stderr = index_proc.communicate()
        logger.debug(stdout.decode('utf-8'))
        logger.debug(stderr.decode('utf-8'))
        assert index_proc.returncode == 0

        with open(tmp_index_file_name, 'rb') as index_f:
            index_bin = index_f.read()
        output = subprocess.check_output([GZTOOL_PATH, '-ell'], input=index_bin).decode('utf-8')
        logger.debug(output)

        gzip_index = meta.key + '.idx'
        meta.s3.put_object(Bucket=meta.meta_bucket, Key=gzip_index, Body=index_bin)

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
        df.to_csv('test.csv')
        return out_stream.getvalue(), {'total_lines': total_lines}


@CloudObjectWrapper(preprocesser=GZipTextAsyncPreprocesser)
class GZipText:
    def partition_chunk_lines(self, lines_per_chunk, strategy='expand'):
        total_lines = int(self.cloud_object.get_attribute('total_lines'))
        parts = ceil(total_lines / lines_per_chunk)
        pairs = [(lines_per_chunk * i, (lines_per_chunk * i) + lines_per_chunk - 1) for i in range(parts)]

        # adjust last pair
        if pairs[-1][1] > total_lines:
            if strategy == 'expand':
                l0, _ = pairs[-1]
                pairs[-1] = (l0, total_lines)
            elif strategy == 'merge':
                l0, l1 = pairs.pop()
                extra = l1 - l0
                pair = pairs[-1]
                pairs[-1] = pair[0], pair[1] + extra
            else:
                raise Exception(f'Unknown strategy {strategy}')

        return pairs

    def partition_num_chunks(self, n_chunks):
        x = self.cloud_object.get_attribute('total_lines')
        pass

    def __get_line_range(self, line0, line1):
        meta_obj = self.cloud_object.get_meta_obj()
        meta_buff = io.BytesIO(meta_obj.read())
        meta_buff.seek(0)
        df = pd.read_parquet(meta_buff)
        res = df.loc[(df['line_number'] >= line0) & (df['line_number'] <= line1)]
        window0 = res.head(1)['compressed_byte'].values[0].item()
        window1 = df.loc[res.tail(1).index + 1]['compressed_byte'].values[0].item()

        return GZipLineIterator(range0=window0, range1=window1, line0=line0, line1=line1,
                                cloud_object=self.cloud_object, child=self)


class GZipLineIterator(CloudObjectChunk):
    def __init__(self, range0, range1, line0, line1, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.range0 = range0
        self.range1 = range1
        self.line0 = line0
        self.line1 = line1

        self._body = None

    def setup(self):
        tmp_index_file = tempfile.mktemp()

        # Get index and store it to temp file
        res = self.cloud_object._s3.get_object(Bucket=self.cloud_object._meta_bucket, Key=self.child._index_key)
        with open(tmp_index_file, 'wb') as index_tmp:
            index_tmp.write(res['Body'].read())

        res = self.cloud_object._s3.get_object(Bucket=self.cloud_object._obj_bucket, Key=self.cloud_object._key,
                                               Range=f'bytes={self.range0 - 1}-{self.range1 - 1}')
        self._body = res['Body']

        tmp_chunk_file = tempfile.mktemp()
        with open(tmp_chunk_file, 'wb') as chunk_tmp:
            chunk_tmp.write(self._body.read())

        cmd = [GZTOOL_PATH, '-I', tmp_index_file, '-n', str(self.range0), '-L', str(self.line0), tmp_chunk_file]
        index_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        stdout, stderr = index_proc.communicate()
        # logger.debug(stdout.decode('utf-8'))
        logger.debug(stderr.decode('utf-8'))

        return stdout
