import logging
import os
import re
import io
import subprocess
import tempfile
from math import ceil

import pandas as pd

from ..cloudobject import CloudDataType
from ..dataslice import CloudObjectSlice
from dataplug.preprocess.stubs import BatchPreprocessor

logger = logging.getLogger(__name__)

# GZTool version 1.4.3
# https://github.com/circulosmeos/gztool
# GZTOOL_PATH = '/home/lab144/.local/bin/gztool'
GZTOOL_PATH = '/home/aitor-pc/.local/bin/gztool'
CHUNK_SIZE = 65536

RE_WINDOWS = re.compile(r'#\d+: @ \d+ / \d+ L\d+ \( \d+ @\d+ \)')
RE_NUMS = re.compile(r'\d+')
RE_NLINES = re.compile(r'Number of lines\s+:\s+\d+')


class GZipTextAsyncPreprocesser(BatchPreprocessor):
    def __init__(self):
        super().__init__()

    @staticmethod
    def preprocess(data_stream, meta):
        tmp_index_file_name = tempfile.mktemp()
        try:
            os.remove(tmp_index_file_name)
        except FileNotFoundError:
            pass

        # Create index and save to tmp file
        # (tmp file is needed, sending to stdout is not working at the moment)
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

        # Generate list of access windows from index
        output = subprocess.check_output([GZTOOL_PATH, '-ell', '-I', tmp_index_file_name]).decode('utf-8')
        logger.debug(output)

        # Store index binary file
        gzip_index_key = meta.meta_key + '.idx'
        meta.s3.upload_file(Filename=tmp_index_file_name, Bucket=meta.meta_bucket, Key=gzip_index_key)

        # Get the total number of lines
        total_lines = RE_NUMS.findall(RE_NLINES.findall(output).pop()).pop()

        # Generator that parses output to avoid copying all window data as lists
        def _lines_generator():
            for f in RE_WINDOWS.finditer(output):
                nums = [int(n) for n in RE_NUMS.findall(f.group())]
                yield nums

        # Generate data frame that stores window data
        df = pd.DataFrame(_lines_generator(),
                          columns=['window', 'compressed_byte', 'uncompressed_byte',
                                   'line_number', 'window_size', 'window_offset'])
        df.set_index(['window'], inplace=True)

        # Store data frame as parquet
        out_stream = io.BytesIO()
        df.to_parquet(out_stream, engine='pyarrow')
        # df.to_csv('test.csv')  # debug

        os.remove(tmp_index_file_name)

        return out_stream.getvalue(), {'total_lines': total_lines, 'index_key': gzip_index_key}


@CloudDataType(preprocessor=GZipTextAsyncPreprocesser)
class GZipText:
    @staticmethod
    def _get_ranges_from_line_pairs(cloud_object, pairs):
        meta_obj = cloud_object.s3.get_object(Bucket=cloud_object._meta_bucket, Key=cloud_object._meta_key)
        meta_buff = io.BytesIO(meta_obj.read())
        meta_buff.seek(0)
        df = pd.read_parquet(meta_buff)

        byte_ranges = [None] * len(pairs)
        for i, (line_0, line_1) in enumerate(pairs):
            window = df.loc[(df['line_number'] >= line_0) & (df['line_number'] <= line_1)]
            window0 = window.head(1)['compressed_byte'].values[0].item()
            window1 = df.loc[window.tail(1).index + 1]['compressed_byte'].values[0].item()
            byte_ranges[i] = (window0, window1)

        return byte_ranges

    @staticmethod
    def partition_chunk_lines(cloud_object, lines_per_chunk, strategy='expand'):
        total_lines = int(cloud_object.get_attribute('total_lines'))
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

        byte_ranges = GZipText._get_ranges_from_line_pairs(cloud_object, pairs)
        chunks = [GZipChunk(cloud_object, range_0, range_1, line_0, line_1)
                  for (range_0, range_1), (line_0, line_1) in zip(byte_ranges, pairs)]

        return chunks

    def partition_num_chunks(self, n_chunks):
        # TODO
        raise NotImplementedError()


class GZipChunk(CloudObjectSlice):
    def __init__(self, line_0, line_1, *args, **kwargs):
        self.line_0 = line_0
        self.line_1 = line_1
        super().__init__(*args, **kwargs)

    def get(self, stream=False):
        tmp_index_file = tempfile.mktemp()

        # Get index and store it to temp file
        self.s3.download_file(Bucket=self.meta_bucket, Key=self.attributes['index_key'], Filename=tmp_index_file)

        # Get compressed byte range
        res = self.s3.get_object(Bucket=self.object_bucket, Key=self.object_key,
                                 Range=f'bytes={self.range_0 - 1}-{self.range_1 - 1}')
        body = res['Body']

        cmd = [GZTOOL_PATH, '-I', tmp_index_file, '-n', str(self.range_0), '-L', str(self.line_0)]
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        chunk = body.read(CHUNK_SIZE)
        while chunk != b"":
            proc.stdin.write(chunk)
            chunk = body.read(CHUNK_SIZE)

        stdout, stderr = proc.communicate()
        # logger.debug(stdout.decode('utf-8'))
        logger.debug(stderr.decode('utf-8'))

        return stdout
