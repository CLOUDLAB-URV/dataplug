from __future__ import annotations

import io
import logging
import os
import re
import subprocess
import tempfile
import threading
import time
from math import ceil

import numpy as np
import pandas as pd
import tqdm

from dataplug.core.cloudobject import CloudDataFormatTemplate, CloudObject, CloudObjectSlice
from ...preprocessing.preprocessor import BatchPreprocessor, PreprocessingMetadata
from ...util import force_delete_path

logger = logging.getLogger(__name__)

# GZTool version 1.4.3
# https://github.com/circulosmeos/gztool

CHUNK_SIZE = 65536
RE_WINDOWS = re.compile(r"#\d+: @ \d+ / \d+ L\d+ \( \d+ @\d+ \)")
RE_NUMS = re.compile(r"\d+")
RE_NLINES = re.compile(r"Number of lines\s+:\s+\d+")


def _get_gztool_path():
    """
    Utility function that returns the absolute path for gzip file binary or raises exception if it is not found
    TODO currently only works on unix-based systems
    """
    proc = subprocess.run(["which", "gztool"], check=True, capture_output=True, text=True)
    path = proc.stdout.rstrip("\n")
    logger.debug("Using gztool located in %s", path)
    return path


class GZipTextPreprocessor(BatchPreprocessor):
    def __init__(self):
        super().__init__()

    def preprocess(self, cloud_object: CloudObject) -> PreprocessingMetadata:
        """
        Create index file from gzip archive using gztool (https://github.com/circulosmeos/gztool)
        """
        gztool = _get_gztool_path()
        tmp_index_file_name = tempfile.mktemp()
        try:
            obj_res = cloud_object.storage.get_object(Bucket=cloud_object.path.bucket, Key=cloud_object.path.key)
            assert obj_res.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
            data_stream = obj_res["Body"]

            force_delete_path(tmp_index_file_name)
            t0 = time.perf_counter()

            # Create index and save to tmp file
            # TODO tmp file is needed, sending to stdout is not working at the moment (todo fix)
            index_proc = subprocess.Popen(
                [gztool, "-i", "-x", "-I", tmp_index_file_name],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            # TODO program might get stuck if subprocess fails, blocking io should be done in a backgroun thread or using async/await
            with tqdm.tqdm(total=cloud_object.size) as pb:
                try:
                    chunk = data_stream.read(CHUNK_SIZE)
                    while chunk != b"":
                        # logger.debug('Writing %d bytes to Pipe STDIN', len(chunk))
                        index_proc.stdin.write(chunk)
                        pb.update(len(chunk))
                        chunk = data_stream.read(CHUNK_SIZE)
                    if hasattr(data_stream, "close"):
                        data_stream.close()
                except BrokenPipeError as e:
                    stdout, stderr = index_proc.communicate()
                    logger.error(stdout.decode("utf-8"))
                    logger.error(stderr.decode("utf-8"))
                    raise e

            stdout, stderr = index_proc.communicate()
            logger.debug(stdout.decode("utf-8"))
            logger.debug(stderr.decode("utf-8"))
            if index_proc.returncode > 0:
                logger.debug(stdout.decode("utf-8"))
                logger.debug(stderr.decode("utf-8"))
                raise Exception("Error creating gz index")

            # Generate list of access windows from index
            proc = subprocess.run(
                [gztool, "-ell", "-I", tmp_index_file_name],
                check=True,
                capture_output=True,
                text=True,
            )
            output = proc.stdout
            # logger.debug(output)

            # Store index binary file
            gzip_index_key = cloud_object.meta_path.key + ".idx"
            cloud_object.storage.upload_file(
                Filename=tmp_index_file_name,
                Bucket=cloud_object.meta_path.bucket,
                Key=gzip_index_key,
            )

            # Get the total number of lines
            total_lines = int(RE_NUMS.findall(RE_NLINES.findall(output).pop()).pop())
            logger.debug("Indexed gzipped text file with %s total lines", total_lines)
            t1 = time.perf_counter()
            logger.debug("Index generated in %.3f seconds", t1 - t0)

            # Generator function that parses output to avoid copying all window data as lists
            def _lines_generator():
                for f in RE_WINDOWS.finditer(output):
                    nums = [int(n) for n in RE_NUMS.findall(f.group())]
                    yield nums

            # Generate data frame that stores gzip index windows offsets
            df = pd.DataFrame(
                _lines_generator(),
                columns=[
                    "window",
                    "compressed_byte",
                    "uncompressed_byte",
                    "line_number",
                    "window_size",
                    "window_offset",
                ],
            )
            df.set_index(["window"], inplace=True)

            # Store data frame as parquet
            out_stream = io.BytesIO()
            df.to_parquet(out_stream, engine="pyarrow")
            # df.to_csv(os.path.join(tempfile.gettempdir(), f'{meta.obj_path.stem}.csv'))  # debug

            os.remove(tmp_index_file_name)

            out_stream.seek(0)
            return PreprocessingMetadata(
                metadata=out_stream,
                attributes={
                    "total_lines": total_lines,
                    "index_key": gzip_index_key,
                },
            )
        finally:
            force_delete_path(tmp_index_file_name)


def _get_ranges_from_line_pairs(cloud_object: CloudObject, pairs):
    meta_obj = cloud_object.storage.get_object(Bucket=cloud_object.meta_path.bucket, Key=cloud_object.meta_path.key)
    meta_buff = io.BytesIO(meta_obj["Body"].read())
    meta_buff.seek(0)
    df = pd.read_parquet(meta_buff)
    line_indexes = df["line_number"].to_numpy()
    num_windows = df.shape[0]

    byte_ranges = [None] * len(pairs)
    for i, (line_0, line_1) in enumerate(pairs):
        # Find the closest window index for line_0
        window_head_idx = (np.abs(line_indexes - line_0)).argmin()
        # Check if window line entry pont is past requested line_0, if so, get previous window
        window_head_line = df.iloc[window_head_idx]["line_number"]
        if window_head_line > line_0:
            window_head_idx = window_head_idx - 1
        # Get offset in compressed archive for window 0
        window0_offset = df.iloc[window_head_idx]["compressed_byte"]

        # Find the closest window index for line_0
        widow_tail_idx = (np.abs(line_indexes - line_1)).argmin()
        # Check if window line entry pont is before requested line_1, if so, get next window
        window_tail_line = df.iloc[widow_tail_idx]["line_number"]
        if window_tail_line < line_1:
            widow_tail_idx = widow_tail_idx + 1
        if widow_tail_idx >= num_windows:
            # Adjust offset for lines inside last window, use end of compressed archive for 2nd offset
            window1_offset = cloud_object.size
        else:
            window1_offset = df.iloc[widow_tail_idx]["compressed_byte"]

        byte_ranges[i] = (window0_offset, window1_offset)

    return byte_ranges


def partition_chunk_lines(cloud_object: CloudObject, lines_per_chunk, strategy="expand"):
    """
    Partitioning strategy for GZipped compressed text files, it partitions the text based on number of lines
    per partition
    :param cloud_object: Parent cloud object
    :param lines_per_chunk: Number of lines for each partition
    :param strategy: What to do with remaining lines: 'expand' to create a new chunk with them (less than lines_per_chunk),
           or 'merge' to add these lines to the previous chunk (more than lines_per_chunk).
    :return:
    """
    total_lines = int(cloud_object.get_attribute("total_lines"))
    parts = ceil(total_lines / lines_per_chunk)
    pairs = [((lines_per_chunk * i) + 1, (lines_per_chunk * i) + lines_per_chunk) for i in range(parts)]

    # Adjust last pair
    if pairs[-1][1] > total_lines:
        if strategy == "expand":
            l0, _ = pairs[-1]
            pairs[-1] = (l0, total_lines)
        elif strategy == "merge":
            l0, l1 = pairs.pop()
            extra = l1 - l0
            pair = pairs[-1]
            pairs[-1] = pair[0], pair[1] + extra
        else:
            raise Exception(f"Unknown strategy {strategy}")

    byte_ranges = _get_ranges_from_line_pairs(cloud_object, pairs)
    chunks = [
        GZipTextSlice(line_0, line_1, range_0, range_1)
        for (line_0, line_1), (range_0, range_1) in zip(byte_ranges, pairs)
    ]

    return chunks


def partition_num_chunks(self, n_chunks):
    # TODO implement this strategy
    raise NotImplementedError()


@CloudDataFormatTemplate(preprocessor=GZipTextPreprocessor)
class GZipText:
    pass


class GZipTextSlice(CloudObjectSlice):
    def __init__(self, line_0, line_1, *args, **kwargs):
        self.line_0 = line_0
        self.line_1 = line_1
        super().__init__(*args, **kwargs)

    def get(self):
        tmp_index_file = tempfile.mktemp()
        gztool = _get_gztool_path()
        lines = []
        lines_to_read = self.line_1 - self.line_0 + 1

        try:
            t0 = time.perf_counter()
            # Get index and store it to temp file
            self.cloud_object.storage.download_file(
                Bucket=self.cloud_object.meta_path.bucket,
                Key=self.cloud_object["index_key"],
                Filename=tmp_index_file,
            )

            # Get compressed byte range
            res = self.cloud_object.storage.get_object(
                Bucket=self.cloud_object.path.bucket,
                Key=self.cloud_object.path.key,
                Range=f"bytes={self.range_0 - 1}-{self.range_1 - 1}",
            )
            body = res["Body"]

            cmd = [
                gztool,
                "-I",
                tmp_index_file,
                "-n",
                str(self.range_0),
                "-L",
                str(self.line_0),
            ]
            proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE)

            # TODO program might get stuck if subprocess fails, blocking io should be done in a backgroun thread or using async/await
            def _writer_feeder():
                logger.debug("Writer thread started")
                input_chunk = body.read(CHUNK_SIZE)
                while input_chunk != b"":
                    # logger.debug('Writing %d bytes to pipe', len(chunk))
                    try:
                        proc.stdin.write(input_chunk)
                    except BrokenPipeError:
                        break
                    input_chunk = body.read(CHUNK_SIZE)
                try:
                    proc.stdin.flush()
                    proc.stdin.close()
                except BrokenPipeError:
                    pass
                logger.debug("Writer thread finished")

            writer_thread = threading.Thread(target=_writer_feeder)
            writer_thread.start()

            output_chunk = proc.stdout.read(CHUNK_SIZE)
            last_line = None
            with tqdm.tqdm(total=lines_to_read) as pb:
                while output_chunk != b"":
                    # logger.debug('Read %d bytes from pipe', len(chunk))
                    text = output_chunk.decode("utf-8")
                    chunk_lines = text.splitlines()

                    if last_line is not None:
                        last_line = last_line + chunk_lines.pop(0)
                        lines.append(last_line)
                        last_line = None

                    if text[-1] != "\n":
                        last_line = chunk_lines.pop()

                    lines.extend(chunk_lines)
                    pb.update(len(chunk_lines))

                    # Stop decompressing lines if number of lines to read in this chunk is reached
                    if len(lines) > lines_to_read:
                        proc.stdout.close()
                        break

                    # Try to read next decompressed chunk
                    # a ValueError is raised if the pipe is closed, meaning the writer or the subprocess closed it
                    try:
                        output_chunk = proc.stdout.read(CHUNK_SIZE)
                    except ValueError:
                        output_chunk = b""

            try:
                proc.wait()
            except ValueError as e:
                logger.error(e)

            writer_thread.join()

            t1 = time.perf_counter()
            logger.debug("Got partition in %.3f seconds", t1 - t0)

            return lines[: self.line_1 - self.line_0]
        finally:
            force_delete_path(tmp_index_file)
