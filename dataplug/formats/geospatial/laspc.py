from __future__ import annotations

import io
import math
import subprocess
import tempfile
import typing
from concurrent.futures import ThreadPoolExecutor

import tqdm

from ...core import *
from ...preprocessing import BatchPreprocessor, PreprocessingMetadata
from ...util import force_delete_path

if typing.TYPE_CHECKING:
    from typing import List, Tuple

try:
    import laspy
except ModuleNotFoundError:
    pass

logger = logging.getLogger(__name__)

CHUNK_SIZE = 65536


@CloudDataFormat
class LiDARPointCloud:
    pass


def _get_lasindex_path():
    proc = subprocess.run(["which", "lasindex"], check=True, capture_output=True, text=True)
    path = proc.stdout.rstrip("\n")
    logger.debug("Using lasindex located in %s", path)
    return path


def _get_laxquery_path():
    proc = subprocess.run(["which", "laxquery"], check=True, capture_output=True, text=True)
    path = proc.stdout.rstrip("\n")
    logger.debug("Using lasindex located in %s", path)
    return path


@FormatPreprocessor(LiDARPointCloud)
class LiDARPreprocessor(BatchPreprocessor):
    def __init__(self):
        try:
            import laspy
        except ModuleNotFoundError as e:
            logger.error("Missing Geospatial packages!")
            raise e
        super().__init__()

    def preprocess(self, cloud_object: CloudObject) -> PreprocessingMetadata:
        lasindex = _get_lasindex_path()

        # lasindex tool requires index terminated with .lax
        tmp_index_file_path = tempfile.mktemp() + ".lax"

        try:
            force_delete_path(tmp_index_file_path)

            obj_res = cloud_object.storage.get_object(Bucket=cloud_object.path.bucket, Key=cloud_object.path.key)
            assert obj_res.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
            data_stream = obj_res["Body"]

            cmd = [lasindex, "-stdin", "-o", tmp_index_file_path]
            logger.debug(" ".join(cmd))
            index_proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

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
                    # index_proc.stdin.close()
                except BrokenPipeError as e:
                    stdout, stderr = index_proc.communicate()
                    logger.error(stdout.decode("utf-8"))
                    logger.error(stderr.decode("utf-8"))
                    raise e

            stdout, stderr = index_proc.communicate()
            logger.debug(stdout.decode("utf-8"))
            logger.debug(stderr.decode("utf-8"))

            # Get attributes
            with cloud_object.open("rb") as input_file:
                las_file = laspy.open(source=input_file, closefd=False)
                las_meta = {
                    "mins": las_file.header.mins,
                    "maxs": las_file.header.maxs,
                    "point_count": las_file.header.point_count,
                    "point_format_size": las_file.header.point_format.size,
                    "offset_to_point_data": las_file.header.offset_to_point_data,
                }

            with open(tmp_index_file_path, "rb") as index_file:
                lax_data = index_file.read()

            return PreprocessingMetadata(metadata=lax_data, attributes=las_meta)
        finally:
            force_delete_path(tmp_index_file_path)


class LiDARSlice(CloudObjectSlice):
    def __init__(
        self,
        min_x: float,
        min_y: float,
        max_x: float,
        max_y: float,
        las_file_byte_ranges: List[Tuple[int, int]],
        buffer_size: int,
    ):
        self.min_x = min_x
        self.min_y = min_y
        self.max_x = max_x
        self.max_y = max_y
        self.las_file_byte_ranges = las_file_byte_ranges
        self.buffer_size = buffer_size
        super().__init__()

    def _get_lasdata(self):
        # Download original file header
        byte_range = f"bytes=0-{self.cloud_object.attributes.offset_to_point_data}"
        res = self.cloud_object.storage.get_object(
            Bucket=self.cloud_object.path.bucket, Key=self.cloud_object.path.key, Range=byte_range
        )
        assert res.get("ResponseMetadata", {}).get("HTTPStatusCode") in (200, 206)
        header_buff = io.BytesIO(res["Body"].read())
        header_buff.seek(0)

        with laspy.open(header_buff, "r") as lasf:
            header = lasf.header

        buffer = bytearray(self.buffer_size)

        # Calculate offsets for local buffer
        buffer_offsets = []
        previous = 0
        for byte_range in self.las_file_byte_ranges:
            r0, r1 = byte_range
            size = r1 - r0
            offset_0, offset_1 = previous, previous + size
            buffer_offsets.append((offset_0, offset_1))
            previous = offset_1

        def _fetch_interval(args):
            interval_byte_range, buffer_offsets = args
            range_0, range_1 = interval_byte_range
            # print(range_1 - range_0)
            byte_range = f"bytes={range_0}-{range_1 - 1}"
            # print(byte_range)
            res = self.cloud_object.storage.get_object(
                Bucket=self.cloud_object.path.bucket, Key=self.cloud_object.path.key, Range=byte_range
            )
            assert res.get("ResponseMetadata", {}).get("HTTPStatusCode") in (200, 206)
            body = res["Body"].read()

            offset_0, offset_1 = buffer_offsets
            # print(offset_1, offset_1, offset_1 - offset_0)
            mem_buff = memoryview(buffer)
            # print(range_1 - range_0, len(body))
            mem_buff[offset_0:offset_1] = body
            return True

        with ThreadPoolExecutor(max_workers=32) as pool:
            res = pool.map(_fetch_interval, zip(self.las_file_byte_ranges, buffer_offsets))
            all(res)

        # Read point interval into a mini-las container
        points = laspy.PackedPointRecord.from_buffer(buffer=memoryview(buffer), point_format=header.point_format)
        las_chunk = laspy.LasData(header=lasf.header, points=points)
        las_chunk.update_header()

        # Filter out points not pertaining to this partition
        mask = (
            (las_chunk.x >= self.min_x)
            & (las_chunk.x <= self.max_x)
            & (las_chunk.y >= self.min_y)
            & (las_chunk.y <= self.max_y)
        )
        las_chunk.points = las_chunk.points[mask]
        return las_chunk

    def get(self):
        return self._get_lasdata()

    def to_file(self, file_name):
        self._get_lasdata().write(file_name)


@PartitioningStrategy(LiDARPointCloud)
def square_split_strategy(cloud_object: CloudObject, num_chunks: int) -> List[LiDARSlice]:
    """
    This partition strategy chunks a LAS file in equal spatial squared chunks.
    'num_chunks' must be a perfect square, otherwise the number of chunks will be rounded to the closest perfect
    square. E.g. for num_chunks=4, the tile will be split in 2*2 sub-tile, but for num_chunks=7, the tile will be split
    in 3*3 tiles.
    """
    # Calculate how many splits for each abscissa
    splits_x = round(math.sqrt(num_chunks))
    splits_y = round(num_chunks / splits_x)

    # Get LAS file metadata such as bounding coordinates
    min_x, min_y = cloud_object.attributes.mins[0], cloud_object.attributes.mins[1]
    max_x, max_y = cloud_object.attributes.maxs[0], cloud_object.attributes.maxs[1]
    offset_to_point_data = cloud_object.attributes.offset_to_point_data
    point_size = cloud_object.attributes.point_format_size

    # Calculate the size of each sub-tile
    x_size = (max_x - min_x) / splits_x
    y_size = (max_y - min_y) / splits_y

    # Calculate the coordinate bounds (x_min, y_min) and (x_max, y_max) for each sub-tile
    bounds = []
    for i in range(splits_x):
        for j in range(splits_y):
            x_min_bound = (x_size * i) + min_x
            y_min_bound = (y_size * j) + min_y
            x_max_bound = x_min_bound + x_size
            y_max_bound = y_min_bound + y_size
            bounds.append((x_min_bound, y_min_bound, x_max_bound, y_max_bound))

    # Convert to string
    bounds_str_fmt = []
    for bound in bounds:
        bounds_str_fmt.append(",".join(str(coord) for coord in bound))

    # Download the index file and store to local temp file
    # LASIndex requires filename with .lax suffix
    tmp_index_file_path = tempfile.mktemp() + ".lax"
    try:
        cloud_object.storage.download_file(
            cloud_object.meta_path.bucket, cloud_object.meta_path.key, tmp_index_file_path
        )

        cmd = [_get_laxquery_path(), tmp_index_file_path]
        cmd.extend(bounds_str_fmt)
        logger.debug(" ".join(cmd))
        out = subprocess.check_output(cmd)
    finally:
        force_delete_path(tmp_index_file_path)

    # Convert raw text output to list of int tuples
    lines = out.splitlines()
    point_chunks = []
    for line in lines:
        # Remove trailing ;
        line = line.decode("utf-8")[:-1]
        intervals = []
        for interval in line.split(";"):
            start, end = interval.split(",")
            start, end = int(start), int(end)
            if (
                end - start
            ) < 100:  # Skip intervals that are too small, yes we're losing points but nobody has to know
                continue
            intervals.append((start, end))
        point_chunks.append(intervals)

    # Convert ranges of points to byte ranges based on point record size
    chunks_byte_ranges = []
    for point_chunk in point_chunks:
        byte_intervals = []
        for start, end in point_chunk:
            byte_0, byte_1 = (start * point_size) + offset_to_point_data, (end * point_size) + offset_to_point_data
            byte_intervals.append((byte_0, byte_1))
        chunks_byte_ranges.append(byte_intervals)

    # Create slice objects
    slices = []
    for bound, byte_ranges in zip(bounds, chunks_byte_ranges):
        x_min_bound, y_min_bound, x_max_bound, y_max_bound = bound
        buffer_size = sum((range_1 - range_0) for range_0, range_1 in byte_ranges)
        data_slice = LiDARSlice(x_min_bound, y_min_bound, x_max_bound, y_max_bound, byte_ranges, buffer_size)
        slices.append(data_slice)

    return slices
