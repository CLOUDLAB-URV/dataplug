from __future__ import annotations

import io
import logging
import math
import subprocess
import tempfile
import typing
from concurrent.futures import ThreadPoolExecutor

import tqdm

from dataplug.cloudobject import CloudDataType, CloudObject
from dataplug.dataslice import CloudObjectSlice
from dataplug.preprocessing import BatchPreprocessor, PreprocessingMetadata
from dataplug.util import force_delete_path

if typing.TYPE_CHECKING:
    from typing import List, Tuple

try:
    import laspy
except ModuleNotFoundError:
    pass

logger = logging.getLogger(__name__)

CHUNK_SIZE = 65536


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

            obj_res = cloud_object.s3.get_object(Bucket=cloud_object.path.bucket, Key=cloud_object.path.key)
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
                    "offset_to_point_data": las_file.header.offset_to_point_data
                }

            with open(tmp_index_file_path, "rb") as index_file:
                lax_data = index_file.read()

            return PreprocessingMetadata(metadata=lax_data, attributes=las_meta)
        finally:
            force_delete_path(tmp_index_file_path)


@CloudDataType(preprocessor=LiDARPreprocessor)
class LiDARPointCloud:
    def __init__(self, cloud_object):
        self.cloud_object = cloud_object


class LiDARSlice(CloudObjectSlice):
    def __init__(self, min_x: float, min_y: float, max_x: float, max_y: float, byte_ranges: List[Tuple[int, int]]):
        self.min_x = min_x
        self.min_y = min_y
        self.max_x = max_x
        self.max_y = max_y
        self.byte_ranges = byte_ranges
        super().__init__()

    def _get_points(self, buffer):
        # Download original file header
        byte_range = f"bytes=0-{self.cloud_object.attributes.offset_to_point_data}"
        res = self.cloud_object.s3.get_object(Bucket=self.cloud_object.path.bucket, Key=self.cloud_object.path.key,
                                              Range=byte_range)
        assert res.get("ResponseMetadata", {}).get("HTTPStatusCode") in (200, 206)
        header_buff = io.BytesIO(res["Body"].read())
        header_buff.seek(0)

        with laspy.open(header_buff, "r") as lasf:
            header = lasf.header

        las_chunk = laspy.open(buffer, "w", header=header, closefd=False)

        def _fetch_interval(inverval_byte_range):
            range_offset_0, range_offset_1 = inverval_byte_range
            byte_range = f"bytes={range_offset_0}-{range_offset_1 - 1}"
            res = self.cloud_object.s3.get_object(Bucket=self.cloud_object.path.bucket, Key=self.cloud_object.path.key,
                                                  Range=byte_range)
            assert res.get("ResponseMetadata", {}).get("HTTPStatusCode") in (200, 206)
            body = res["Body"].read()

            # Read point interval into a mini-las container
            points = laspy.PackedPointRecord.from_buffer(buffer=body, point_format=header.point_format)
            las_interval = laspy.LasData(header=lasf.header, points=points)
            las_interval.update_header()

            # Filter out points not pertaining to this partition
            mask = (las_interval.x >= self.min_x) & (las_interval.x <= self.max_x) \
                   & (las_interval.y >= self.min_y) & (las_interval.y <= self.max_y)
            points_filtered = las_interval.points[mask]
            return points_filtered

        # with ThreadPoolExecutor(max_workers=len(self.byte_ranges)) as pool:
        with ThreadPoolExecutor(max_workers=len(self.byte_ranges)) as pool:
            res = pool.map(_fetch_interval, self.byte_ranges)
            for chunk in res:
                las_chunk.write_points(chunk)

        las_chunk.close()

    def get(self):
        buffer = io.BytesIO()
        self._get_points(buffer)
        return buffer.getvalue()

    def to_file(self, file_name):
        with open(file_name, "wb") as buffer:
            self._get_points(buffer)


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
        cloud_object.s3.download_file(cloud_object.meta_path.bucket, cloud_object.meta_path.key, tmp_index_file_path)

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
        data_slice = LiDARSlice(x_min_bound, y_min_bound, x_max_bound, y_max_bound, byte_ranges)
        slices.append(data_slice)

    return slices
