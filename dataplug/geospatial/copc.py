from __future__ import annotations

import logging
import math
import io
from typing import TYPE_CHECKING

import numpy as np

from ..cloudobject import CloudDataType, CloudObjectSlice
from ..preprocessing import BatchPreprocessor, PreprocessingMetadata

if TYPE_CHECKING:
    from typing import List
    from ..cloudobject import CloudObject

logger = logging.getLogger(__name__)


class COPCPreprocessor(BatchPreprocessor):
    def __init__(self):
        try:
            import pdal
            import laspy.copc
        except ModuleNotFoundError as e:
            logger.error("Missing Geospatial packages!")
            raise e
        super().__init__()

    def preprocess(self, cloud_object: CloudObject) -> PreprocessingMetadata:
        """
        This preprocessing job opens a COPC file and extracts some attributes related to this tile
        :param cloud_object:
        :return:
        """
        import laspy.copc

        with cloud_object.open(mode="rb") as copc_file:
            copc_reader = laspy.copc.CopcReader(copc_file)
            copc_attrs = {
                "points": copc_reader.header.point_count,
                "x_scale": copc_reader.header.x_scale,
                "y_scale": copc_reader.header.y_scale,
                "z_scale": copc_reader.header.z_scale,
                "x_offset": copc_reader.header.x_offset,
                "y_offset": copc_reader.header.y_offset,
                "z_offset": copc_reader.header.z_offset,
                "x_max": copc_reader.header.x_max,
                "y_max": copc_reader.header.y_max,
                "z_max": copc_reader.header.z_max,
                "x_min": copc_reader.header.x_min,
                "y_min": copc_reader.header.y_min,
                "z_min": copc_reader.header.z_min,
                "root_offset": copc_reader.copc_info.hierarchy_root_offset,
                "root_size": copc_reader.copc_info.hierarchy_root_size,
            }
        print(copc_attrs)

        return PreprocessingMetadata(attributes=copc_attrs)


@CloudDataType(preprocessor=COPCPreprocessor)
class CloudOptimizedPointCloud:
    """
    Cloud Data Type for the COPC file format
    """

    attribute1 = None
    attribute2: int = -2
    points: int
    x_scale: float
    y_scale: float
    z_scale: float
    x_offset: float
    y_offset: float
    z_offset: float
    x_max: float
    y_max: float
    z_max: float
    x_min: float
    y_min: float
    z_min: float
    root_offset: float
    root_size: float


class COPCSlice(CloudObjectSlice):
    def __init__(self, splits_x, splits_y, slice_x, slice_y):
        self.splits_x = splits_x
        self.splits_y = splits_y
        self.slice_x = slice_x
        self.slice_y = slice_y
        super().__init__()

    def _get_points(self):
        from laspy.copc import CopcReader, Bounds
        import laspy

        file_url = self.cloud_object.s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.cloud_object.path.bucket, "Key": self.cloud_object.path.key},
            ExpiresIn=300,
        )

        with CopcReader.open(file_url) as copc_file:
            min_x, min_y = copc_file.header.mins[0], copc_file.header.mins[1]
            max_x, max_y = copc_file.header.maxs[0], copc_file.header.maxs[1]

            x_size = (max_x - min_x) / self.splits_x
            y_size = (max_y - min_y) / self.splits_y

            x_min_bound = (x_size * self.slice_x) + min_x
            y_min_bound = (y_size * self.slice_y) + min_y
            x_max_bound = x_min_bound + x_size
            y_max_bound = y_min_bound + y_size

            query_bounds = Bounds(
                mins=np.asarray([x_min_bound, y_min_bound]),
                maxs=np.asarray([x_max_bound, y_max_bound]),
            )

            points = copc_file.query(query_bounds)
            new_header = laspy.LasHeader(
                version=copc_file.header.version,
                point_format=copc_file.header.point_format,
            )
            new_header.offsets = copc_file.header.offsets
            new_header.scales = copc_file.header.scales

            crs = copc_file.header.parse_crs()
            new_header.add_crs(crs, keep_compatibility=True)

            return points, new_header

    def get(self):
        import laspy

        points, header = self._get_points()

        out_buff = io.BytesIO()
        with laspy.open(out_buff, mode="w", header=header, closefd=False) as output:
            output.write_points(points)

        return_value = out_buff.getvalue()

        return return_value

    def to_file(self, file_name):
        import laspy

        points, header = self._get_points()

        out_buff = io.BytesIO()
        with laspy.open(file_name, mode="w", header=header) as output:
            output.write_points(points)


def copc_square_split_strategy(cloud_object: CloudOptimizedPointCloud, num_chunks: int) -> List[COPCSlice]:
    """
    This partition strategy chunks a COPC file in equal spatial squared chunks.
    'num_chunks' must be a perfect square, otherwise the number of chunks will be rounded to the closes perfect
    square. E.g. for num_chunks=4, the tile will be split in 2*2 sub-tile, but for num_chunks=7, the tile will be split
    in 3*2 tiles.
    """
    x = round(math.sqrt(num_chunks))
    y = round(num_chunks / x)
    real_num_chunks = x * y
    if real_num_chunks != num_chunks:
        logger.warning(
            "Created %d partitions of size %d x %d as %d is not a perfect square",
            real_num_chunks,
            x,
            y,
            num_chunks,
        )
    else:
        logger.info("Created %d partitions of size %d x %d", real_num_chunks, x, y)
    slices = [COPCSlice(x, y, ix, iy) for ix in range(x) for iy in range(y)]
    return slices
