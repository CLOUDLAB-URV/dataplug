import logging
import math
import os
from typing import TYPE_CHECKING, List

import boto3
from rasterio.session import AWSSession
import rasterio
from rasterio.windows import Window

from dataplug import CloudObject
from dataplug.entities import CloudDataFormat, CloudObjectSlice, PartitioningStrategy
from dataplug.preprocessing.metadata import PreprocessingMetadata

if TYPE_CHECKING:
    from dataplug.cloudobject import CloudObject

# Set environment variables for GDAL and AWS
os.environ["GDAL_HTTP_UNSAFESSL"] = "YES"
os.environ["CPL_VSIL_CURL_USE_HEAD"] = "NO"
os.environ["AWS_REQUEST_PAYER"] = "requester"

# Logger configuration
logger = logging.getLogger(__name__)

# Configure the AWS session (requester pays)
aws_session = AWSSession(boto3.Session(), requester_pays=True)


def preprocess_cog(cloud_object: CloudObject) -> PreprocessingMetadata:
    logger.info("Starting COG preprocessing: %s", cloud_object.path.key)
    with cloud_object.open(mode="rb") as cog_file:
        with rasterio.open(cog_file) as src:
            meta = src.meta
            bounds = src.bounds
            crs = src.crs.to_string()
            width = src.width
            height = src.height
            attrs = {
                "width": width,
                "height": height,
                "crs": crs,
                "bounds": {
                    "left": bounds.left,
                    "bottom": bounds.bottom,
                    "right": bounds.right,
                    "top": bounds.top,
                },
                "transform": list(src.transform),
            }
            logger.debug("Extracted metadata: %s", attrs)
    return PreprocessingMetadata(attributes=attrs)


@CloudDataFormat(preprocessing_function=preprocess_cog)
class CloudOptimizedGeoTiff:
    width: int
    height: int
    crs: str
    bounds: dict
    transform: list


def extract_meta(cloud_object: CloudObject):
    meta = cloud_object.attributes
    if hasattr(meta, "_asdict"):
        return meta._asdict()
    elif isinstance(meta, dict):
        return meta
    elif hasattr(meta, "__dict__"):
        return meta.__dict__
    else:
        return meta


class BlockWindowSlice(CloudObjectSlice):
    def __init__(self, window: Window):
        self.window = window
        # Attributes to identify the grid position and tile
        self.block_x = None
        self.block_y = None
        self.tile_key = None
        super().__init__()

    def get(self):
        file_url = self.cloud_object.storage.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": self.cloud_object.path.bucket,
                "Key": self.cloud_object.path.key,
                "RequestPayer": "requester",
            },
            ExpiresIn=300,
        )
        try:
            with rasterio.Env(aws_session=aws_session):
                with rasterio.open(file_url) as src:
                    logger.debug("Reading block window %s from COG %s", self.window, self.cloud_object.path.key)
                    data = src.read(window=self.window)
            return data
        except Exception as e:
            logger.error("Error fetching data from COG %s: %s", self.cloud_object.path.key, e)
            raise

    def to_file(self, file_name: str):
        file_url = self.cloud_object.storage.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": self.cloud_object.path.bucket,
                "Key": self.cloud_object.path.key,
                "RequestPayer": "requester",
            },
            ExpiresIn=300,
        )
        try:
            with rasterio.Env(aws_session=aws_session):
                with rasterio.open(file_url) as src:
                    profile = src.profile
                    profile.update({
                        "driver": "GTiff",
                        "width": int(self.window.width),
                        "height": int(self.window.height),
                        "transform": rasterio.windows.transform(self.window, src.transform),
                    })
                    logger.debug("Writing block window %s to file %s", self.window, file_name)
                    with rasterio.open(file_name, "w", **profile) as dst:
                        dst.write(src.read(window=self.window))
        except Exception as e:
            logger.error("Error writing file %s for COG %s: %s", file_name, self.cloud_object.path.key, e)
            raise

@PartitioningStrategy(CloudOptimizedGeoTiff)
def grid_partition_strategy(cloud_object: CloudObject, n_splits) -> List[BlockWindowSlice]:
    """
    Splits the COG into a grid of n_splits x n_splits.

    - n_splits: Number of splits along each axis.

    Additionally, the tile identifier (tile_key) is extracted from the file name.
    """
    with cloud_object.open("rb") as cog_file:
        with rasterio.open(cog_file) as src:
            # Extract the tile_key from the file name (without extension)
            tile_key = os.path.basename(cloud_object.path.key)
            step_w = src.width / n_splits
            step_h = src.height / n_splits

            slices = []
            for block_x in range(n_splits):
                for block_y in range(n_splits):
                    offset_w = round(step_w * block_y)
                    offset_h = round(step_h * block_x)
                    width = math.ceil(step_w * (block_y + 1)) - offset_w
                    height = math.ceil(step_h * (block_x + 1)) - offset_h

                    window = Window(offset_w, offset_h, width, height)
                    slice_obj = BlockWindowSlice(window)
                    slice_obj.block_x = block_x
                    slice_obj.block_y = block_y
                    slice_obj.tile_key = tile_key
                    slices.append(slice_obj)

    logger.info("Created %d grid slices for COG %s with n_splits=%d", 
                len(slices), cloud_object.path.key, n_splits)
    return slices
