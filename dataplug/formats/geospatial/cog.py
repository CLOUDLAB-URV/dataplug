import logging
import math
import os
from typing import TYPE_CHECKING, List

import boto3
from rasterio.session import AWSSession
import rasterio
from rasterio.windows import Window
import rasterio.transform

from dataplug import CloudObject
from dataplug.entities import CloudDataFormat, CloudObjectSlice, PartitioningStrategy
from dataplug.preprocessing.metadata import PreprocessingMetadata
from dataplug.util import setup_logging

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
        super().__init__()

    def get(self):
        meta = extract_meta(self.cloud_object)
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
        meta = extract_meta(self.cloud_object)
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
def block_window_strategy(cloud_object: CloudObject) -> List[BlockWindowSlice]:
    """
    Partition the COG using its internal block windows.
    """
    with cloud_object.open("rb") as cog_file:
        with rasterio.open(cog_file) as src:
            # Retrieve block windows from band 1 (assuming all bands share the same tiling)
            windows = list(src.block_windows(1))
    logger.info("Found %d block windows for COG %s", len(windows), cloud_object.path.key)
    slices = [BlockWindowSlice(window) for _, window in windows]
    logger.debug("Created %d block window slices for COG %s", len(slices), cloud_object.path.key)
    return slices
