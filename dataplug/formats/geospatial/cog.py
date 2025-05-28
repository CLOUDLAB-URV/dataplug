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
    uri = f"s3://{cloud_object.path.bucket}/{cloud_object.path.key}"
    with rasterio.Env(aws_session=aws_session):
        with rasterio.open(uri) as src:
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

    def __getstate__(self):
        state = self.__dict__.copy()
        # Store the Window as a tuple of numeric values (col_off, row_off, width, height)
        state['window'] = (
            float(self.window.col_off),
            float(self.window.row_off),
            float(self.window.width),
            float(self.window.height)
        )
        return state

    def __setstate__(self, state):
        if 'window' in state and isinstance(state['window'], tuple):
            try:
                col_off = float(state['window'][0])
                row_off = float(state['window'][1])
                width_val = float(state['window'][2])
                height_val = float(state['window'][3])
            except Exception as e:
                logger.error("Error converting tuple window values: %s", state['window'])
                raise e
            state['window'] = Window(col_off, row_off, width_val, height_val)
        self.__dict__.update(state)
        
    def get(self):
        url = self.cloud_object.storage.generate_presigned_url(
            "get_object",
            Params={
                "Bucket": self.cloud_object.path.bucket,
                "Key": self.cloud_object.path.key,
                "RequestPayer": "requester",
            },
            ExpiresIn=300,
        )
        with rasterio.Env(aws_session=aws_session):
            with rasterio.open(url) as src:
                data = src.read(window=self.window)
        return data

    def to_file(self, file_name: str):
        # Ensure self.window is an instance of Window
        if not isinstance(self.window, Window):
            if isinstance(self.window, tuple):
                try:
                    col_off = float(self.window[0])
                    row_off = float(self.window[1])
                    width_val = float(self.window[2])
                    height_val = float(self.window[3])
                except Exception as e:
                    logger.error("Error converting tuple window values: %s", self.window)
                    raise e
                self.window = Window(col_off, row_off, width_val, height_val)
            elif isinstance(self.window, dict):
                try:
                    col_off = float(self.window.get('col_off'))
                    row_off = float(self.window.get('row_off'))
                    width_val = float(self.window.get('width'))
                    height_val = float(self.window.get('height'))
                except Exception as e:
                    logger.error("Error converting dict window values: %s", self.window)
                    raise e
                self.window = Window(col_off, row_off, width_val, height_val)

        logger.debug("Final value of self.window: %s", self.window)
        logger.debug("self.window.width: %s", self.window.width)
        logger.debug("self.window.height: %s", self.window.height)

        # Convert width and height to integers, ensuring they are numeric
        try:
            width_int = int(self.window.width)
            height_int = int(self.window.height)
        except ValueError as e:
            logger.error("Window dimensions are not numeric: width=%s, height=%s", self.window.width, self.window.height)
            raise e

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
                    profile = src.profile.copy()
                    profile.update({
                        "driver": "GTiff",
                        "width": width_int,
                        "height": height_int,
                        "transform": rasterio.windows.transform(self.window, src.transform),
                    })
                    logger.debug("Writing block window %s to file %s", self.window, file_name)
                    with rasterio.open(file_name, "w", **profile) as dst:
                        dst.write(src.read(window=self.window))
        except Exception as e:
            logger.error("Error writing file %s for COG %s: %s", file_name, self.cloud_object.path.key, e)
            raise


@PartitioningStrategy(CloudOptimizedGeoTiff)
def grid_partition_strategy(cloud_object: CloudObject, n_splits: int) -> List[BlockWindowSlice]:
    """
    Splits the COG into a grid of n_splits x n_splits.

    - n_splits: Number of splits along each axis.

    Additionally, the tile identifier (tile_key) is extracted from the file name.
    """
    # Validate n_splits
    if not isinstance(n_splits, int) or n_splits < 1:
        raise ValueError(f"n_splits must be a positive integer, got {n_splits}")

    tile_key = os.path.basename(cloud_object.path.key)
    uri = f"s3://{cloud_object.path.bucket}/{cloud_object.path.key}"
    slices: List[BlockWindowSlice] = []

    with rasterio.Env(aws_session=aws_session):
        with rasterio.open(uri) as src:
            step_w = src.width / n_splits
            step_h = src.height / n_splits

            for bx in range(n_splits):
                for by in range(n_splits):
                    off_w = round(step_w * by)
                    off_h = round(step_h * bx)
                    w = math.ceil(step_w * (by + 1)) - off_w
                    h = math.ceil(step_h * (bx + 1)) - off_h

                    window = Window(off_w, off_h, w, h)
                    sl = BlockWindowSlice(window)
                    sl.block_x = bx
                    sl.block_y = by
                    sl.tile_key = tile_key
                    slices.append(sl)

    logger.info("Created %d grid slices for COG %s with n_splits=%d",
                len(slices), cloud_object.path.key, n_splits)
    return slices
