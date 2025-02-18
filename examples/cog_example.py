from dataplug import CloudObject
from dataplug.formats.geospatial.cog import CloudOptimizedGeoTiff, block_window_strategy
import rasterio
from rasterio.windows import Window, from_bounds
import rasterio.transform
import logging
from rasterio.session import AWSSession
import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # Show debug messages
aws_session = AWSSession(boto3.Session(), requester_pays=True)

def main():
    s3_config = {"region_name": "us-west-2"}
    cog_url = "s3://sentinel-cogs/sentinel-s2-l2a-cogs/60/C/WQ/2023/12/S2B_60CWQ_20231228_1_L2A/AOT.tif"
    co = CloudObject.from_s3(CloudOptimizedGeoTiff, cog_url, s3_config=s3_config)
    co.preprocess()
    
    # Partition the COG using block windows strategy.
    data_slices = co.partition(block_window_strategy)
    total_pixels = 0
    for idx, data_slice in enumerate(data_slices):
        try:
            chunk_data = data_slice.get()
            print(chunk_data)
            print(f"Slice {idx} data shape: {chunk_data.shape}")
            total_pixels += chunk_data.size
        except Exception as e:
            logger.error("Error retrieving slice %d: %s", idx, e)
    print("Total pixels read across all slices:", total_pixels)

if __name__ == "__main__":
    main()
