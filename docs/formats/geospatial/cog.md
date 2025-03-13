# COG

The COG (Cloud Optimized GeoTIFF) plugin provides an efficient way to partition large geospatial raster datasets stored in object storage (e.g., Amazon S3) using Dataplug. This plugin leverages specialized libraries to access and process COG files directly from cloud storage. It extracts essential metadata and supports grid-based partitioning to enable parallel processing of individual image blocks.






## Installation

The COG plugin requires the following python packages:

- **rasterio**
- **numpy**

## Partitioning strategies

`grid_partition_strategy`: Splits the raster image into a grid of squared sub-regions. Specify the number of splits along each axis to partition the image into an equal grid for parallel processing.
