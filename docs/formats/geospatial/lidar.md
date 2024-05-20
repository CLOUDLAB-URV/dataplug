# LiDAR

## Installation

The LiDAR plugin requires the following python packages:

```
laspy
PDAL
```

You can install them using `pip`.

Additionally, the LiDAR plugin requires `lasquery` from `lastools` for pre-processing.
You can follow the instructions [here](https://github.com/aitorarjona/LAStools/tree/lasquery) to install it.
The `lasquery` executable must be in your `PATH`.


## Partitioning strategies

- `square_split_strategy`: Splits the surface into a given number of squared sub-regions.