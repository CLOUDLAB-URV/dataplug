# Cloud-Optimized Point Cloud

## Installation

The COPC plugin requires the following python packages:

```
laspy
PDAL
```

You can install them using `pip`.

## Partitioning strategies

- `square_split_strategy`: Splits the surface into a given number of squared sub-regions.