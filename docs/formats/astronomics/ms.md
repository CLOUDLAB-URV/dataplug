# MeasurementSet

## Installation

The MeasurementSet plugin requires the following python packages:

```
casacore
```

You can install them using `pip`.

Additionally, the MeasurementSet plugin requires ... for pre-processing.
You can follow the instructions [here](...) to install it.


## Partitioning strategies

- `ms_partitioning_strategy`: Splits the data into a given number of sub-sets that are individually viable MeasurementSet files. 
- `ms_partitioning_strategy_rowsize`: Splits the data into as many individually viable MeasurementSet files necessary to fulfill that each of them has the given number of rows. 