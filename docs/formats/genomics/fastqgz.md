# FASTQGZip

Plugin to partition compressed FASTQ data stored in object storage.

## Install

This plugin requires to have `gztool` binary accessingle from the PATH.
To install it, follow the instructions in the [gztool repository](https://github.com/circulosmeos/gztool).
The tested version is v1.4.3, but other versions may work as well.

## Partitioning strategies

- `partition_reads_batches`: partitions the reads in a given number of batches.
- `partition_sequences_per_chunk`: partitions the reads by a given number of reads per partition.
