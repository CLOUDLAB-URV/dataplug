from math import ceil
from typing import List

from dataplug import CloudDataFormat, PartitioningStrategy, FormatPreprocessor

from ..compressed.gzipped import GZipText, GZipTextPreprocessor, GZipTextSlice, _get_ranges_from_line_pairs


@CloudDataFormat
class FASTQGZip:
    pass


@FormatPreprocessor(FASTQGZip)
class FASTQGZipPreprocessor(GZipTextPreprocessor):
    pass


class FASTQGZipSlice(GZipTextSlice):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


@PartitioningStrategy(FASTQGZip)
def partition_reads_batches(cloud_object: FASTQGZip, num_batches: int) -> List[GZipTextSlice]:
    total_lines = int(cloud_object.get_attribute("total_lines"))

    # Check if number of lines is a multiple of 4 (FASTQ reads are 4 lines each)
    if (total_lines % 4) != 0:
        raise Exception("Number of lines does not correspond to FASTQ reads format!")

    # Split by number of reads per worker (each read is composed of 4 lines)
    num_reads = total_lines // 4
    reads_batch = ceil(num_reads / num_batches)
    read_pairs = [(reads_batch * i, (reads_batch * i) + reads_batch) for i in range(num_batches)]

    # Convert read pairs back to line numbers (starting in 1)
    # For each tuple -> [line_0, line_1)  (first element is inclusive, second element is exclusive)
    line_pairs = [((l0 * 4) + 1, (l1 * 4) + 1) for l0, l1 in read_pairs]

    # Adjust last pair for num batches not multiple of number of total reads (last batch will have fewer lines)
    if line_pairs[-1][1] > total_lines:
        l0, _ = line_pairs[-1]
        line_pairs[-1] = (l0, total_lines + 1)

    # Get byte ranges from line pairs using GZip index
    byte_ranges = _get_ranges_from_line_pairs(cloud_object, line_pairs)
    chunks = [
        GZipTextSlice(line_0, line_1, range_0, range_1)
        for (line_0, line_1), (range_0, range_1) in zip(line_pairs, byte_ranges)
    ]

    return chunks
