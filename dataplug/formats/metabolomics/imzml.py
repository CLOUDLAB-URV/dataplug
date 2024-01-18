from __future__ import annotations

import io
import logging
import shutil
from typing import TYPE_CHECKING

from pyimzml.ImzMLParser import ImzMLParser

from ...core import *
from ...preprocessing.preprocessor import BatchPreprocessor, PreprocessingMetadata

if TYPE_CHECKING:
    from typing import List, Tuple
    from dataplug.cloudobject import CloudObject

logger = logging.getLogger(__name__)


@CloudDataFormat
class ImzML:
    is_continuous: bool
    coordinates: List[Tuple[int, int, int]]
    mz_precision: str
    int_precision: str
    mz_size: int
    int_size: int
    mz_offsets: List[int]
    int_offsets: List[int]
    mz_lengths: List[int]
    int_lengths: List[int]


@FormatPreprocessor(ImzML)
class ImzMLPreprocessor(BatchPreprocessor):
    """
    ImzML preprocessor parses the .imzML file and extracts the metadata needed for chunking.
    """

    def preprocess(self, cloud_object: CloudObject) -> PreprocessingMetadata:
        obj_res = cloud_object.storage.get_object(
            Bucket=cloud_object.path.bucket, Key=cloud_object.path.key.replace(".ibd", ".imzML")
        )
        assert obj_res.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
        data_stream = obj_res["Body"]

        parser = ImzMLParser(data_stream, ibd_file=None)
        if "IMS:1000030" in parser.metadata.file_description:
            is_continuous = True
        elif "IMS:1000031" in parser.metadata.file_description:
            is_continuous = False
        else:
            raise Exception("ImzML has neither continuous nor processed accession")

        # Perform some sanity checks on the ImzML
        last_int_offset = 0
        for mz_off, int_off in zip(parser.mzOffsets, parser.intensityOffsets):
            # mz should always come before int
            if mz_off > int_off:
                raise Exception("This imzML file is not supported")
            if is_continuous:
                # in continuous mode, all mz offsets should point to the same byte
                if mz_off != parser.mzOffsets[0]:
                    raise Exception("This imzML file is not supported")
            else:
                # mz and int are expected to be stored sequentially
                if mz_off < last_int_offset:
                    raise Exception("This imzML file is not supported")
            last_int_offset = int_off

        attrs = {
            "is_continuous": is_continuous,
            "coordinates": parser.coordinates,
            "mz_precision": parser.mzPrecision,
            "int_precision": parser.intensityPrecision,
            "mz_size": parser.sizeDict[parser.mzPrecision],
            "int_size": parser.sizeDict[parser.intensityPrecision],
            "mz_offsets": parser.mzOffsets,
            "int_offsets": parser.intensityOffsets,
            "mz_lengths": parser.mzLengths,
            "int_lengths": parser.intensityLengths,
        }

        return PreprocessingMetadata(attributes=attrs)


class ImzMLSlice(CloudObjectSlice):
    def __init__(
        self, spectrum_index, mz_offsets, mz_lengths, int_offsets, int_lengths, mz_range_1=None, *args, **kwargs
    ):
        self.spectrum_index = spectrum_index
        self.mz_offsets = mz_offsets
        self.mz_lengths = mz_lengths
        self.int_offsets = int_offsets
        self.int_lengths = int_lengths
        self.mz_range_1 = mz_range_1
        super().__init__(*args, **kwargs)

    def __fetch_range_into_buffer(self, first_byte: int, last_byte: int, buffer: io.BufferedIOBase):
        get_response = self.cloud_object.storage.get_object(
            Bucket=self.cloud_object.path.bucket,
            Key=self.cloud_object.path.key,
            Range=f"bytes={first_byte}-{last_byte}",
        )
        assert get_response["ResponseMetadata"]["HTTPStatusCode"] in (200, 206)
        shutil.copyfileobj(get_response["Body"], buffer)

    def get(self):
        buff = io.BytesIO()

        if self.mz_range_1 is not None:
            # continuous mode, place mz at the beginning of chunk
            # read from the first byte after the UUID
            self.__fetch_range_into_buffer(16, self.mz_range_1, buff)

        self.__fetch_range_into_buffer(self.range_0, self.range_1, buff)
        buff.seek(0)

        return buff.getvalue()


@PartitioningStrategy(ImzML)
def partition_chunks_strategy(cloud_object: CloudObject, chunk_size: int):
    """
    The partitioning strategy splits the data into chunks of at most the given size in bytes.
    Spectra will not be split across chunks. If the chunk size is too small to fit in a spectrum,
    an exception will be raised.
    """
    slices = []

    first_mz_offset = cloud_object.attributes.mz_offsets[0]
    first_mz_length = cloud_object.attributes.mz_lengths[0]
    is_continuous = cloud_object.attributes.is_continuous

    mz_size = cloud_object.attributes.mz_size
    int_size = cloud_object.attributes.int_size

    mz_range_1 = 16 + first_mz_length * mz_size - 1 if is_continuous else None
    initial_chunk_size_bytes = first_mz_length if is_continuous else 0
    current_chunk_size_bytes = initial_chunk_size_bytes
    chunk_range_start = (
        cloud_object.attributes.int_offsets[0] if is_continuous else cloud_object.attributes.mz_offsets[0]
    )
    chunk_range_end = None
    chunk_first_spectrum_idx = 0
    chunk_mz_offsets, chunk_mz_lengths = [], []
    chunk_int_offsets, chunk_int_lengths = [], []
    offsets_lengths = [cloud_object.attributes.int_offsets, cloud_object.attributes.int_lengths]
    if not is_continuous:
        offsets_lengths.append(cloud_object.attributes.mz_offsets)
        offsets_lengths.append(cloud_object.attributes.mz_lengths)

    def make_slice():
        return ImzMLSlice(
            spectrum_index=chunk_first_spectrum_idx,
            # take into account the UUID (16 bytes)
            mz_offsets=[first_mz_offset - 16] if is_continuous else chunk_mz_offsets,
            mz_lengths=[first_mz_length] if is_continuous else chunk_mz_lengths,
            int_offsets=chunk_int_offsets,
            int_lengths=chunk_int_lengths,
            range_0=chunk_range_start,
            range_1=chunk_range_end,
            mz_range_1=mz_range_1,
        )

    for i, (int_off, int_len, *mz_off_len) in enumerate(zip(*offsets_lengths)):
        if is_continuous:
            spectrum_size_bytes = int_len * int_size
        else:
            mz_off, mz_len = mz_off_len
            spectrum_size_bytes = mz_len * mz_size + int_len * int_size

        if current_chunk_size_bytes + spectrum_size_bytes > chunk_size:
            if len(chunk_int_offsets) == 0:
                raise Exception("Refusing to make slice with no spectra, your chunk size might be too small")
            slices.append(make_slice())
            current_chunk_size_bytes = initial_chunk_size_bytes
            chunk_range_start = int_off if is_continuous else mz_off
            chunk_mz_offsets, chunk_mz_lengths = [], []
            chunk_int_offsets, chunk_int_lengths = [], []
            chunk_first_spectrum_idx = i

        chunk_int_lengths.append(int_len)
        if is_continuous:
            chunk_int_offsets.append(int_off - chunk_range_start + first_mz_length * mz_size)
        else:
            chunk_int_offsets.append(int_off - chunk_range_start)
            chunk_mz_offsets.append(mz_off - chunk_range_start)
            chunk_mz_lengths.append(mz_len)

        current_chunk_size_bytes += spectrum_size_bytes
        chunk_range_end = int_off + int_len * int_size - 1

    if len(chunk_int_offsets) != 0:
        slices.append(make_slice())

    return slices
