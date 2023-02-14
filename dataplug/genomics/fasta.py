import bz2
import io
import itertools
import pickle
import re
from functools import reduce
from typing import BinaryIO, List

import pandas as pd

from ..cloudobject import CloudDataType, CloudObject

from dataplug.preprocess.preprocessor import MapReducePreprocessor, PreprocessingMetadata


def rename_sequence(sequence, param, name_id, offset_head, offset_base):
    sequence = sequence.replace(f" {param[3]}", "")  # Remove 3rt param
    sequence = sequence.replace(f" {param[2]} ", f" {offset_base} ")  # offset_base -> offset_base
    sequence = sequence.replace(" <Y> ", f" {offset_head} ")  # Y --> offset_head
    sequence = sequence.replace(">> ", f"{name_id} ")  # '>>' -> name_id
    return sequence


class FASTAPreprocessor(MapReducePreprocessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @staticmethod
    def __get_length(min_range, content, data, start_base, end_base):
        start_base -= min_range
        end_base -= min_range
        len_base = len(data[start_base:end_base].replace("\n", ""))
        # name_id num_chunks_has_divided offset_head offset_bases ->
        # name_id num_chunks_has_divided offset_head offset_bases len_bases
        content[-1] = f"{content[-1]} {len_base}"

    def map(
        self,
        cloud_object: CloudObject,
        mapper_id: int,
        map_chunk_size: int,
        num_mappers: int,
    ) -> PreprocessingMetadata:
        range_0 = mapper_id * map_chunk_size
        range_1 = cloud_object.size if mapper_id == num_mappers - 1 else (mapper_id + 1) * map_chunk_size
        get_res = cloud_object.s3.get_object(
            Bucket=cloud_object.path.bucket, Key=cloud_object.path.key, Range=f"bytes={range_0}-{range_1 - 1}"
        )
        assert get_res["ResponseMetadata"]["HTTPStatusCode"] == 206
        data = get_res["Body"].read()

        # print('---')
        # print(data.decode('utf-8'))
        # print('---')

        # we use greedy regex so that match offsets takes into account the \n character
        matches = list(re.finditer(rb">.+(\n)?", data))

        content = []
        for match in matches:
            start = range_0 + match.start()
            end = range_0 + match.end()
            seq_id = match.group().decode("utf-8").split(" ")[0].replace(">", "")
            content.append((seq_id, start, end))

        if matches and b"\n" not in matches[-1].group():
            # last match corresponds to a cut sequence identifier, as newline was not read
            offset = range_0 + match.start()
            # read split sequence id line
            with cloud_object.open("rb") as fasta_file:
                fasta_file.seek(offset)
                seq_id_line = fasta_file.readline()
                # get the current offset after reading line, it will be offset for the start of the sequence
                end = fasta_file.tell()
            seq_id = seq_id_line.decode("utf-8").split(" ")[0].replace(">", "")
            content.pop()  # remove last split sequence id added previously
            content.append((seq_id, offset, end))

        # print(content)
        map_result = pickle.dumps(content)
        return PreprocessingMetadata(metadata=map_result)

    def reduce(
        self, map_results: List[PreprocessingMetadata], cloud_object: CloudObject, n_mappers: int
    ) -> PreprocessingMetadata:
        results = (pickle.loads(meta.metadata) for meta in map_results)
        flat_results = itertools.chain(*results)  # flatten list

        df = pd.DataFrame(data=flat_results, columns=["sequence", "id_offset", "seq_offset"])
        num_sequences = df.shape[0]

        # Export to parquet to an in-memory buffer
        buff = io.BytesIO()
        df.to_parquet(buff)
        buff.seek(0)
        del df

        return PreprocessingMetadata(metadata=buff, attributes={"num_sequences": num_sequences})


@CloudDataType(preprocessor=FASTAPreprocessor)
class FASTA:
    def __init__(self, cloud_object):
        self.cloud_object = cloud_object


def partition_chunks_strategy(cloud_object: CloudObject, num_chunks: int):
    raise NotImplementedError()
