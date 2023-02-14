import bz2
import itertools
import pickle
import re
from functools import reduce
from typing import BinaryIO, List

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
        data = get_res["Body"].read().decode("utf-8")

        content = []
        # If it were '>' it would also find the ones inside the head information
        ini_heads = list(re.finditer(r"\n>", data))
        heads = list(re.finditer(r">.+\n", data))

        if ini_heads or data[0] == ">":  # If the list is not empty or there is > in the first byte
            first_sequence = True
            prev = -1
            for m in heads:
                start = range_0 + m.start()
                end = range_0 + m.end()
                if first_sequence:
                    first_sequence = False
                    if mapper_id > 0 and start - 1 > range_0:
                        # If it is not the worker of the first part of the file and in addition it
                        # turns out that the partition begins in the middle of the base of a sequence.
                        # (start-1): avoid having a split sequence in the index that only has '\n'.
                        match_text = list(re.finditer(".*\n", data[0 : m.start()]))
                        if match_text and len(match_text) > 1:
                            text = match_text[0].group().split(" ")[0].replace("\n", "")
                            offset = match_text[1].start() + range_0
                            # >> offset_head offset_bases_split ^first_line_before_space_or_\n^
                            content.append(f">> <Y> {str(offset)} ^{text}^")  # Split sequences
                        else:
                            # When the first header found is false, when in a split stream there is a split header
                            # that has a '>' inside (ex: >tr|...o-alpha-(1->5)-L-e...\n)
                            first_sequence = True
                if prev != start:  # When if the current sequence base is not empty
                    # name_id offset_head offset_bases
                    id_name = m.group().replace("\n", "").split(" ")[0].replace(">", "")
                    content.append(f"{id_name} {str(start)} {str(end)}")
                prev = end

            # Check if the last head of the current one is cut. (ini_heads[-1].start() + 1): ignore '\n'
            if len(heads) != 0 and len(ini_heads) != 0 and ini_heads[-1].start() + 1 > heads[-1].start():
                last_seq_start = ini_heads[-1].start() + range_0 + 1  # (... + 1): ignore '\n'
                text = data[last_seq_start - range_0 : :]
                # [<->|<_>]name_id_split offset_head
                # if '<->' there is all id
                content.append(f"{'<-' if ' ' in text else '<_'}{text.split(' ')[0]} {str(last_seq_start)}")

        return PreprocessingMetadata(metadata=pickle.dumps(content))

    def reduce(self, map_results: List[BinaryIO], cloud_object: CloudObject, n_mappers: int) -> PreprocessingMetadata:
        if len(map_results) > 1:
            results = list(filter(None, map_results))
            for i, list_seq in enumerate(results):
                if i > 0:
                    list_prev = results[i - 1]
                    # If it is not empty the current and previous dictionary
                    if list_prev and list_seq:
                        param = list_seq[0].split(" ")
                        seq_prev = list_prev[-1]
                        param_seq_prev = seq_prev.split(" ")

                        # If the first sequence is split
                        if ">>" in list_seq[0]:
                            if "<->" in seq_prev or "<_>" in seq_prev:
                                # If the split was after a space, then there is all id
                                if "<->" in seq_prev:
                                    name_id = param_seq_prev[0].replace("<->", "")
                                else:
                                    name_id = param_seq_prev[0].replace("<_>", "") + param[3].replace("^", "")
                                list_seq[0] = rename_sequence(list_seq[0], param, name_id, param_seq_prev[1], param[2])
                            else:
                                list_seq[0] = seq_prev
                            # Remove previous sequence
                            list_prev.pop()

        num_sequences = reduce(lambda x, y: x + y, map(lambda r: len(r), results))
        index_result = bz2.compress(b"\n".join(s.encode("utf-8") for s in itertools.chain(*results)))

        return PreprocessingMetadata(metadata=index_result, attributes={"num_sequences": num_sequences})


@CloudDataType(preprocessor=FASTAPreprocessor)
class FASTA:
    def __init__(self, cloud_object):
        self.cloud_object = cloud_object


def partition_chunks_strategy(cloud_object: CloudObject, num_chunks: int):
    fasta_chunks = []
    fasta_file_sz = cloud_object.size
    fa_chunk_size = int(fasta_file_sz / num_chunks)
    num_sequences = cloud_object["num_sequences"]

    get_res = cloud_object.s3.get_object(Bucket=cloud_object.meta_path.bucket, Key=cloud_object.meta_path.key)
    assert get_res["ResponseMetadata"]["HTTPStatusCode"] == 200
    compressed_faidx = get_res["Body"].read()
    faidx = bz2.decompress(compressed_faidx).decode("utf-8").split("\n")

    i = j = 0
    min_offset = fa_chunk_size * j
    max_offset = fa_chunk_size * (j + 1)
    while max_offset <= fasta_file_sz:
        # Find first full/half sequence of the chunk
        if int(faidx[i].split(" ")[1]) <= min_offset < int(faidx[i].split(" ")[2]):  # In the head
            fa_chunk = {"offset_head": int(faidx[i].split(" ")[1]), "offset_base": int(faidx[i].split(" ")[2])}
        elif i == num_sequences - 1 or min_offset < int(faidx[i + 1].split(" ")[1]):  # In the base
            fa_chunk = {"offset_head": int(faidx[i].split(" ")[1]), "offset_base": min_offset}
        elif i < num_sequences:
            i += 1
            while i + 1 < num_sequences and min_offset > int(faidx[i + 1].split(" ")[1]):
                i += 1
            if min_offset < int(faidx[i].split(" ")[2]):
                fa_chunk = {"offset_head": int(faidx[i].split(" ")[1]), "offset_base": int(faidx[i].split(" ")[2])}
            else:
                fa_chunk = {"offset_head": int(faidx[i].split(" ")[1]), "offset_base": min_offset}
        else:
            raise Exception("ERROR: there was a problem getting the first byte of a fasta chunk.")

        # Find last full/half sequence of the chunk
        if i == num_sequences - 1 or max_offset < int(faidx[i + 1].split(" ")[1]):
            fa_chunk["last_byte"] = max_offset - 1 if fa_chunk_size * (j + 2) <= fasta_file_sz else fasta_file_sz - 1
        else:
            if max_offset < int(faidx[i + 1].split(" ")[2]):  # Split in the middle of head
                fa_chunk["last_byte"] = int(faidx[i + 1].split(" ")[1]) - 1
                i += 1
            elif i < num_sequences:
                i += 1
                while i + 1 < num_sequences and max_offset > int(faidx[i + 1].split(" ")[1]):
                    i += 1
                fa_chunk["last_byte"] = max_offset - 1
            else:
                raise Exception("ERROR: there was a problem getting the last byte of a fasta chunk.")

        fa_chunk["chunk_id"] = j
        fasta_chunks.append(fa_chunk)
        j += 1
        min_offset = fa_chunk_size * j
        max_offset = fa_chunk_size * (j + 1)

    return fasta_chunks
