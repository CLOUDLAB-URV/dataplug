import json
import re
from typing import BinaryIO, ByteString, List, Tuple, Dict

import diskcache
import tempfile

from ..cloudobject import CloudDataType, CloudObject

from dataplug.preprocess.preprocessor import MapReducePreprocessor


class FASTAPreprocesser(MapReducePreprocessor):
    def __init__(self):
        super().__init__()

    @staticmethod
    def __get_length(min_range, content, data, start_base, end_base):
        start_base -= min_range
        end_base -= min_range
        len_base = len(data[start_base:end_base].replace('\n', ''))
        # name_id num_chunks_has_divided offset_head offset_bases ->
        # name_id num_chunks_has_divided offset_head offset_bases len_bases
        content[-1] = f'{content[-1]} {len_base}'

    def map(self, data_stream: BinaryIO, cloud_object: CloudObject,
            mapper: int, chunk_size: int, n_mappers: int) -> ByteString:
        min_range = mapper * chunk_size
        max_range = int(cloud_object.size) \
            if mapper == n_mappers - 1 \
            else (mapper + 1) * chunk_size
        # data = self.storage.get_object(bucket=self.bucket, key=key,
        #                                extra_get_args={'Range': f'bytes={min_range}-{max_range - 1}'}).decode('utf-8')
        data = data_stream.read().decode('utf-8')

        sequences = []
        # If it were '>' it would also find the ones inside the head information
        ini_heads = list(re.finditer(r"\n>", data))
        heads = list(re.finditer(r">.+\n", data))

        # If the list is not empty or there is > in the first byte (if is empty it will return an empty list)
        if ini_heads or data[0] == '>':
            first_sequence = True
            prev = -1
            for m in heads:
                start = min_range + m.start()
                end = min_range + m.end()
                if first_sequence:
                    first_sequence = False
                    if mapper > 0 and start - 1 > min_range:
                        # If it is not the worker of the first part of the file and in addition it
                        # turns out that the partition begins in the middle of the base of a sequence.
                        # (start-1): avoid having a split sequence in the index that only has '\n'
                        match_text = list(re.finditer('.*\n', data[0:m.start()]))
                        if match_text:
                            text = match_text[0].group().split(' ')[0]
                            length_0 = len(data[match_text[0].start():m.start()].replace('\n', ''))
                            offset_0 = match_text[0].start() + min_range
                            if len(match_text) > 1:
                                offset_1 = match_text[1].start() + min_range
                                length_1 = len(data[match_text[1].start():m.start()].replace('\n', ''))
                                length_base = f"{length_0}-{length_1}"
                                offset = f"{offset_0}-{offset_1}"
                            else:
                                length_base = f"{length_0}"
                                offset = f'{offset_0}'
                            # >> num_chunks_has_divided offset_head offset_bases_split length/s
                            # first_line_before_space_or_\n
                            sequences.append(f">> <X> <Y> {str(offset)} {length_base} ^{text}^")  # Split sequences
                        else:
                            # When the first header found is false, when in a split stream there is a split header
                            # that has a '>' inside (ex: >tr|...o-alpha-(1->5)-L-e...\n)
                            first_sequence = True
                            start = end = -1  # Avoid entering the following condition
                if prev != start:
                    # When if the current sequence base is not empty
                    if prev != -1:
                        FASTAPreprocesser.__get_length(min_range, sequences, data, prev, start)
                    # name_id num_chunks_has_divided offset_head offset_bases
                    sequences.append(f"{m.group().split(' ')[0].replace('>', '')} 1 {str(start)} {str(end)}")
                prev = end

            len_head = len(heads)
            if ini_heads[-1].start() + 1 > heads[-1].start():
                # Check if the last head of the current one is cut. (ini_heads[-1].start() + 1): ignore '\n'
                last_seq_start = ini_heads[-1].start() + min_range + 1  # (... + 1): ignore '\n'
                if len_head != 0:
                    # Add length of bases to last sequence
                    FASTAPreprocesser.__get_length(min_range, sequences, data, prev, last_seq_start)
                text = data[last_seq_start - min_range::]
                # [<->|<_>]name_id_split offset_head
                sequences.append(f"{'<-' if ' ' in text else '<_'}{text.split(' ')[0]} {str(last_seq_start)}")
                # if '<->' there is all id
            elif len_head != 0:
                # Add length of bases to last sequence
                FASTAPreprocesser.__get_length(min_range, sequences, data, prev, max_range)

        return json.dumps(sequences).encode('utf-8')

    def reduce(self, results: List[BinaryIO], cloud_object: CloudObject,
               n_mappers: int) -> Tuple[ByteString, Dict[str, str]]:
        if len(results) == 1:
            sequences = json.loads(results.pop().read().decode('utf-8'))
            output = "\n".join(sequences)
            return output.encode('utf-8'), {}

        cache = diskcache.Cache(tempfile.mktemp())
        for i, result in enumerate(results):
            sequences = json.loads(result.read())
            # dict_prev = results[i - 1]
            # seq_range_prev = dict_prev['sequences']
            if i > 0 and '>>' in sequences[0]:
                param_seq = sequences[0].split(' ')
                prev_sequences = cache[i - 1]
                last_prev_seq = prev_sequences[-1]
                param_seq_prev = last_prev_seq.split(' ')
                if '<->' in last_prev_seq or '<_>' in last_prev_seq:
                    if '<->' in last_prev_seq[-1]:  # If the split was after a space, then there is all id
                        name_id = param_seq_prev[0].replace('<->', '')
                    else:
                        name_id = param_seq_prev[0].replace('<_>', '') + param_seq[5].replace('^', '')
                    length = param_seq[4].split('-')[1]
                    offset_head = param_seq_prev[1]
                    offset_base = param_seq[3].split('-')[1]
                    prev_sequences.pop()  # Remove previous sequence
                    cache[i - 1] = prev_sequences
                    split = 0
                else:
                    length = param_seq[4].split('-')[0]
                    name_id = param_seq_prev[0]
                    offset_head = param_seq_prev[2]
                    offset_base = param_seq[3].split('-')[0]
                    split = int(param_seq_prev[1])
                    # Update number of partitions of the sequence
                    for x in range(i - split, i):  # Update previous sequences
                        # num_chunks_has_divided + 1 (i+1: total of current partitions of sequence)
                        s = cache[x]
                        s[-1] = s[-1].replace(f' {split} ', f' {split + 1} ')
                        cache[x] = s
                sequences[0] = sequences[0].replace(f' {param_seq[5]}', '')  # Remove 4rt param
                sequences[0] = sequences[0].replace(f' {param_seq[3]} ',
                                                    f' {offset_base} ')  # [offset_base_0-offset_base_1|offset_base] -> offset_base
                sequences[0] = sequences[0].replace(f' {param_seq[4]}',
                                                    f' {length}')  # [length_0-length_1|length] -> length
                sequences[0] = sequences[0].replace(' <X> ', f' {str(split + 1)} ')  # X --> num_chunks_has_divided
                sequences[0] = sequences[0].replace(' <Y> ', f' {offset_head} ')  # Y --> offset_head
                sequences[0] = sequences[0].replace('>> ', f'{name_id} ')  # '>>' -> name_id
            cache[i] = sequences

        output = ""
        for i in range(len(results)):
            output += "\n".join(cache[i])
            output += "\n"

        return output.encode('utf-8'), {}


@CloudDataType(preprocessor=FASTAPreprocesser)
class FASTA:
    def __init__(self, cloud_object):
        self.cloud_object = cloud_object
