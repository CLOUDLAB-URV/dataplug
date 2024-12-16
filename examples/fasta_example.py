import logging
import math

from dataplug import CloudObject
from dataplug.formats.genomics.fasta import FASTA, partition_chunks_strategy
from dataplug.util import setup_logging


def main():
    setup_logging(logging.DEBUG)
    FASTA.debug()

    # Localhost minio config
    minio = {"endpoint_url": "http://127.0.0.1:9000",
             "role_arn": "arn:aws:iam::123456789012:role/S3Access"}

    co = CloudObject.from_s3(FASTA, "s3://genomics/fasta_sample.fasta",
                             s3_config={"endpoint_url": "http://127.0.0.1:9000",
                                        "role_arn": "arn:aws:iam::123456789012:role/S3Access"})

    # Perform preprocessing in 4 parallel jobs (chunk size = total size / 4)
    parallel_config = {"verbose": 10}
    chunk_size = math.ceil(co.size / 4)
    co.preprocess(parallel_config=parallel_config, chunk_size=chunk_size)

    print(f"FASTA file has {co.attributes.num_sequences} sequences")
    data_slices = co.partition(partition_chunks_strategy, num_chunks=8)

    for data_slice in data_slices:
        batch = data_slice.get().decode('utf-8')
        print(batch)
        print('---')

    # batch = data_slices[3].get()
    # batch = data_slices[1].get()

    # for data_slice in data_slices:
    #     batch = data_slice.get()
    #     for line in batch[:4]:
    #         print(line)
    #     print('...')
    #     for line in batch[-4:]:
    #         print(line)
    #     print('---')


if __name__ == "__main__":
    main()
