import logging

from dataplug import CloudObject
from dataplug.formats.genomics.fasta import FASTA, partition_chunks_strategy
from dataplug.preprocessing import LithopsPreprocessor, DummyPreprocessor
from dataplug.util import setup_logging


def main():
    setup_logging(logging.DEBUG)

    # Localhost minio config
    local_minio = {
        "region_name": "us-east-1",
        'endpoint_url': 'http://127.0.0.1:9000',
        "botocore_config_kwargs": {"signature_version": "s3v4"},
        'use_token': False
    }

    co = CloudObject.from_s3(FASTA, "s3://genomics/fasta_sample.fasta", s3_config=local_minio)

    backend = DummyPreprocessor()
    # backend = LithopsPreprocessor()
    # co.preprocess(backend, force=True, num_mappers=3)

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
