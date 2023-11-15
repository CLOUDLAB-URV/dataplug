import logging

from dataplug import CloudObject
from dataplug.preprocessing import LocalPreprocessor
from dataplug.formats.genomics.fastq import FASTQGZip, partition_reads_batches
from dataplug.util import setup_logging


def main():
    setup_logging(logging.DEBUG)

    FASTQGZip.check()

    # Localhost minio config
    # local_minio = {
    #     "aws_access_key_id": "minioadmin",
    #     "aws_secret_access_key": "minioadmin",
    #     "region_name": "us-east-1",
    #     "endpoint_url": "http://127.0.0.1:9000",
    #     "botocore_config_kwargs": {"signature_version": "s3v4"},
    #     "role_arn": "arn:aws:iam::123456789012:role/S3Access",
    # }

    # co = CloudObject.from_path(FASTQGZip, "s3://genomics/SRR6052133_1.fastq.gz", storage_config=local_minio)
    co = CloudObject.from_path(FASTQGZip, "file://./sample_data/sunflower.fastq.gz")

    backend = LocalPreprocessor()
    co.preprocess(backend, force=True)

    data_slices = co.partition(partition_reads_batches, num_batches=3000)

    lines = 0
    for data_slice in data_slices:
        batch = data_slice.get()
        lines += len(batch)

    print(lines)

    # batch = data_slices[-2].get()
    # for line in batch[:4]:
    #     print(line)
    # print("...")
    # for line in batch[-4:]:
    #     print(line)
    # print("---")
    #
    # batch = data_slices[-1].get()
    # for line in batch[:4]:
    #     print(line)
    # print("...")
    # for line in batch[-4:]:
    #     print(line)
    # print("---")

    # batch = '\n'.join(data_slices[1].get())
    # print(batch)

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
