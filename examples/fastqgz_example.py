import logging

from dataplug import CloudObject
from dataplug.formats.genomics.fastq import FASTQGZip, partition_reads_batches
from dataplug.util import setup_logging


def main():
    setup_logging(logging.DEBUG)

    # Localhost minio config
    local_minio = {
        "endpoint_url": "http://127.0.0.1:9000",
        "role_arn": "arn:aws:iam::123456789012:role/S3Access",
    }

    co = CloudObject.from_s3(FASTQGZip, "s3://genomics/HI.4019.002.index_7.ANN0831_R1.fastq.gz", s3_config=local_minio)
    co.preprocess()

    data_slices = co.partition(partition_reads_batches, num_batches=10)

    lines = 0
    for data_slice in data_slices:
        batch = data_slice.get()
        lines += len(batch)

    print(lines)

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
