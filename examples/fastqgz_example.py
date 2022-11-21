import logging

from dataplug import CloudObject
from dataplug.genomics.fastq import FASTQGZip, partition_reads_batches
from dataplug.preprocess import LocalPreprocessor
from dataplug.util import setup_logging


def main():
    setup_logging(logging.DEBUG)

    # Localhost minio config
    local_minio = {
        'aws_access_key_id': 'minioadmin',
        'aws_secret_access_key': 'minioadmin',
        'region_name': 'us-east-1',
        'endpoint_url': 'http://192.168.1.110:9000',
        'botocore_config_kwargs': {'signature_version': 's3v4'},
        'role_arn': 'arn:aws:iam::123456789012:role/S3Access'
    }

    co = CloudObject.from_s3(FASTQGZip,
                             's3://genomics/SRR21394969.fastq.gz',
                             s3_config=local_minio)

    if not co.is_preprocessed():
        backend = LocalPreprocessor()
        co.preprocess(backend)

    # backend = LocalPreprocessor()
    # co.preprocess(backend)

    data_slices = co.partition(partition_reads_batches, num_batches=100)

    # for data_slice in data_slices:
    #     batch = data_slice.get()
    #     print(batch)

    batch = data_slices[3].get()
    # batch = data_slices[1].get()

    for data_slice in data_slices:
        batch = data_slice.get()
        for line in batch[:4]:
            print(line)
        print('...')
        for line in batch[-4:]:
            print(line)
        print('---')



if __name__ == '__main__':
    main()
