import logging

from dataplug import CloudObject
from dataplug.genomics import FASTQGZip

logging.basicConfig(level=logging.DEBUG)


def main():
    config = {
        'aws_access_key_id': 'minioadmin',
        'aws_secret_access_key': 'minioadmin',
        'region_name': 'us-east-1',
        'endpoint_url': 'http://192.168.1.110:9000',
        's3_config_kwargs': {
            'signature_version': 's3v4'
        }
    }

    co = CloudObject.from_s3(FASTQGZip, 's3://genomics/SRR21394969.fastq.gz', s3_config=config)
    co.fetch()

    is_staged = co.is_preprocessed()
    print(is_staged)
    # if not is_staged:
    #     co.force_preprocess()
    co.preprocess(local=True)

    total_lines = co.get_attribute('total_lines')
    print(total_lines)

    iter_data = co.partition(FASTQGZip.partition_chunk_lines, 100_000, strategy='merge')
    it.setup()


if __name__ == '__main__':
    main()
