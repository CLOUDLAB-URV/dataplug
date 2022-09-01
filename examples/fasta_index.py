import logging

from cloudnative_datasets import CloudObject
from cloudnative_datasets.genomics import FASTA

# logging.basicConfig(level=logging.DEBUG)


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

    co = CloudObject.new_from_s3(FASTA, 's3://genomics/fasta_sample.fasta', s3_config=config)
    co.fetch()

    is_staged = co.is_staged()
    print(is_staged)
    # if not is_staged:
    #     co.force_preprocess()<
    co.force_preprocess(local=True, num_workers=4)

    # total_lines = co.get_attribute('total_lines')
    # print(total_lines)
    #
    # it = co.partition(FASTA.partition_chunk_lines, 100_000, strategy='merge')
    # it.setup()


if __name__ == '__main__':
    main()
