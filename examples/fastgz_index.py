import logging

from cloudnative_datasets import CloudObject
from cloudnative_datasets.genomics import FASTQGZip

logging.basicConfig(level=logging.DEBUG)


def main():
    config = {
        'aws_access_key_id': 'minioadmin',
        'aws_secret_access_key': 'minioadmin',
        's3_client_kwargs': {
            'endpoint_url': 'http://127.0.0.1:9000',
            'region_name': 'us-east-1'
        },
        's3_config_kwargs': {
            'signature_version': 's3v4'
        }
    }

    co = CloudObject.new_from_s3(FASTQGZip, 's3://genomics/1c-12S_S96_L001_R1_001.fastq.gz', s3_config=config)
    meta = co.fetch()
    print(meta)

    is_staged = co.is_staged()
    print(is_staged)
    if not is_staged:
        co.preprocess()
    co.preprocess()

    total_lines = co.get_attribute('total_lines')
    print(total_lines)

    it = co.call(FASTQGZip.get_line_range, 1, 125000)
    it.setup()



if __name__ == '__main__':
    main()
