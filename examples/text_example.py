import logging

from cloudnative_datasets import CloudObject
from cloudnative_datasets.basic import UTF8Text, whole_words_strategy

# logging.basicConfig(level=logging.DEBUG)
# logging.getLogger("botocore").setLevel(logging.WARNING)
# logging.getLogger('cloudnative_datasets').setLevel(logging.DEBUG)

if __name__ == '__main__':
    config = {
        'aws_access_key_id': 'minioadmin',
        'aws_secret_access_key': 'minioadmin',
        'region_name': 'us-east-1',
        'endpoint_url': 'http://127.0.0.1:9000',
        's3_config_kwargs': {
            'signature_version': 's3v4'
        }
    }

    co = CloudObject.new_from_s3(UTF8Text, 's3://testdata/lorem_ipsum.txt', s3_config=config)

    data_slices = co.partition(whole_words_strategy, num_chunks=100)
    for data_slice in data_slices:
        text = data_slice.get()
        print(text)
        print('---')
