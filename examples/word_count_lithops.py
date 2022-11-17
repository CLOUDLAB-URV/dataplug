import logging
import time
from collections import defaultdict

import lithops

from dataplug import CloudObject
from dataplug.basic import UTF8Text, whole_words_strategy
from dataplug.util import setup_logging


def word_count(data_slice):
    t0 = time.perf_counter()
    text = data_slice.get()
    t1 = time.perf_counter()
    print(f'Slice get: {t1 - t0:.3f} s')

    words = defaultdict(lambda: 0)
    for word in text.split():
        word = word.strip().lower().replace('.', '').replace(',', '')
        words[word] += 1
    return dict(words)


if __name__ == '__main__':
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

    co = CloudObject.from_s3(UTF8Text, 's3://testdata/lorem_ipsum.txt', s3_config=local_minio)

    data_slices = co.partition(whole_words_strategy, num_chunks=8)

    fexec = lithops.FunctionExecutor()
    fexec.map(word_count, data_slices)
    result = fexec.get_result()

    merged = defaultdict(lambda: 0)
    for map_result in result:
        for word, count in map_result.items():
            merged[word] += count

    top = sorted(list(merged.items()), key=lambda tup: tup[1], reverse=True)

    print(top[:10])
