import logging
import time

import botocore.config
import lithops
import ray
from collections import defaultdict

from dataplug import CloudObject
from dataplug.basic import UTF8Text, whole_words_strategy


# logging.basicConfig(level=logging.DEBUG)
# logging.getLogger("botocore").setLevel(logging.WARNING)
# logging.getLogger('dataplug').setLevel(logging.DEBUG)

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


def lithops_backend(data_slice):
    fexec = lithops.FunctionExecutor()
    fexec.map(word_count, data_slices)
    result = fexec.get_result()
    return result


def ray_backend(data_slice):
    ray.init()
    ray_task = ray.remote(word_count)
    tasks = []
    for data_slice in data_slices:
        tasks.append(ray_task.remote(data_slice))
    return ray.get(tasks)


if __name__ == '__main__':
    config = {
        'aws_access_key_id': 'minioadmin',
        'aws_secret_access_key': 'minioadmin',
        'region_name': 'us-east-1',
        'endpoint_url': 'http://192.168.1.110:9000',
        'config': botocore.config.Config(signature_version='s3v4')
    }

    co = CloudObject.from_s3(UTF8Text, 's3://testdata/lorem_ipsum.txt', s3_config=config)

    data_slices = co.partition(whole_words_strategy, num_chunks=8)

    # result = lithops_backend(data_slices)
    result = ray_backend(data_slices)

    merged = defaultdict(lambda: 0)
    for map_result in result:
        for word, count in map_result.items():
            merged[word] += count

    top = sorted(list(merged.items()), key=lambda tup: tup[1], reverse=True)

    print(top[:10])
