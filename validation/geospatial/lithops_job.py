import concurrent.futures

import lithops
import itertools

from workflow import partition_las, create_dem, merge_dem_partitions


def partition_las_lithops_wrapper(obj, storage):
    path = obj.key
    body = obj.data_stream.read()
    partitions = partition_las(path, body)

    def _upload_partition(partition):
        key, partition, body = partition
        co = storage.put_cloudobject(body)
        return key, partition, co

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(partitions)) as pool:
        futures = [None] * len(partitions)
        for i, partition in enumerate(partitions):
            future = pool.submit(_upload_partition, partition)
            futures[i] = future
        result = [f.result() for f in futures]

    return result


def create_dem_lithops_wrapper(key, partition, co, storage):
    body = storage.get_cloudobject(co)

    file_path, partition, dem = create_dem(key, partition, body)

    res_co = storage.put_cloudobject(dem)
    return file_path, partition, res_co


def merge_dem_partitions_lithops_wrapper(partitions, storage):
    file_key = partitions[0][0]

    def _get_partition(co):
        obj = storage.get_cloudobject(co)
        return obj

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(partitions)) as pool:
        futures = [None] * len(partitions)
        for i, partition in enumerate(partitions):
            key, j, co = partition
            future = pool.submit(_get_partition, co)
            futures[i] = future
        partitions_data = [(key, j, f.result()) for f in futures]

    result = merge_dem_partitions(file_key, partitions_data)

    return result


if __name__ == '__main__':
    lithops_config = {
        'lithops':
            {
                'backend': 'localhost',
                'storage': 'minio'
            },
        'minio':
            {
                'storage_bucket': 'lithops-meta',
                'endpoint': 'http://192.168.1.110:9000',
                'access_key_id': 'minioadmin',
                'secret_access_key': 'minioadmin'
            }
    }

    fexec = lithops.FunctionExecutor(config=lithops_config, log_level='DEBUG')

    fut1 = fexec.map(partition_las_lithops_wrapper, 'minio://geospatial/')
    partitions = fexec.get_result(fs=fut1)

    partitions_flat = list(itertools.chain.from_iterable(partitions))

    fut2 = fexec.map(create_dem_lithops_wrapper, partitions_flat)
    dems = fexec.get_result(fs=fut2)

    grouped_dems = []
    for key, group in itertools.groupby(dems, lambda part: part[0]):
        grouped_dems.append(list(group))

    fut3 = fexec.map(merge_dem_partitions_lithops_wrapper, grouped_dems)
    fexec.wait(fs=fut3)

    fexec.clean(clean_cloudobjects=True)
