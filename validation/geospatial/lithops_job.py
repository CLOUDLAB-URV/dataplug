import concurrent.futures

import boto3
import lithops
import itertools

from lpc_workflow import convert_to_copc, partition_las, create_dem, merge_dem_partitions, partition_copc, SQUARE_SPLIT

BUCKET = 'point-cloud-datasets'

LITHOPS_LOCAL_CFG = {
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


def partition_las_lithops_wrapper(key, storage):
    body = storage.get_object(bucket=BUCKET, key=key)
    partitions = partition_las(key, body)

    def _upload_partition(partition):
        k, p, b = partition
        co = storage.put_cloudobject(b)
        return k, p, co

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


def create_dem_copc_lithops_wrapper(key, partition_num, storage):
    obj_url = storage.get_client().generate_presigned_url('get_object',
                                                          Params={'Bucket': BUCKET,
                                                                  'Key': key},
                                                          ExpiresIn=300)

    las_partition = partition_copc(obj_url, partition_num)
    file_path, partition, dem = create_dem(key, partition_num, las_partition)

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

    new_key = key.replace('laz/', 'dem/')
    new_key = key.replace('.laz', '.gtiff')
    # storage.put_object(bucket=BUCKET, key=new_key, body=result[1])

    return new_key


def convert_to_copc_lithops_wrapper(key, storage):
    new_key = key.replace('laz/', 'copc/')

    exists = False
    try:
        storage.head_object(bucket=BUCKET, key=new_key)
        exists = True
    except lithops.storage.utils.StorageNoSuchKeyError:
        pass

    if exists:
        return new_key

    obj = storage.get_object(bucket=BUCKET, key=key)

    res = convert_to_copc(obj)
    storage.put_object(bucket=BUCKET, key=new_key, body=res)

    return new_key


def run_naive_workflow():
    fexec = lithops.FunctionExecutor(log_level='DEBUG')
    storage = lithops.storage.Storage()

    keys = storage.list_keys(bucket=BUCKET, prefix='laz/CA_YosemiteNP_2019/')
    keys = keys[:3]
    print(keys)

    try:
        fut1 = fexec.map(partition_las_lithops_wrapper, keys)
        partitions = fexec.get_result(fs=fut1)

        partitions_flat = list(itertools.chain.from_iterable(partitions))

        fut2 = fexec.map(create_dem_lithops_wrapper, partitions_flat)
        dems = fexec.get_result(fs=fut2)

        grouped_dems = []
        for key, group in itertools.groupby(dems, lambda part: part[0]):
            grouped_dems.append(list(group))

        fut3 = fexec.map(merge_dem_partitions_lithops_wrapper, grouped_dems)
        fexec.wait(fs=fut3)
    finally:
        fexec.clean(clean_cloudobjects=True)


def run_cloudnative_workflow():
    fexec = lithops.FunctionExecutor(log_level='DEBUG')
    storage = lithops.storage.Storage()

    keys = storage.list_keys(bucket=BUCKET, prefix='copc/CA_YosemiteNP_2019/')
    # keys = keys[:1]
    # print(keys)

    part_keys = [(key, part) for key in keys for part in range(SQUARE_SPLIT * 2)]
    # print(part_keys)

    try:
        fut1 = fexec.map(create_dem_copc_lithops_wrapper, part_keys)
        dems = fexec.get_result(fs=fut1)

        grouped_dems = []
        for key, group in itertools.groupby(dems, lambda part: part[0]):
            grouped_dems.append(list(group))

        fut2 = fexec.map(merge_dem_partitions_lithops_wrapper, grouped_dems)
        fexec.wait(fs=fut2)
    finally:
        fexec.clean(clean_cloudobjects=True)


def preprocess_dataset():
    fexec = lithops.FunctionExecutor(log_level='DEBUG')

    storage = lithops.storage.Storage()

    keys = storage.list_keys(bucket='point-cloud-datasets', prefix='laz/CA_YosemiteNP_2019/')

    fut = fexec.map(convert_to_copc_lithops_wrapper, keys)
    res = fexec.get_result(fs=fut)

    print(res)


if __name__ == '__main__':
    preprocess_dataset()
    # run_naive_workflow()
    # run_cloudnative_workflow()
