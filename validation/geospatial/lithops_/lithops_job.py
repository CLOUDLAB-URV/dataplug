import concurrent.futures
import json

import time
import lithops
import itertools

from lpc_workflow import convert_to_copc, partition_las, create_dem, merge_dem_partitions, partition_copc, SQUARE_SPLIT

BUCKET = 'point-cloud-datasets'

lithopsLOCAL_CFG = {
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

    try:
        t0 = time.time()
        t0_part = time.time()
        fut1 = fexec.map(partition_las_lithops_wrapper, keys)
        partitions = fexec.get_result(fs=fut1)
        t1_part = time.time()

        print(f'Partitioning wallclock time: {t1_part - t0_part} s')

        partitions_flat = list(itertools.chain.from_iterable(partitions))

        t0_proc = time.time()
        fut2 = fexec.map(create_dem_lithops_wrapper, partitions_flat)
        dems = fexec.get_result(fs=fut2)
        t1_proc = time.time()

        print(f'Processing wallclock time: {t1_proc - t0_proc} s')

        grouped_dems = []
        for key, group in itertools.groupby(dems, lambda part: part[0]):
            grouped_dems.append(list(group))

        t0_merge = time.time()
        fut3 = fexec.map(merge_dem_partitions_lithops_wrapper, grouped_dems)
        fexec.wait(fs=fut3)
        t1_merge = time.time()

        print(f'Merging wallclock time: {t1_merge - t0_merge} s')

        t1 = time.time()

        print(f'Workflow wallclock time: {t1 - t0} s')

        fut1_stats = [f.stats for f in fut1]
        with open('naive_partition_stats.json', 'w') as file:
            file.write(json.dumps(fut1_stats, indent=2))

        fut2_stats = [f.stats for f in fut2]
        with open('naive_process_stats.json', 'w') as file:
            file.write(json.dumps(fut2_stats, indent=2))

        fut3_stats = [f.stats for f in fut3]
        with open('naive_merge_stats.json', 'w') as file:
            file.write(json.dumps(fut3_stats, indent=2))
    finally:
        fexec.clean(clean_cloudobjects=True)


def run_cloudnative_workflow():
    fexec = lithops.FunctionExecutor(log_level='DEBUG')
    storage = lithops.storage.Storage()

    keys = storage.list_keys(bucket=BUCKET, prefix='copc/CA_YosemiteNP_2019/')

    part_keys = [(key, part) for key in keys for part in range(SQUARE_SPLIT * SQUARE_SPLIT)]

    try:
        t0 = time.time()
        t0_proc = time.time()
        fut1 = fexec.map(create_dem_copc_lithops_wrapper, part_keys)
        dems = fexec.get_result(fs=fut1)
        t1_proc = time.time()

        print(f'Processing wallclock time: {t1_proc - t0_proc} s')

        grouped_dems = []
        for key, group in itertools.groupby(dems, lambda part: part[0]):
            grouped_dems.append(list(group))

        t0_merge = time.time()
        fut2 = fexec.map(merge_dem_partitions_lithops_wrapper, grouped_dems)
        fexec.wait(fs=fut2)
        t1_merge = time.time()

        print(f'Merging wallclock time: {t1_merge - t0_merge} s')

        t1 = time.time()

        print(f'Workflow wallclock time: {t1 - t0} s')

        fut1_stats = [f.stats for f in fut1]
        with open('co_process_stats.json', 'w') as file:
            file.write(json.dumps(fut1_stats, indent=2))

        fut2_stats = [f.stats for f in fut2]
        with open('co_merge_stats.json', 'w') as file:
            file.write(json.dumps(fut2_stats, indent=2))
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
