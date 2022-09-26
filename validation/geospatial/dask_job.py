import time

from dask.distributed import Client, progress
from pprint import pprint
import dask.bytes
import dask.bag

from lpc_workflow import partition_las, create_dem, merge_dem_partitions


def partition_las_dask_wrapper(args):
    chunk, path = args
    data = chunk[0].compute()
    partitions = partition_las(file_path=path, lidar_data=data)
    return partitions


def create_dem_dask_wrapper(args):
    file_path, partition, las_data = args
    models = create_dem(file_path, partition, las_data)
    return models


def merge_partitions_dask_wrapper(args):
    key, partitions = args
    result = merge_dem_partitions(key, partitions)
    return result[1]


def run_naive_workflow():
    storage_options = {
        'key': 'minioadmin',
        'secret': 'minioadmin',
        'client_kwargs': {
            'region_name': 'us-east-1',
            'endpoint_url': 'http://192.168.1.110:9000'
        }
    }

    # client = Client(threads_per_worker=1, n_workers=4, processes=True)
    # client.start()

    _, blocks, paths = dask.bytes.read_bytes('s3://geospatial/*.las', include_path=True, **storage_options)

    bag = dask.bag.from_sequence(zip(blocks, paths))

    result = (bag
              .map(partition_las_dask_wrapper)
              .flatten()
              .map(create_dem_dask_wrapper)
              .groupby(lambda args: args[0])
              .map(merge_partitions_dask_wrapper)
              .to_textfiles('s3://geospatial-result/*.gtiff', storage_options=storage_options, encoding=None))


if __name__ == '__main__':
    run_naive_workflow()