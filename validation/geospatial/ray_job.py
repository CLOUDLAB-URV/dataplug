from typing import List, Dict, Any

import ray
from ray.data import Datasource
import s3fs
import pyarrow as pa
from pyarrow import fs
from ray.data.block import Block, BlockMetadata
from ray.data.datasource import WriteResult
from ray.types import ObjectRef

from lpc_workflow import partition_las, create_dem, merge_dem_partitions


def partition_las_ray_wrapper(args):
    file_path, lidar_data = args
    result = partition_las(file_path, lidar_data)
    return result


def create_dem_ray_wrapper(args):
    file_path, partition, las_data = args
    result = create_dem(file_path, partition, las_data)
    return result


def merge_dem_partitions_ray_wrapper(args):
    key = args[0][0]
    partitions = args
    result = merge_dem_partitions(key, partitions)
    return [result]


class S3DataStore(Datasource):
    def do_write(self, blocks: List[ObjectRef[Block]], metadata: List[BlockMetadata], ray_remote_args: Dict[str, Any],
                 **write_args) -> List[ObjectRef[WriteResult]]:
        s3 = s3fs.S3FileSystem(
            key='minioadmin',
            secret='minioadmin',
            client_kwargs={
                'endpoint_url': 'http://192.168.1.110:9000'
            }
        )

        for block in blocks:
            data = ray.get(block)
            print(data[0])
            # s3.open()
            # print(block)
        return [ray.put(True)]


ray.init()

if __name__ == '__main__':
    file_system = fs.S3FileSystem(
        access_key='minioadmin',
        secret_key='minioadmin',
        region='us-east-1',
        endpoint_override='http://192.168.1.110:9000'
    )

    s3 = s3fs.S3FileSystem(
        key='minioadmin',
        secret='minioadmin',
        client_kwargs={
            'endpoint_url': 'http://192.168.1.110:9000'
        }
    )

    paths = s3.glob('s3://geospatial/*.las')
    paths = ['s3://' + path for path in paths]
    print(paths)

    print('Reading files...')
    ds = ray.data.read_binary_files(paths, filesystem=file_system, include_paths=True)
    print(ds)

    partitioned_ds = ds.flat_map(partition_las_ray_wrapper)
    print(partitioned_ds)

    repartitioned_ds = partitioned_ds.repartition(num_blocks=4)
    print(repartitioned_ds)

    models_ds = repartitioned_ds.map(create_dem_ray_wrapper)

    grouped_ds = models_ds.groupby(lambda args: args[0]).map_groups(merge_dem_partitions_ray_wrapper)
    # print(grouped_ds)
