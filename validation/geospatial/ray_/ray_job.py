import time

import boto3
import ray
import s3fs
from pyarrow import fs

import io
import json
import tempfile

import rasterio
from laspy import CopcReader, Bounds
from rasterio.merge import merge
import rasterio as rio

import numpy as np
import os
import pdal
import laspy
import shutil

MAX_X_SIZE = 550.0
MAX_Y_SIZE = 550.0

SQUARE_SPLIT = 3


def force_delete_path(path):
    if os.path.exists(path):
        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)


def square_split(x_min, y_min, x_max, y_max, square_splits):
    x_size = (x_max - x_min) / square_splits
    y_size = (y_max - y_min) / square_splits

    bounds = []
    for i in range(square_splits):
        for j in range(square_splits):
            x_min_bound = (x_size * i) + x_min
            y_min_bound = (y_size * j) + y_min
            x_max_bound = x_min_bound + x_size
            y_max_bound = y_min_bound + y_size
            bounds.append((x_min_bound, y_min_bound, x_max_bound, y_max_bound))
    return bounds


def recursive_split(x_min, y_min, x_max, y_max, max_x_size, max_y_size):
    x_size = x_max - x_min
    y_size = y_max - y_min

    if x_size > max_x_size:
        left = recursive_split(x_min, y_min, x_min + (x_size // 2), y_max, max_x_size, max_y_size)
        right = recursive_split(x_min + (x_size // 2), y_min, x_max, y_max, max_x_size, max_y_size)
        return left + right
    elif y_size > max_y_size:
        up = recursive_split(x_min, y_min, x_max, y_min + (y_size // 2), max_x_size, max_y_size)
        down = recursive_split(x_min, y_min + (y_size // 2), x_max, y_max, max_x_size, max_y_size)
        return up + down
    else:
        return [(x_min, y_min, x_max, y_max)]


def partition_las(file_path, lidar_data):
    print(f'>>> partition_las - start - {time.time()} - {file_path}')

    with laspy.open(lidar_data) as file:
        sub_bounds = square_split(
            file.header.x_min,
            file.header.y_min,
            file.header.x_max,
            file.header.y_max,
            SQUARE_SPLIT
        )
        # sub_bounds = recursive_split(
        #     file.header.mins[0],
        #     file.header.mins[1],
        #     file.header.maxs[0],
        #     file.header.maxs[1],
        #     MAX_X_SIZE,
        #     MAX_Y_SIZE
        # )

        buffers = [io.BytesIO() for _ in range(len(sub_bounds))]
        writers = [laspy.open(buff, mode='w', header=file.header, closefd=False, do_compress=True) for buff in buffers]

        print(f'Going to partition file {file_path} into {len(sub_bounds)} chunks')

        try:
            count = 0
            for points in file.chunk_iterator(1_000_000):
                # For performance we need to use copy
                # so that the underlying arrays are contiguous
                x, y = points.x.copy(), points.y.copy()

                point_piped = 0

                for i, (x_min, y_min, x_max, y_max) in enumerate(sub_bounds):
                    mask = (x >= x_min) & (x <= x_max) & (y >= y_min) & (y <= y_max)

                    if np.any(mask):
                        sub_points = points[mask]
                        # print('write')
                        writers[i].write_points(sub_points)

                    point_piped += np.sum(mask)
                    if point_piped == len(points):
                        break
                count += len(points)
        finally:
            for writer in writers:
                if writer is not None:
                    writer.close()

        return_value = [(file_path, i, buf.getvalue()) for i, buf in enumerate(buffers)]
        print(f'>>> partition_las - end - {time.time()} - {file_path}')
        return return_value


def partition_copc(file_url, partition_num):
    print(f'>>> partition_copc - start - {time.time()}')

    with CopcReader.open(file_url) as copc_file:
        sub_bounds = square_split(
            copc_file.header.mins[0],
            copc_file.header.mins[1],
            copc_file.header.maxs[0],
            copc_file.header.maxs[1],
            SQUARE_SPLIT
        )

        query_bounds = Bounds(
            mins=np.asarray((sub_bounds[partition_num][0], sub_bounds[partition_num][1])),
            maxs=np.asarray((sub_bounds[partition_num][2], sub_bounds[partition_num][3]))
        )

        points = copc_file.query(query_bounds)
        new_header = laspy.LasHeader(
            version=copc_file.header.version,
            point_format=copc_file.header.point_format,
        )
        new_header.offsets = copc_file.header.offsets
        new_header.scales = copc_file.header.scales

        crs = copc_file.header.parse_crs()
        new_header.add_crs(crs, keep_compatibility=True)

        out_buff = io.BytesIO()
        with laspy.open(out_buff, mode='w', header=new_header, closefd=False) as output:
            output.write_points(points)

        return_value = out_buff.getvalue()

        print(f'>>> partition_copc - end - {time.time()}')
        return return_value


def create_dem(file_path, partition, las_data):
    print(f'>>> create_dem - start - {time.time()} - {file_path} - {partition}')

    tmp_prefix = tempfile.mktemp()
    laz_filename = tmp_prefix + '.laz'
    dem_filename = tmp_prefix + '_dem.gtiff'

    try:
        with open(laz_filename, 'wb') as laz_file:
            laz_file.write(las_data)

        dem_pipeline_json = {
            'pipeline': [
                {
                    'type': 'readers.las',
                    'filename': laz_filename,
                    # 'spatialreference': 'EPSG:25830'
                },
                # {
                #     'type': 'filters.reprojection',
                #     'in_srs': 'EPSG:25830',
                #     'out_srs': 'EPSG:25830'
                # },
                {
                    'type': 'filters.assign',
                    'assignment': 'Classification[:]=0'
                },
                {
                    'type': 'filters.elm'
                },
                {
                    'type': 'filters.outlier',
                    'method': 'radius',
                    'radius': 1.0,
                    'min_k': 4
                },
                {
                    'type': 'filters.smrf',
                    'ignore': 'Classification[7:7]',
                    'slope': 0.2,
                    'window': 16,
                    'threshold': 0.45,
                    'scalar': 1.2
                },
                {
                    'type': 'filters.range',
                    # Classification equals 2 (corresponding to ground in LAS).
                    'limits': 'Classification[2:2]',
                },
                {
                    'type': 'writers.gdal',
                    'gdaldriver': 'GTiff',
                    'nodata': '-9999',
                    'output_type': 'max',
                    'resolution': 1,
                    'filename': dem_filename
                }
            ]
        }

        pipeline = pdal.Pipeline(json.dumps(dem_pipeline_json))
        # pipeline.validate()
        # pipeline.loglevel = 8
        print(f'Executing DEM pipeline for {file_path}...')
        result = pipeline.execute()
        print(f'DEM result wrote {result} bytes')

        with open(dem_filename, 'rb') as dem_file:
            dem = dem_file.read()

        print(f'>>> create_dem - end - {time.time()} - {file_path} - {partition}')
        return file_path, partition, dem
    finally:
        try:
            os.remove(laz_filename)
        except FileNotFoundError:
            pass
        try:
            os.remove(dem_filename)
        except FileNotFoundError:
            pass


def merge_dem_partitions(key, partitions):
    print(f'>>> merge_dem_partitions - start - {time.time()} - {key}')

    file_path = key

    tmp_file_prefix = tempfile.mktemp()

    dem_files = []

    for partition in partitions:
        _, i, dem = partition

        tmp_dem_file = tmp_file_prefix + f'_dem-part{i}.gtiff'
        with open(tmp_dem_file, 'wb') as file:
            file.write(dem)
        dem_files.append(tmp_dem_file)

    print(f'Merging {file_path}...')

    dems = [rasterio.open(f) for f in dem_files]
    mosaic_dem, output_dem = merge(dems)

    dem_output_meta = dems[0].meta.copy()
    dem_output_meta.update(
        {
            'driver': 'GTiff',
            'blockxsize': 256,
            'blockysize': 256,
            'tiled': True,
            'height': mosaic_dem.shape[1],
            'width': mosaic_dem.shape[2],
            'transform': output_dem
        }
    )

    file_dem_merged = tmp_file_prefix + '_dem-merged.gtiff'
    with rio.open(file_dem_merged, 'w', **dem_output_meta) as m:
        m.write(mosaic_dem)

    print(f'Done merging {file_path}')

    with open(file_dem_merged, 'rb') as f:
        dem_merged = f.read()
    try:
        os.remove(file_dem_merged)
    except FileNotFoundError:
        pass

    for f in dem_files:
        try:
            os.remove(f)
        except FileNotFoundError:
            pass

    print(f'>>> merge_dem_partitions - start - {time.time()} - {key}')
    return file_path, dem_merged


def create_dem_copc_ray_wrapper(args):
    path, partition_num = args

    # s3 = boto3.client('s3',
    #                   endpoint_url='http://192.168.1.110:9000',
    #                   aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin',
    #                   aws_session_token=None, config=boto3.session.Config(signature_version='s3v4'), verify=False)
    s3 = boto3.client('s3')

    # print(path)
    bucket, key = path.split('/', 1)

    obj_url = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=300)

    las_partition = partition_copc(obj_url, partition_num)
    file_path, partition, dem = create_dem(key, partition_num, las_partition)
    dem_ref = ray.put(dem)

    return file_path, partition, dem_ref


def partition_las_ray_wrapper(args):
    file_path, lidar_data = args
    result = partition_las(file_path, lidar_data)
    result_ref = [(path, i, ray.put(res)) for path, i, res in result]
    return result_ref


def create_dem_ray_wrapper(args):
    file_path, partition, data_ref = args
    las_data = ray.get(data_ref)
    result_path, result_partition, result_dem = create_dem(file_path, partition, las_data)
    result_ref = ray.put(result_dem)
    return result_path, result_partition, result_ref


def merge_dem_partitions_ray_wrapper(args):
    key = args[0][0]
    partitions = [(key, part, ray.get(part_ref)) for key, part, part_ref in args]
    result = merge_dem_partitions(key, partitions)
    result_ref = ray.put(result)
    return [result_ref]


ray.init(address='ray://3.238.92.177:10001')


# ray.init()


def run_naive_workflow():
    # file_system = fs.S3FileSystem(
    #     access_key='minioadmin',
    #     secret_key='minioadmin',
    #     region='us-east-1',
    #     endpoint_override='http://192.168.1.110:9000'
    # )
    #
    # s3 = s3fs.S3FileSystem(
    #     key='minioadmin',
    #     secret='minioadmin',
    #     client_kwargs={
    #         'endpoint_url': 'http://192.168.1.110:9000'
    #     }
    # )

    file_system = fs.S3FileSystem()
    s3 = s3fs.S3FileSystem()

    paths = s3.glob('s3://point-cloud-datasets/laz/CA_YosemiteNP_2019/*.laz')
    # paths = s3.glob('s3://geospatial/laz/*.laz')
    paths = paths[:len(paths) // 4]
    print(paths)

    print(f'>>> pipeline - start - {time.time()}')

    print('Reading files...')
    ds = ray.data.read_binary_files(paths, filesystem=file_system, include_paths=True)
    print('Done')

    print('Executing pipeline...')
    (ds
     .flat_map(partition_las_ray_wrapper)
     .repartition(num_blocks=len(paths) * (SQUARE_SPLIT * SQUARE_SPLIT))
     .map(create_dem_ray_wrapper)
     .groupby(lambda args: args[0])
     .map_groups(merge_dem_partitions_ray_wrapper)
     )
    print('Done')

    print(f'>>> pipeline - end - {time.time()}')


def run_cloudnative_workflow():
    # file_system = fs.S3FileSystem(
    #     access_key='minioadmin',
    #     secret_key='minioadmin',
    #     region='us-east-1',
    #     endpoint_override='http://192.168.1.110:9000'
    # )
    #
    # s3 = s3fs.S3FileSystem(
    #     key='minioadmin',
    #     secret='minioadmin',
    #     client_kwargs={
    #         'endpoint_url': 'http://192.168.1.110:9000'
    #     }
    # )

    file_system = fs.S3FileSystem()
    s3 = s3fs.S3FileSystem()

    paths = s3.glob('s3://point-cloud-datasets/copc/CA_YosemiteNP_2019/*.laz')
    # paths = s3.glob('s3://geospatial/copc/*.laz')
    # print(paths)
    paths = paths[:len(paths) // 4]
    print(paths)

    print(f'>>> pipeline - start - {time.time()}')

    print('Executing pipeline...')
    part_keys = [(path, part) for path in paths for part in range(SQUARE_SPLIT * SQUARE_SPLIT)]
    ds = ray.data.from_items(part_keys)
    models_ds = (ds
                 .map(create_dem_copc_ray_wrapper)
                 .groupby(lambda args: args[0])
                 .map_groups(merge_dem_partitions_ray_wrapper))
    print('Done')

    print(f'>>> pipeline - end - {time.time()}')


if __name__ == '__main__':
    # run_naive_workflow()
    run_cloudnative_workflow()
