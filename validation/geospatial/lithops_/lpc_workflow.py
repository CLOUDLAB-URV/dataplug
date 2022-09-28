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


def convert_to_copc(lidar_data):
    input_file_path = tempfile.mktemp()
    output_file_path = tempfile.mktemp()

    try:
        force_delete_path(input_file_path)
        force_delete_path(output_file_path)

        with open(input_file_path, 'wb') as input_file:
            input_file.write(lidar_data)

        pipeline_json = [
            {
                "type": "readers.las",
                "filename": input_file_path
            },
            {
                "type": "writers.copc",
                "filename": output_file_path
            }
        ]

        pipeline = pdal.Pipeline(json.dumps(pipeline_json))
        pipeline.execute()

        with open(output_file_path, 'rb') as result_file:
            result = result_file.read()

        return result

    finally:
        force_delete_path(input_file_path)
        force_delete_path(output_file_path)


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

        try:
            count = 0
            for points in file.chunk_iterator(1_000_000):
                print(f'{count / file.header.point_count * 100}%')

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
            print(f'{count / file.header.point_count * 100}%')
        finally:
            for writer in writers:
                if writer is not None:
                    writer.close()

        return [(file_path, i, buf.getvalue()) for i, buf in enumerate(buffers)]


def partition_copc(file_url, partition_num):
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

        return out_buff.getvalue()


def create_dem(file_path, partition, las_data):
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

    return file_path, dem_merged
