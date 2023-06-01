import io
import logging
import time

import laspy

from dataplug import CloudObject
from dataplug.types.geospatial.laspc import LiDARPointCloud, square_split_strategy
from dataplug.util import setup_logging


def main():
    setup_logging(logging.CRITICAL)

    # Localhost minio config
    local_minio = {
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
        "region_name": "us-east-1",
        "endpoint_url": "http://127.0.0.1:9000",
        "botocore_config_kwargs": {"signature_version": "s3v4"},
        "role_arn": "arn:aws:iam::123456789012:role/S3Access",
    }

    co = CloudObject.from_s3(
        LiDARPointCloud, "s3://dataplug-geospatial/example_files/example.las", s3_config=local_minio
    )

    # backend = LithopsPreprocessor()
    # backend = DummyPreprocessor()
    # co.preprocess(backend, force=True)

    # preprocessed = co.is_preprocessed()
    # print(preprocessed)
    # if not preprocessed:
    #     backend = LithopsPreprocessor()
    # backend = DummyPreprocessor()
    # co.preprocessing(backend)

    print(f"No. of points: {co['point_count']}")
    print(f"Point size: {co.attributes.point_format_size}")

    data_slices = co.partition(square_split_strategy, num_chunks=9)

    # data_slices[0].get()

    total_point_count = 0
    for data_slice in data_slices:
        t0 = time.perf_counter()
        buff = data_slice.get()
        t1 = time.perf_counter()
        print(f"Took {t1 - t0:.2f} seconds to get partition")

        las_chunk = laspy.open(io.BytesIO(buff), "r", closefd=False)
        print("Min XYZ: ", las_chunk.header.mins)
        print("Max XYZ: ", las_chunk.header.maxs)
        print("Chunk point count: ", las_chunk.header.point_count)
        total_point_count += las_chunk.header.point_count

    print("Total point count: ", total_point_count)


if __name__ == "__main__":
    main()
