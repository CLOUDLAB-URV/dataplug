import laspy

from dataplug import CloudObject
from dataplug.types.geospatial.copc import CloudOptimizedPointCloud, square_split_strategy

# setup_logging(logging.INFO)

# logging.basicConfig(format="%(asctime)s.%(msecs)03d[%(levelname)-8s] <%(created).6f> %(message)s",
#                     datefmt="%Y-%m-%d %H:%M:%S",
#                     stream=sys.stdout)
# logging.getLogger().setLevel(logging.DEBUG)
# botocore_log = logging.getLogger("botocore")
# botocore_log.setLevel(logging.CRITICAL)
# # requests_log.propagate = True
#
# requests_log = logging.getLogger("urllib3")
# requests_log.setLevel(logging.DEBUG)
# requests_log.propagate = True

if __name__ == "__main__":
    # Localhost minio config
    local_minio = {
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
        "region_name": "us-east-1",
        "endpoint_url": "http://192.168.1.110:9000",
        "botocore_config_kwargs": {"signature_version": "s3v4"},
        "role_arn": "arn:aws:iam::123456789012:role/S3Access",
        "use_token": True,
    }

    co = CloudObject.from_s3(
        CloudOptimizedPointCloud,
        "s3://geospatial/copc/CA_YosemiteNP_2019/USGS_LPC_CA_YosemiteNP_2019_D19_11SKB6892.laz",
        s3_config=local_minio,
    )
    # backend = DummyPreprocessor()
    # co.preprocessing(backend, force=True)

    # print(co.get_attribute('points'))
    # print(co.attributes.points)
    # print(co['points'])

    slices = co.partition(square_split_strategy, num_chunks=9)

    for i, data_slice in enumerate(slices):
        las_data = data_slice.get()
        lidar_file = laspy.open(las_data)
        print(f"LiDAR slice #{i}:")
        print(
            f"Bounds: "
            f"({round(lidar_file.header.mins[0], 3)},{round(lidar_file.header.mins[1], 3)}) - "
            f"({round(lidar_file.header.maxs[0], 3)},{round(lidar_file.header.maxs[1], 3)})"
        )
        print(f"Point count: {lidar_file.header.point_count}")
        print("---")
