import logging

import botocore

from dataplug import CloudObject
from dataplug.geospatial.laspc import LiDARPointCloud
from dataplug.preprocess import LithopsPreprocessor, DummyPreprocessor
from dataplug.util import setup_logging


def main():
    setup_logging(logging.DEBUG)

    # Localhost minio config
    local_minio = {
        'aws_access_key_id': 'minioadmin',
        'aws_secret_access_key': 'minioadmin',
        'region_name': 'us-east-1',
        'endpoint_url': 'http://192.168.1.110:9000',
        'botocore_config_kwargs': {'signature_version': 's3v4'},
        'role_arn': 'arn:aws:iam::123456789012:role/S3Access'
    }

    co = CloudObject.from_s3(LiDARPointCloud,
                             's3://geospatial/laz/USGS_LPC_CA_YosemiteNP_2019_D19_11SKB6892.laz',
                             s3_config=local_minio)

    # backend = LithopsPreprocessor()
    backend = DummyPreprocessor()
    co.preprocess(backend, force=True)

    # preprocessed = co.is_preprocessed()
    # print(preprocessed)
    # if not preprocessed:
    #     backend = LithopsPreprocessor()
        # backend = DummyPreprocessor()
        # co.preprocess(backend)

    print(f"No. of points: {co['point_count']}")


if __name__ == '__main__':
    main()
