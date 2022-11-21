import logging

import botocore

from dataplug import CloudObject
from dataplug.geospatial.laspc import LiDARPointCloud
from dataplug.preprocess import LithopsPreprocessor, LocalPreprocessor
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
                             's3://geospatial/laz/cnig/PNOA_2016_CAT_324-4570_ORT-CLA-COL.laz',
                             s3_config=local_minio)

    preprocessed = co.is_preprocessed()
    print(preprocessed)
    if not preprocessed:
        # backend = LithopsPreprocessor()
        backend = LocalPreprocessor()
        co.preprocess(backend)


if __name__ == '__main__':
    main()
