import logging

import botocore

from dataplug import CloudObject
from dataplug.geospatial import LiDARPointCloud
from dataplug.preprocess import LithopsPreprocessor, LocalPreprocessor


logging.basicConfig(level=logging.DEBUG)


def main():
    config = {
        'aws_access_key_id': 'minioadmin',
        'aws_secret_access_key': 'minioadmin',
        'region_name': 'us-east-1',
        'endpoint_url': 'http://192.168.1.110:9000',
        'config': botocore.config.Config(signature_version='s3v4')
    }

    co = CloudObject.from_s3(LiDARPointCloud,
                             's3://geospatial/laz/cnig/PNOA_2016_CAT_324-4570_ORT-CLA-COL.laz',
                             s3_config=config)

    preprocessed = co.is_preprocessed()
    print(preprocessed)
    if not preprocessed:
        # backend = LithopsPreprocessor()
        backend = LocalPreprocessor()
        co.preprocess(backend)


if __name__ == '__main__':
    main()
