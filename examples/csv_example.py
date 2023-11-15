from dataplug import CloudObject
from dataplug.formats.generic.csv import CSV, batches_partition_strategy
from dataplug.preprocessing import LocalPreprocessor

from dataplug.util import setup_logging

setup_logging("DEBUG")

if __name__ == "__main__":
    # config = {
    #     'aws_access_key_id': 'minioadmin',
    #     'aws_secret_access_key': 'minioadmin',
    #     'region_name': 'us-east-1',
    #     'endpoint_url': 'http://192.168.1.110:9000',
    # 'endpoint_url': 'http://127.0.0.1:9000',
    # 'botocore_config_kwargs': {'signature_version': 's3v4'},
    # 'role_arn': 'arn:aws:iam::123456789012:role/S3Access'
    # }
    # config = {"endpoint_url": "http://127.0.0.1:9000", "role_arn": "arn:aws:iam::123456789012:role/S3Access"}
    # Create Cloud Object reference
    # co = CloudObject.from_path(CSV, "s3://sample_data/cities.csv", storage_config=config)

    CSV.check()

    co = CloudObject.from_path(CSV, "file://./sample_data/cities.csv")

    backend = LocalPreprocessor()
    co.preprocess(backend, force=True)

    co.fetch()
    print(co.attributes.columns)

    data_slices = co.partition(batches_partition_strategy, num_batches=10)

    # data_slices[-1].as_pandas_dataframe()

    for data_slice in data_slices:
        x = data_slice.as_pandas_dataframe()
        print(x)
