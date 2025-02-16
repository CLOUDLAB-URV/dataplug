from dataplug import CloudObject
from dataplug.formats.ms import MS, ms_partitioning_strategy
from dataplug.preprocessing import LocalPreprocessor

if __name__ == "__main__":
    # Localhost minio config
    local_minio = {
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "minioadmin",
        #"region_name": "us-east-1",
        "endpoint_url": "http://localhost:9000",
        "botocore_config_kwargs": {"signature_version": "s3v4"},
        #"role_arn": "arn:aws:iam::123456789012:role/S3Access",
        #"use_token": True,
    }

    ms_uri = "s3://astronomics/partition_1.ms"
    true = True

    co = CloudObject.from_s3(
        MS,
        ms_uri,
        s3_config=local_minio,
    )

    backend = LocalPreprocessor()
    co.preprocess(backend, force=True)

    # co.preprocessing(backend, force=True)

    # print(co.get_attribute('points'))
    # print(co.attributes.points)
    # print(co['points'])

    #slices = co.partition(ms_partitioning_strategy, num_chunks=4)

    #first_slice = slice[1]
    #slice_data = first_slice.get()
    #print(slice_data)