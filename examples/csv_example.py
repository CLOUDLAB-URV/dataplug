from dataplug import CloudObject
from dataplug.formats.generic.csv import CSV, partition_num_chunks

# Localhost minio config
minio = {"endpoint_url": "http://127.0.0.1:9000",
         "role_arn": "arn:aws:iam::123456789012:role/S3Access"}

if __name__ == "__main__":
    CSV.debug()
    # co = CloudObject.new_from_file(CSV, "../examples/sample_data/cities.csv", "s3://dataplug/cities.csv",
    #                                s3_config=minio, override=True)
    #
    parallel_config = {"verbose": 10}
    # co.preprocess(parallel_config=parallel_config, debug=True)

    co = CloudObject.from_s3(CSV, "s3://dataplug/cities.csv",
                             s3_config=minio)

    # co.preprocess(parallel_config=parallel_config, extra_args={"separator": ","}, force=True)
    co.preprocess(parallel_config=parallel_config, force=True)

    data_slices = co.partition(partition_num_chunks, num_chunks=25)
    for ds in data_slices:
        print(ds.get_as_pandas())
        print('---')
