import time 
from dataplug import CloudObject
from dataplug.formats.astronomics.ms import MS, ms_partitioning_strategy

if __name__ == "__main__":
    # Localhost minio config
    local_minio = {
        "credentials": {
            "AccessKeyId": "minioadmin",
            "SecretAccessKey": "minioadmin",
        },
        "endpoint_url": "http://127.0.0.1:9000",  # MinIO server address
        # "region_name": "us-east-1",                               Optional
        # "botocore_config_kwargs": {"signature_version": "s3v4"},  Optional
    }

    ms_uri = "s3://astronomics-test/smallms.ms"

    co = CloudObject.from_s3(
        MS,
        ms_uri,
        False,
        s3_config=local_minio
    )

    parallel_config = {"verbose": 10}
    start_time = time.time()
    co.preprocess(parallel_config, force=True)
    end_time = time.time()

    elapsed_time = end_time - start_time
    print(f"Preprocess stage took {elapsed_time:.2f} seconds.")
    #print(co.attributes)

    slices = co.partition(ms_partitioning_strategy, num_chunks=13)
    for slice in slices:
        print(slice)
        #slice_data = slice.get()
        #print(slice_data)

    # first_slice = slices[0]
    # slice_data = first_slice.get()
    # print(slice_data)
    # second_slice = slices[1]
    # slice_data = second_slice.get()
    # print(slice_data)
    # third_slice = slices[2]
    # slice_data = third_slice.get()
    # print(slice_data)
    # fourth_slice = slices[3]
    # slice_data = fourth_slice.get()
    # print(slice_data)