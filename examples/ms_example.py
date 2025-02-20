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
        # "region_name": "us-east-1",  # Optional but recommended
        # "botocore_config_kwargs": {"signature_version": "s3v4"},  # Optional
    }

    # instalar mc para minio
    # setuppear un minio con sts
    # probar si funciona?
    # crear un bucket y subir el ms
    # probar este codigo de nuevo

    ms_uri = "s3://astronomics/partition_1.ms"

    co = CloudObject.from_s3(
        MS,
        ms_uri,
        False,
        s3_config=local_minio,
        folder = True
        #if directory equals true
    )

    parallel_config = {"verbose": 10}
    co.preprocess(parallel_config, force=True)

    # co.preprocessing(backend, force=True)

    # print(co.get_attribute('points'))
    # print(co.attributes.points)
    # print(co['points'])

    #slices = co.partition(ms_partitioning_strategy, num_chunks=4)

    #first_slice = slice[1]
    #slice_data = first_slice.get()
    #print(slice_data)