from dataplug import CloudObject
from dataplug.formats.genomics.vcf import VCF, partition_num_chunks

# Localhost minio config
minio = {"endpoint_url": "http://127.0.0.1:9000",
         "role_arn": "arn:aws:iam::123456789012:role/S3Access"}

if __name__ == "__main__":
    VCF.debug()
    parallel_config = {"verbose": 10}

    co = CloudObject.from_s3(VCF, "s3://dataplug/sample.vcf", s3_config=minio)
    co.preprocess(parallel_config=parallel_config, debug=True, force=True)

    data_slices = co.partition(partition_num_chunks, num_chunks=2)
    for ds in data_slices:
        print(ds.get())
        print('---')
