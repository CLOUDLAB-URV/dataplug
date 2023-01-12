import logging
import time

import botocore.config
from collections import defaultdict

from dataplug import CloudObject
from dataplug.basic.csvcloudobject import CSVCloudObject, batches_partition_strategy, partition_size_strategy
from dataplug.preprocess import DummyPreprocessor

import unittest
import os
import filecmp

# logging.basicConfig(level=logging.DEBUG)
# logging.getLogger("botocore").setLevel(logging.WARNING)
# logging.getLogger('dataplug').setLevel(logging.DEBUG)


# class TestCSVPartition(unittest.TestCase):
#     config = {
#         'aws_access_key_id': 'minioadmin',
#         'aws_secret_access_key': 'minioadmin',
#         'region_name': 'us-east-1',
#         'endpoint_url': 'http://localhost:9000',
#         'botocore_config_kwargs': {'signature_version': 's3v4'},
#         'role_arn': 'arn:aws:iam::123456789012:role/S3Access'
#     }
#     co = CloudObject.from_s3(CSV, 's3://testdata/test.csv', s3_config=config)
#
#     def count_lines(self,file_name):
#         with open(file_name) as f:
#             line_count = 0
#             for line in f:
#                 line_count += 1
#         f.close()
#         return line_count
#
#     def test_partition1(self):
#         data_slices = self.co.partition(whole_line_csv_strategy, num_chunks=500, threshold=300)
#         self.assertEqual(len(data_slices), 500)
#
#     def test_partition2(self):
#         data_slices = self.co.partition(whole_line_csv_strategy, num_chunks=1000, threshold=300)
#         self.assertEqual(len(data_slices), 1000)
#
#     def test_samelines(self):
#         data_slices = self.co.partition(whole_line_csv_strategy, num_chunks=2000, threshold=200)
#         f1 = "../sample_data/partitioned.csv"
#         f2 = "../sample_data/test.csv"
#         f = open(f1, "w")
#         for i in data_slices:
#             f.write(i.get())
#         f.close()
#         self.assertEqual(self.count_lines(f1), self.count_lines(f2))
#         os.remove(f1)

if __name__ == '__main__':
    config = {
        'aws_access_key_id': 'minioadmin',
        'aws_secret_access_key': 'minioadmin',
        'region_name': 'us-east-1',
        'endpoint_url': 'http://192.168.1.110:9000',
        'botocore_config_kwargs': {'signature_version': 's3v4'},
        'role_arn': 'arn:aws:iam::123456789012:role/S3Access'
    }
    # Create Cloud Object reference
    co = CloudObject.from_s3(CSVCloudObject, 's3://testdata/cities.csv', s3_config=config)

    # backend = LithopsPreprocessor()
    # co.preprocess(backend)

    # co.fetch()
    # print(co.attributes.columns)

    data_slices = co.partition(batches_partition_strategy, num_batches=10)

    for data_slice in data_slices:
        x = data_slice.as_pandas_dataframe()
        print(x)
