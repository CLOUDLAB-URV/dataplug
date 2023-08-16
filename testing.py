import os

from dataplug import CloudObject
from dataplug.formats import csv

if __name__ == "__main__":
    # Get full path of the example CSV file
    pwd = os.getcwd()
    bucket = os.path.join(pwd, "examples", "sample_data", "cities.csv")
    key = os.path.join(bucket, "cities.csv")

    CloudObject.from_bucket_key(csv.CSV, storage="file", bucket=bucket, key=key)
