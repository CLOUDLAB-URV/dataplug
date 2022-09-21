import time

import boto3
import botocore.exceptions
import requests
import os
import concurrent.futures

LIST_URL = 'https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/CA_YosemiteNP_2019_D19/CA_YosemiteNP_2019/0_file_download_links.txt'
TAKE = 25
BUCKET = 'point-cloud-datasets'
PREFIX = 'laz/CA_YosemiteNP_2019'
WORKERS = 4

if __name__ == '__main__':
    links = requests.get(LIST_URL).text
    links = links.splitlines()
    links = links[:TAKE]
    print(len(links))


    def download_and_upload(link):
        filename = os.path.basename(link)
        key = os.path.join(PREFIX, filename)
        s3 = boto3.client('s3')

        try:
            s3.head_object(Bucket=BUCKET, Key=key)
            exists = True
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == '404':
                exists = False
            else:
                raise error

        if not exists:
            print(f'Download {filename}')
            response = requests.get(link)
            s3.put_object(Bucket=BUCKET, Key=key, Body=response.content)
            print(f'Put {key} OK')
        else:
            print(f'Key {key} exists')


    with concurrent.futures.ThreadPoolExecutor(max_workers=WORKERS) as pool:
        res = pool.map(download_and_upload, links)
