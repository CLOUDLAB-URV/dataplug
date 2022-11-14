import json
import logging
import re
import os
import shutil

import botocore

logger = logging.getLogger(__name__)

S3_PATH_REGEX = re.compile(r'^\w+://.+/.+$')


def setup_logging(level=logging.INFO):
    root_logger = logging.getLogger('dataplug')
    root_logger.setLevel(level)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s")
    ch.setFormatter(formatter)
    root_logger.addHandler(ch)


def split_s3_path(path):
    if not S3_PATH_REGEX.fullmatch(path):
        raise ValueError(f'Path must satisfy regex {S3_PATH_REGEX}')

    bucket, key = path.replace('s3://', '').split('/', 1)
    return bucket, key


def force_delete_path(path):
    if os.path.exists(path):
        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)


def head_object(s3client, bucket, key):
    logger.debug(f'head {bucket=} {key=}')
    metadata = {}
    try:
        head_res = s3client.head_object(Bucket=bucket, Key=key)
        del head_res['ResponseMetadata']
        response = head_res
        if 'Metadata' in head_res:
            metadata.update(head_res['Metadata'])
            del response['Metadata']
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            raise KeyError()
        else:
            raise e
    return response, metadata
