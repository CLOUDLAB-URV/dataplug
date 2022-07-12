import io
import os.path
import re
import logging
import functools
import shutil

import aiofiles as aiofiles
import s3fs
import asyncio

from lithops.storage import Storage as LithopsStorage
from lithops.storage.utils import StorageNoSuchKeyError


def partitioner_strategy(func):
    @functools.wraps(func)
    def with_logging(*args, **kwargs):
        print(func.__name__ + " was called")
        return func(*args, **kwargs)

    return with_logging


key_regex = re.compile(r'^\w+://.+/.+$')

logger = logging.getLogger(__name__)


class CloudObjectBase:
    def __init__(self, cloud_object):
        self.cloud_object = cloud_object


class CloudObject:
    def __init__(self, cls, path, s3_config=None):
        if not key_regex.match(path):
            raise Exception(f'CloudObject path must satisfy regex {key_regex.pattern}')
        self._meta = None
        self._path = path
        self._cls = cls

        if s3_config is None:
            s3_config = {}

        self.s3 = s3fs.S3FileSystem(
            key=s3_config.get('aws_access_key_id'),
            secret=s3_config.get('aws_secret_access_key'),
            client_kwargs=s3_config.get('s3_client_kwargs', None),
            config_kwargs=s3_config.get('s3_config_kwargs', None),
        )

        self._bucket, self._key, self._version = self.s3.split_path(path)
        self._full_path = os.path.join(self._bucket, self._key)
        self._full_meta_path = self._full_path + '.meta'

        logger.debug(f'{self._bucket=},{self._key=},{self._version=}')

        self._child = cls(self)

    def exists(self):
        if not self._meta:
            self.fetch()
        return bool(self._meta)

    def is_staged(self):
        return self.s3.exists(self._full_meta_path)

    def fetch(self):
        if not self._meta:
            logger.debug('fetching object')
            self._meta = self.s3.info(self._full_path)
            logger.debug(self._meta)
        return self._meta

    def new_from_file(self, file):
        if self.exists():
            raise Exception('Object already exists')

        if isinstance(file, str):
            stream = open(file, 'rb')
        else:
            stream = file

        with self.s3.open(self._full_path, 'wb') as f:
            shutil.copyfileobj(stream, f)
