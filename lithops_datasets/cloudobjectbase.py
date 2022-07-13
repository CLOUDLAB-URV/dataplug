import os.path
import re
import logging
import shutil

import s3fs

key_regex = re.compile(r'^\w+://.+/.+$')

logger = logging.getLogger(__name__)


class CloudObjectBase:
    def __init__(self, cloud_object):
        self.cloud_object = cloud_object

    def preprocess(self):
        raise NotImplementedError()


class CloudObject:
    def __init__(self, cloud_object_class, s3_path, s3_config=None):
        if not key_regex.match(s3_path):
            raise Exception(f'CloudObject path must satisfy regex {key_regex.pattern}')
        self._meta = None
        self._s3_path = s3_path
        self._cls = cloud_object_class

        if s3_config is None:
            s3_config = {}

        self.s3 = s3fs.S3FileSystem(
            key=s3_config.get('aws_access_key_id'),
            secret=s3_config.get('aws_secret_access_key'),
            client_kwargs=s3_config.get('s3_client_kwargs', None),
            config_kwargs=s3_config.get('s3_config_kwargs', None),
        )

        self._bucket, self._key, self._version = self.s3.split_path(s3_path)
        self._full_path = os.path.join(self._bucket, self._key)
        self._full_meta_path = self._full_path + '.meta'

        logger.debug(f'{self._bucket=},{self._key=},{self._version=}')

        self._child = cloud_object_class(self)

    @property
    def path(self):
        return self._full_path

    @classmethod
    def new_from_s3(cls, cloud_object_class, s3_path, s3_config=None):
        co_instance = cls(cloud_object_class, s3_path, s3_config)
        return co_instance._child

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
