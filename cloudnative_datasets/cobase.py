import inspect
import os.path
import re
import logging
import shutil

import boto3
import botocore
import s3fs

from typing import Tuple, Dict, BinaryIO

key_regex = re.compile(r'^\w+://.+/.+$')

logger = logging.getLogger(__name__)


class CloudObjectBase:
    def __init__(self, cloud_object):
        self.cloud_object: CloudObject = cloud_object

    def preprocess(self, object_stream: BinaryIO) -> Tuple[bytes, Dict[str, str]]:
        raise NotImplementedError()


class CloudObject:
    def __init__(self, cloud_object_class, s3_path, s3_config=None):
        if not key_regex.match(s3_path):
            raise Exception(f'CloudObject path must satisfy regex {key_regex.pattern}')
        self._obj_meta = None
        self._meta_meta = None
        self._s3_path = s3_path
        self._cls = cloud_object_class

        if s3_config is None:
            s3_config = {}

        self.s3 = s3fs.S3FileSystem(
            key=s3_config.get('aws_access_key_id'),
            secret=s3_config.get('aws_secret_access_key'),
            client_kwargs=s3_config.get('s3_client_kwargs', None),
            config_kwargs=s3_config.get('s3_config_kwargs', None)
        )

        self.s3_client = boto3.client('s3', aws_access_key_id=s3_config.get('aws_access_key_id'),
                                      aws_secret_access_key=s3_config.get('aws_secret_access_key'),
                                      region_name=s3_config.get('s3_client_kwargs', {}).get('region_name'),
                                      endpoint_url=s3_config.get('s3_client_kwargs', {}).get('endpoint_url'),
                                      config=botocore.client.Config(**s3_config.get('s3_config_kwargs', {})))

        self._bucket, self._key, self._version = self.s3.split_path(s3_path)
        self._meta_key = self._key + '.meta'
        self._full_key = os.path.join(self._bucket, self._key)
        self._full_meta_key = self._full_key + '.meta'

        logger.debug(f'{self._bucket=},{self._key=},{self._version=}')

        self._child = cloud_object_class(self)

    @property
    def path(self):
        return self._full_key

    @classmethod
    def new_from_s3(cls, cloud_object_class, s3_path, s3_config=None):
        co_instance = cls(cloud_object_class, s3_path, s3_config)
        return co_instance

    @classmethod
    def new_from_file(cls, cloud_object_class, file, cloud_path=None, s3_config=None):
        if cloud_path is None and isinstance(file, str):
            cloud_path = 's3://' + file
        elif cloud_path is None:
            raise Exception('Cloud path is required')

        co_instance = cls(cloud_object_class, cloud_path, s3_config)

        if co_instance.exists():
            raise Exception('Object already exists')

        if isinstance(file, str):
            stream = open(file, 'rb')
        else:
            stream = file

        with co_instance.s3.open(co_instance._full_key, 'wb') as f:
            shutil.copyfileobj(stream, f)

    def _update_attrs(self):
        print(self._meta_meta)

        self._attributes = {key: value for key, value in self._meta_meta['Metadata'].items()}

    def exists(self):
        if not self._obj_meta:
            self.fetch()
        return bool(self._obj_meta)

    def is_staged(self):
        return self.s3.exists(self._full_meta_key)

    def get_attribute(self, key):
        if not self.is_staged():
            raise Exception('Object is not staged')

        if 'Metadata' not in self._meta_meta:
            self.fetch()
            if 'Metadata' not in self._meta_meta:
                raise KeyError(key)

        if key not in self._meta_meta['Metadata']:
            self.fetch()
            if key not in self._meta_meta['Metadata']:
                raise KeyError(key)

        return self._meta_meta['Metadata'][key]

    def fetch(self):
        if not self._obj_meta:
            logger.debug('fetching object')
            self._obj_meta = self.s3.info(self._full_key)
            res = self.s3_client.head_object(Bucket=self._bucket, Key=self._meta_key)
            del res['ResponseMetadata']
            self._meta_meta = res
            logger.debug(self._obj_meta)
            logger.debug(self._meta_meta)
        return self._obj_meta

    def preprocess(self):
        with self.s3.open(self._full_key) as input_stream:
            body, meta = self._child.preprocess(input_stream)
        self.s3_client.put_object(
            Body=body,
            Bucket=self._bucket,
            Key=self._meta_key,
            Metadata=meta
        )

    def get_meta_obj(self):
        return self.s3.open(self._full_meta_key)

    def call(self, f, *args, **kwargs):
        if isinstance(f, str):
            func_name = f
        elif inspect.ismethod(f) or inspect.isfunction(f):
            func_name = f.__name__
        else:
            raise Exception(f)

        attr = getattr(self._child, func_name)
        return attr.__call__(*args, **kwargs)
