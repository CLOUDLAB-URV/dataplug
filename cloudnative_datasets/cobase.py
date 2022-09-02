import inspect
import logging
import functools
import math
import types
from typing import Tuple, Dict, BinaryIO

import boto3
import botocore

from .util import split_s3_path
from .preprocessers import AsyncPreprocesser, MapReducePreprocesser

logger = logging.getLogger(__name__)


class CloudObjectWrapper:
    def __init__(self, preprocesser=None, inherit=None):
        print(preprocesser, inherit)
        self.co_class = None
        self.preprocesser = preprocesser

    def __call__(self, cls):
        if not inspect.isclass(cls):
            raise TypeError(f'CloudObject expected to use with class type, not {type(cls)}')

        if self.co_class is None:
            self.co_class = cls
        else:
            raise Exception(f"Can't overwrite decorator, now is {self.co_class}")

        return self


class CloudObject:
    def __init__(self, cloud_object_class, s3_path, s3_config=None):
        self._obj_meta = None
        self._meta_meta = None
        self._s3_path = s3_path
        self._cls = cloud_object_class
        self._obj_attrs = {}
        self._s3_config = s3_config or {}

        self._s3 = boto3.client('s3',
                                aws_access_key_id=self._s3_config.get('aws_access_key_id'),
                                aws_secret_access_key=self._s3_config.get('aws_secret_access_key'),
                                region_name=self._s3_config.get('region_name'),
                                endpoint_url=self._s3_config.get('endpoint_url'),
                                config=botocore.client.Config(**self._s3_config.get('s3_config_kwargs', {})))

        self._obj_bucket, self._key = split_s3_path(s3_path)
        self._meta_bucket = self._obj_bucket + '.meta'

        logger.debug(f'{self._obj_bucket=},{self._meta_bucket=},{self._key=}')

    @property
    def path(self):
        return self._s3_path

    @property
    def meta_bucket(self):
        return self._meta_bucket

    @property
    def obj_bucket(self):
        return self._obj_bucket

    @property
    def s3(self):
        return self._s3

    @classmethod
    def new_from_s3(cls, cloud_object_class, s3_path, s3_config=None):
        co_instance = cls(cloud_object_class, s3_path, s3_config)
        return co_instance

    @classmethod
    def new_from_file(cls, cloud_object_class, file_path, cloud_path, s3_config=None):
        co_instance = cls(cloud_object_class, cloud_path, s3_config)

        if co_instance.exists():
            raise Exception('Object already exists')

        bucket, key = split_s3_path(cloud_path)

        co_instance._s3.upload_file(Filename=file_path, Bucket=bucket, Key=key)

    def _update_attrs(self):
        print(self._meta_meta)
        self._attributes = {key: value for key, value in self._meta_meta['Metadata'].items()}

    def exists(self):
        if not self._obj_meta:
            self.fetch()
        return bool(self._obj_meta)

    def is_staged(self):
        try:
            self._s3.head_object(Bucket=self._meta_bucket, Key=self._key)
            return True
        except botocore.exceptions.ClientError as e:
            logger.debug(e.response)
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise e

    def get_attribute(self, key):
        return self._obj_attrs[key]

    def fetch(self):
        if not self._obj_meta:
            logger.debug('fetching object head')
            try:
                head_res = self._s3.head_object(Bucket=self._obj_bucket, Key=self._key)
                del head_res['ResponseMetadata']
                self._obj_meta = head_res
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    self._obj_meta = None
                else:
                    raise e
        if not self._meta_meta:
            logger.debug('fetching meta head')
            try:
                head_res = self._s3.head_object(Bucket=self._meta_bucket, Key=self._key)
                del head_res['ResponseMetadata']
                self._meta_meta = head_res
                if 'Metadata' in head_res:
                    self._obj_attrs.update(head_res['Metadata'])
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == '404':
                    self._meta_meta = None
                else:
                    raise e
        return self._obj_meta, self._meta_meta

    def force_preprocess(self, local: bool = False, chunk_size: int = None, num_workers: int = None):
        if local:
            self.__local_preprocess(chunk_size, num_workers)
        else:
            raise NotImplementedError()

    def __local_preprocess(self, chunk_size: int = None, num_workers: int = None):
        if issubclass(self._cls.preprocesser, AsyncPreprocesser):
            get_res = self._s3.get_object(Bucket=self._obj_bucket, Key=self._key)
            logger.debug(get_res)
            obj_size = get_res['ContentLength']

            meta = types.SimpleNamespace(
                s3=self._s3,
                key=self._key,
                meta_bucket=self._meta_bucket,
                worker_id=1,
                chunk_size=obj_size,
                obj_size=obj_size,
                partitions=1
            )

            result = self._cls.preprocesser.preprocess(data_stream=get_res['Body'], meta=meta)

            try:
                body, meta = result
            except TypeError:
                raise Exception(f'Preprocessing result is {result}')

            if body is None or meta is None:
                raise Exception('Preprocessing result is {}'.format((body, meta)))

            put_res = self._s3.put_object(
                Body=body,
                Bucket=self._meta_bucket,
                Key=self._key,
                Metadata=meta
            )
            logger.debug(put_res)
            self._obj_attrs.update(meta)
        elif issubclass(self._cls.preprocesser, MapReducePreprocesser):
            head_res = self._s3.head_object(Bucket=self._obj_bucket, Key=self._key)
            print(head_res)
            obj_size = head_res['ContentLength']

            if chunk_size is not None and num_workers is not None:
                raise Exception('Both chunk_size and num_workers is not allowed')
            elif chunk_size is not None and num_workers is None:
                iterations = math.ceil(obj_size / chunk_size)
            elif chunk_size is None and num_workers is not None:
                iterations = num_workers
                chunk_size = round(obj_size / num_workers)
            else:
                raise Exception('At least chunk_size or num_workers parameter is required')

            map_results = []
            for i in range(iterations):
                r0 = i * chunk_size
                r1 = ((i * chunk_size) + chunk_size)
                r1 = r1 if r1 <= obj_size else obj_size
                get_res = self._s3.get_object(Bucket=self._obj_bucket, Key=self._key, Range=f'bytes={r0}-{r1}')

                meta = types.SimpleNamespace(
                    s3=self._s3,
                    key=self._key,
                    meta_bucket=self._meta_bucket,
                    worker_id=i,
                    chunk_size=chunk_size,
                    obj_size=obj_size,
                    partitions=iterations
                )

                result = self._cls.preprocesser.map(data_stream=get_res['Body'], meta=meta)
                map_results.append(result)

            reduce_result, meta = self._cls.preprocesser.reduce(map_results, self._s3)

    def get_meta_obj(self):
        get_res = self._s3.get_object(Bucket=self._meta_bucket, Key=self._key)
        return get_res['Body']

    def call(self, f, *args, **kwargs):
        if isinstance(f, str):
            func_name = f
        elif inspect.ismethod(f) or inspect.isfunction(f):
            func_name = f.__name__
        else:
            raise Exception(f)

        attr = getattr(self._child, func_name)
        return attr.__call__(*args, **kwargs)

    def partition(self, strategy, *args, **kwargs):
        return self.call(strategy, *args, **kwargs)
