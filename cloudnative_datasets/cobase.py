from __future__ import annotations

import inspect
import logging
import math
from typing import Union, Callable, List, Concatenate
from dataclasses import dataclass
from typing import TYPE_CHECKING, Tuple, Dict, Optional

import boto3
import botocore

from .cochunkbase import CloudObjectSlice

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
else:
    S3Client = object

from .util import split_s3_path, head_object
from .storage import PureS3Path, PickleableS3ClientProxy
from .preprocessers import BatchPreprocesser, MapReducePreprocesser

logger = logging.getLogger(__name__)


class CloudObjectWrapper:
    def __init__(self,
                 preprocesser: Union[BatchPreprocesser, MapReducePreprocesser] = None,
                 inherit_from: 'CloudObjectWrapper' = None):
        # print(preprocesser, inherit)
        self.co_class: object = None
        self.__preprocesser: Union[BatchPreprocesser, MapReducePreprocesser] = preprocesser
        self.__parent: 'CloudObjectWrapper' = inherit_from

    @property
    def preprocesser(self) -> Union[BatchPreprocesser, MapReducePreprocesser]:
        if self.__preprocesser is not None:
            return self.__preprocesser
        elif self.__parent is not None:
            return self.__parent.preprocesser
        else:
            raise Exception('There is not preprocesser')

    def __call__(self, cls):
        if not inspect.isclass(cls):
            raise TypeError(f'CloudObject expected to use with class type, not {type(cls)}')

        if self.co_class is None:
            self.co_class = cls
        else:
            raise Exception(f"Can't overwrite decorator, now is {self.co_class}")

        return self


class CloudObject:
    def __init__(self,
                 cloud_object_class: CloudObjectWrapper,
                 s3_uri_path: str,
                 s3_config: dict = None):
        self._obj_meta: Optional[Dict[str, str]] = None
        self._meta_meta: Optional[Dict[str, str]] = None
        self._obj_path: PureS3Path = PureS3Path.from_uri(s3_uri_path)
        self._meta_path: PureS3Path = PureS3Path.from_bucket_key(self._obj_path.bucket + '.meta', self._obj_path.key)
        self._cls: CloudObjectWrapper = cloud_object_class
        self._s3: PickleableS3ClientProxy = PickleableS3ClientProxy(
            aws_access_key_id=s3_config.get('aws_access_key_id'),
            aws_secret_access_key=s3_config.get('aws_secret_access_key'),
            region_name=s3_config.get('region_name'),
            endpoint_url=s3_config.get('endpoint_url'),
            config=s3_config.get('s3_config_kwargs')
        )

        logger.debug(f'{self._obj_path=},{self._meta_path=}')

    @property
    def path(self) -> PureS3Path:
        return self._obj_path

    @property
    def meta_path(self) -> PureS3Path:
        return self._meta_path

    @property
    def size(self) -> int:
        if not self._obj_meta:
            self.fetch()
        return int(self._obj_meta['ContentLength'])

    @property
    def s3client(self) -> S3Client:
        return self._s3

    @classmethod
    def new_from_s3(cls, cloud_object_class, s3_path, s3_config=None, fetch=True) -> 'CloudObject':
        co_instance = cls(cloud_object_class, s3_path, s3_config)
        if fetch:
            co_instance.fetch(enforce_obj=True)
        return co_instance

    @classmethod
    def new_from_file(cls, cloud_object_class, file_path, cloud_path, s3_config=None) -> 'CloudObject':
        co_instance = cls(cloud_object_class, cloud_path, s3_config)

        if co_instance.exists():
            raise Exception('Object already exists')

        bucket, key = split_s3_path(cloud_path)

        co_instance._s3.upload_file(Filename=file_path, Bucket=bucket, Key=key)
        return co_instance

    def exists(self) -> bool:
        if not self._obj_meta:
            self.fetch()
        return bool(self._obj_meta)

    def is_preprocessed(self) -> bool:
        try:
            self._s3.head_object(Bucket=self._meta_path.bucket, Key=self._meta_path.key)
            return True
        except botocore.exceptions.ClientError as e:
            logger.debug(e.response)
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise e

    def fetch(self, enforce_obj: bool = False, enforce_meta: bool = False) -> \
            Tuple[Optional[Dict[str, str]], Optional[Dict[str, str]]]:
        """
        Get object metadata from storage with HEAD object request
        :param enforce_obj: True to raise KeyError exception if object key is not found in storage
        :param enforce_meta: True to raise KeyError exception if metadata key is not found in storage
        :return: Tuple for object metadata and objectmeta metadata
        """
        if not self._obj_meta:
            try:
                res, _ = head_object(self._s3, self._obj_path.bucket, self._obj_path.key)
                self._obj_meta = res
            except KeyError as e:
                self._obj_meta = None
                if enforce_obj:
                    raise e
        if not self._meta_meta:
            try:
                res, _ = head_object(self._s3, self._meta_path.bucket, self._meta_path.key)
                self._meta_meta = res
            except KeyError as e:
                self._meta_meta = None
                if enforce_meta:
                    raise e
            return self._obj_meta, self._meta_meta

    def force_preprocess(self, local: bool = False, chunk_size: int = None, num_workers: int = None):
        if local:
            self.__local_preprocess(chunk_size, num_workers)
        else:
            raise NotImplementedError()

    def __local_preprocess(self, chunk_size: int = None, num_workers: int = None):
        preprocesser = self._cls.preprocesser
        if issubclass(preprocesser, BatchPreprocesser):
            get_res = self._s3.get_object(Bucket=self._obj_path.bucket, Key=self._obj_path.key)
            logger.debug(get_res)
            obj_size = get_res['ContentLength']

            meta = PreprocesserMetadata(
                s3=self._s3,
                obj_path=self._obj_path,
                meta_path=self._meta_path,
                worker_id=1,
                chunk_size=obj_size,
                obj_size=obj_size,
                partitions=1
            )

            result = preprocesser.preprocess(data_stream=get_res['Body'], meta=meta)

            try:
                body, meta = result
            except TypeError:
                raise Exception(f'Preprocessing result is {result}')

            if body is None or meta is None:
                raise Exception('Preprocessing result is {}'.format((body, meta)))

            self._s3.upload_fileobj(
                Fileobj=body,
                Bucket=self._meta_bucket,
                Key=self._meta_key,
                ExtraArgs={'Metadata': meta}
            )

            if hasattr(body, 'close'):
                body.close()

            self._obj_attrs.update(meta)
        elif issubclass(preprocesser, MapReducePreprocesser):
            head_res = self._s3.head_object(Bucket=self._obj_bucket, Key=self._obj_key)
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
                get_res = self._s3.get_object(Bucket=self._obj_bucket, Key=self._obj_key, Range=f'bytes={r0}-{r1}')

                meta = PreprocesserMetadata(
                    s3=self._s3,
                    object_bucket=self._obj_bucket,
                    object_key=self._obj_key,
                    meta_bucket=self._meta_bucket,
                    meta_key=self._meta_key,
                    worker_id=i,
                    chunk_size=chunk_size,
                    obj_size=obj_size,
                    partitions=iterations
                )

                result = self._cls.__preprocesser.map(data_stream=get_res['Body'], meta=meta)
                map_results.append(result)

            reduce_result, meta = self._cls.__preprocesser.reduce(map_results, self._s3)
        else:
            raise Exception(f'Preprocessor is not a subclass of {BatchPreprocesser} or {MapReducePreprocesser}')

        def get_meta_obj(self):
            get_res = self._s3.get_object(Bucket=self._meta_bucket, Key=self._obj_key)
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

    def partition(self, strategy: Callable[Concatenate['CloudObject', ...], List[CloudObjectSlice]], *args, **kwargs):
        slices = strategy(self, *args, **kwargs)
        [slice.contextualize(self) for slice in slices]
        return slices


@dataclass
class PreprocesserMetadata:
    """
    Data Class structure containing the metadata used by the preprocesser class
    """
    s3: boto3.client
    obj_path: PureS3Path
    meta_path: PureS3Path
    worker_id: int
    chunk_size: int
    obj_size: int
    partitions: int = 1
