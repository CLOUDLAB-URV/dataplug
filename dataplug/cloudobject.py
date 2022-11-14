from __future__ import annotations

import inspect
import logging
from typing import Union, Callable, List, Concatenate
from typing import TYPE_CHECKING, Tuple, Dict, Optional

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
else:
    S3Client = object

from .util import split_s3_path, head_object
from .storage import PureS3Path, PickleableS3ClientProxy
from .preprocess import BatchPreprocessor, MapReducePreprocessor, PreprocessorBackendBase

logger = logging.getLogger(__name__)


class CloudDataType:
    def __init__(self,
                 preprocessor: Union[BatchPreprocessor, MapReducePreprocessor] = None,
                 inherit_from: 'CloudDataType' = None):
        # print(preprocesser, inherit)
        self.co_class: object = None
        self.__preprocessor: Union[BatchPreprocessor, MapReducePreprocessor] = preprocessor
        self.__parent: 'CloudDataType' = inherit_from

    @property
    def preprocessor(self) -> Union[BatchPreprocessor, MapReducePreprocessor]:
        if self.__preprocessor is not None:
            return self.__preprocessor
        elif self.__parent is not None:
            return self.__parent.preprocessor
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
                 cloud_object_class: CloudDataType,
                 s3_uri_path: str,
                 s3_config: dict = None):
        self._obj_meta: Optional[Dict[str, str]] = None
        self._meta_meta: Optional[Dict[str, str]] = None
        self._obj_path: PureS3Path = PureS3Path.from_uri(s3_uri_path)
        self._meta_path: PureS3Path = PureS3Path.from_bucket_key(self._obj_path.bucket + '.meta', self._obj_path.key)
        self._cls: CloudDataType = cloud_object_class
        self._s3: PickleableS3ClientProxy = PickleableS3ClientProxy(
            aws_access_key_id=s3_config.get('aws_access_key_id'),
            aws_secret_access_key=s3_config.get('aws_secret_access_key'),
            region_name=s3_config.get('region_name'),
            endpoint_url=s3_config.get('endpoint_url'),
            config_kwargs=s3_config.get('s3_config_kwargs'),
            use_token=s3_config.get('use_token')
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
    def s3(self) -> S3Client:
        return self._s3

    @classmethod
    def from_s3(cls, cloud_object_class, s3_path, s3_config=None, fetch=True) -> 'CloudObject':
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
            head_object(self.s3, bucket=self._meta_path.bucket, key=self._meta_path.key)
            return True
        except KeyError:
            return False

    def fetch(self, enforce_obj: bool = False, enforce_meta: bool = False) -> \
            Tuple[Optional[Dict[str, str]], Optional[Dict[str, str]]]:
        """
        Get object metadata from storage with HEAD object request
        :param enforce_obj: True to raise KeyError exception if object key is not found in storage
        :param enforce_meta: True to raise KeyError exception if metadata key is not found in storage
        :return: Tuple for object metadata and objectmeta metadata
        """
        logger.info('Fetching object from S3')
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

    def preprocess(self, preprocessor_backend: PreprocessorBackendBase, chunk_size: int = None,
                   num_workers: int = None, *args, **kwargs):
        preprocessor_backend.do_preprocess(preprocessor=self._cls.preprocessor, cloud_object=self,
                                           chunk_size=chunk_size, num_workers=num_workers, *args, **kwargs)

    # def get_meta_obj(self):
    #     get_res = self._s3.get_object(Bucket=self._meta_path.bucket, Key=self._obj_path.key)
    #     return get_res['Body']
    #
    # def call(self, f, *args, **kwargs):
    #     if isinstance(f, str):
    #         func_name = f
    #     elif inspect.ismethod(f) or inspect.isfunction(f):
    #         func_name = f.__name__
    #     else:
    #         raise Exception(f)
    #
    #     attr = getattr(self._child, func_name)
    #     return attr.__call__(*args, **kwargs)

    def partition(self, strategy, *args, **kwargs):
        slices = strategy(self, *args, **kwargs)
        [s.contextualize(self) for s in slices]
        return slices
