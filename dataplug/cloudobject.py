from __future__ import annotations

import copy
import logging
import pickle
from collections import namedtuple
from copy import deepcopy
from functools import partial
from types import SimpleNamespace
from typing import TYPE_CHECKING

import botocore.exceptions
import smart_open

from .entities import CloudDataFormat, CloudObjectSlice
from .preprocessing.preprocess import monolithic_preprocessing, mapreduce_preprocessing
from .storage.picklableS3 import PickleableS3ClientProxy, S3Path
from .util import head_object, upload_file_with_progress

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from typing import List, Dict, Optional, Any
else:
    S3Client = object

logger = logging.getLogger(__name__)


class CloudObject:
    def __init__(
            self,
            data_format: CloudDataFormat,
            object_path: S3Path,
            meta_path: S3Path,
            attrs_path: S3Path,
            storage_config: Optional[Dict[str, Any]] = None,
            is_folder: bool = False                         # Allows to process formats that are defined as folders
    ):
        self._obj_headers: Optional[Dict[str, str]] = None  # Storage headers of the data object
        self._meta_headers: Optional[Dict[str, str]] = None  # Storage headers of the metadata object
        self._attrs_headers: Optional[Dict[str, str]] = None  # Storage headers of the attributes object

        # S3 Path for the metadata object. Located in bucket suffixed
        # with .meta with the same key as original data object
        self._obj_path = object_path

        # S3 Path for the attributes object. Located in bucket suffixed
        # with .meta with key as original data object suffixed with .attrs
        self._meta_path = meta_path

        self._format_cls: CloudDataFormat = data_format  # cls reference for the CloudDataType of this object

        self._attrs_path = attrs_path  # S3 Path for the attributes object
        self._attrs: Optional[SimpleNamespace] = None

        self._is_folder = is_folder

        storage_config = storage_config or {}
        self._s3: S3Client = PickleableS3ClientProxy(**storage_config)

        logger.info(f"Created reference for %s", self)
        logger.debug(f"{self._obj_path=},{self._meta_path=}")

    @property
    def path(self) -> S3Path:
        return self._obj_path

    @property
    def meta_path(self) -> S3Path:
        return self._meta_path

    @property
    def size(self) -> int:
        if not self._obj_headers:
            self.fetch()
        return int(self._obj_headers["ContentLength"])

    @property
    def meta_size(self) -> int:
        if self._meta_headers is None or "ContentLength" not in self._meta_headers:
            raise AttributeError()
        return int(self._meta_headers["ContentLength"])

    @property
    def storage(self) -> S3Client:
        return self._s3

    @property
    def attributes(self) -> Any:
        return self._attrs

    @property
    def open(self) -> smart_open.smart_open:
        assert self.storage is not None
        logger.debug("Creating new smart_open client for uri %s", self.path.as_uri())
        client = copy.deepcopy(self.storage)
        return partial(smart_open.open, self.path.as_uri(), transport_params={"client": client})

    @property
    def open_metadata(self) -> smart_open.smart_open:
        assert self.storage is not None
        logger.debug("Creating new smart_open client for uri %s", self.path.as_uri())
        client = copy.deepcopy(self.storage)
        return partial(smart_open.open, self.meta_path.as_uri(), transport_params={"client": client})

    @classmethod
    def from_s3(
            cls,
            data_format: CloudDataFormat,
            storage_uri: str,
            fetch: Optional[bool] = True,
            metadata_bucket: Optional[str] = None,
            s3_config: Optional[Dict[str, Any]] = None
    ) -> CloudObject:
        obj_path = S3Path.from_uri(storage_uri)
        if metadata_bucket is None:
            metadata_bucket = obj_path.bucket + ".meta"
        metadata_path = S3Path.from_bucket_key(metadata_bucket, obj_path.key)
        attributes_path = S3Path.from_bucket_key(metadata_bucket, obj_path.key + ".attrs")
        co = cls(data_format, obj_path, metadata_path, attributes_path, s3_config,data_format.is_folder)
        if fetch:
            co.fetch()
        return co

    @classmethod
    def from_bucket_key(cls, data_format, bucket, key, fetch=True) -> CloudObject:
        obj_path = S3Path.from_bucket_key(bucket, key)
        metadata_path = S3Path.from_bucket_key(bucket + ".meta", key)
        attributes_path = S3Path.from_bucket_key(bucket + ".meta", key + ".attrs")

        co = cls(data_format, obj_path, metadata_path, attributes_path)
        if fetch:
            co.fetch()
        return co

    @classmethod
    def new_from_file(cls, data_format, file_path, cloud_path, s3_config=None, override=False) -> "CloudObject":
        obj_path = S3Path.from_uri(cloud_path)
        metadata_path = S3Path.from_bucket_key(obj_path.bucket + ".meta", obj_path.key)
        attributes_path = S3Path.from_bucket_key(obj_path.bucket + ".meta", obj_path.key + ".attrs")
        co_instance = cls(data_format, obj_path, metadata_path, attributes_path, s3_config)

        if co_instance.exists():
            if not override:
                raise Exception("Object already exists")
            else:
                # Clean preprocessing metadata if object already exists
                co_instance.clean()

        upload_file_with_progress(co_instance.storage, co_instance.path.bucket, co_instance.path.key, file_path)
        return co_instance

    def exists(self) -> bool:
        if not self._obj_headers:
            try:
                self.fetch()
            except KeyError:
                return False
        return bool(self._obj_headers)

    def is_preprocessed(self) -> bool:
        try:
            head_object(self.storage, bucket=self._meta_path.bucket, key=self._meta_path.key)
            return True
        except KeyError:
            return False

    def fetch(self):
        if not self._obj_headers:
            if self._is_folder:
                self._obj_headers = {'Meta': 'Data'}        #Dummy metadata to temporarily bypass this. What could we fill this in with? Consider options
            else:
                logger.info("Fetching object from S3")
                self._fetch_object()
        if not self._meta_headers:
            logger.info("Fetching metadata from S3")
            self._fetch_metadata()

    def _fetch_object(self):
        self._obj_headers, _ = head_object(self._s3, self._obj_path.bucket, self._obj_path.key)
    
    def _fetch_metadata(self):
        try:
            res, _ = head_object(self._s3, self._meta_path.bucket, self._meta_path.key)
            self._meta_headers = res
            res, _ = head_object(self._s3, self._attrs_path.bucket, self._attrs_path.key)
            self._attrs_headers = res
            get_res = self.storage.get_object(Bucket=self._attrs_path.bucket, Key=self._attrs_path.key)
            try:
                attrs_dict = pickle.load(get_res["Body"])
                # Get default attributes from the class,
                # so we can have default attributes different from None set in the Class
                base_attrs = deepcopy(self._format_cls.attrs_types)
                # Replace attributes that have been set in the preprocessing stage
                base_attrs.update(attrs_dict)
                # Create namedtuple so that the attributes object is immutable
                co_named_tuple = namedtuple(self._format_cls.co_class.__name__ + "Attributes", base_attrs.keys())
                self._attrs = co_named_tuple(**base_attrs)
            except Exception as e:
                logger.error(e)
                self._attrs = None
        except KeyError as e:
            self._meta_headers = None
            self._attrs = None

    def clean(self):
        logger.info("Cleaning indexes and metadata for %s", self)
        self._s3.delete_object(Bucket=self._meta_path.bucket, Key=self._meta_path.key)
        self._meta_headers = None
        self.storage.delete_object(Bucket=self._attrs_path.bucket, Key=self._attrs_path.key)
        self._attrs_headers = None
        self._attrs = {}

    def preprocess(self, parallel_config=None, extra_args=None, chunk_size=None, force=False, debug=False):
        assert self.exists(), "Object not found in S3"
        if self.is_preprocessed() and not force:
            return

        parallel_config = parallel_config or {}
        extra_args = extra_args or {}

        # Check if the metadata bucket exists, if not create it
        try:
            meta_bucket_head = self.storage.head_bucket(Bucket=self.meta_path.bucket)
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] != '404':
                raise error
            meta_bucket_head = None

        if not meta_bucket_head:
            logger.info("Creating meta bucket %s".format(self.meta_path.bucket))
            try:
                self.storage.create_bucket(Bucket=self.meta_path.bucket)
            except botocore.exceptions.ClientError as error:
                logger.error("Metadata bucket %s not found -- Also failed to create it", self.meta_path.bucket)
                raise error

        if chunk_size is None:
            monolithic_preprocessing(self, parallel_config, self._format_cls.preprocessing_function, extra_args)
        else:
            assert chunk_size != 0 and chunk_size <= self.size, ("Chunk size must be greater than 0 "
                                                                 "and less or equal to object size")
            assert self._format_cls.finalizer_function is not None, "Finalizer function must be defined for mapreduce"
            mapreduce_preprocessing(self, parallel_config, chunk_size, self._format_cls.preprocessing_function,
                                    self._format_cls.finalizer_function, extra_args)

        self.fetch()

    def get_attribute(self, key: str) -> Any:
        return getattr(self._attrs, key)

    def partition(self, strategy, *args, **kwargs) -> List[CloudObjectSlice]:
        assert self.is_preprocessed(), "Object must be preprocessed before partitioning"

        slices = strategy(self, *args, **kwargs)
        # Store a reference to this CloudObject instance in the slice
        for s in slices:
            s.cloud_object = self
        return slices

    def __getitem__(self, item):
        return self._attrs.__getattribute__(item)

    def __repr__(self):
        return f"{self.__class__.__name__}<{self._format_cls.co_class.__name__}>({self.path.as_uri()})"
