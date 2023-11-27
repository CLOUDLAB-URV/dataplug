from __future__ import annotations

import logging
import math
import pickle
import re
from collections import namedtuple
from copy import deepcopy
from functools import partial
from types import SimpleNamespace
from typing import TYPE_CHECKING

import botocore.exceptions
import smart_open

from .core import *
from .preprocessing import (
    BatchPreprocessor,
    MapReducePreprocessor,
    PreprocessorBackendBase,
)
from .storage.storage import StoragePath, S3ObjectStorage, create_client
from .util import split_s3_path, head_object, NoPublicConstructor

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from typing import List, Tuple, Dict, Optional, Any
else:
    S3Client = object

logger = logging.getLogger(__name__)


class CloudObject(metaclass=NoPublicConstructor):
    """
    Reference to a Cloud Object.
    """

    def __init__(
            self,
            data_format: CloudDataFormat,
            object_path: StoragePath,
            meta_path: StoragePath,
            attributes_path: StoragePath,
            storage: S3Client,
    ):
        self._obj_headers: Optional[Dict[str, str]] = None  # Storage headers of the data object
        self._meta_headers: Optional[Dict[str, str]] = None  # Storage headers of the metadata object
        self._attrs_headers: Optional[Dict[str, str]] = None  # Storage headers of the attributes object
        self._obj_path: StoragePath = object_path  # S3 Path for the metadata object. Located in bucket suffixed with .meta with the same key as original data object
        self._meta_path: StoragePath = meta_path  # Storage path for the metadata object. Located in bucket suffixed with .meta with key as original data object suffixed with .meta
        self._attrs_path: StoragePath = attributes_path  # Storage path for the attributes object. Located in bucket suffixed with .meta with key as original data object suffixed with .attrs
        self._format: CloudDataFormat = data_format  # cls reference for the data format of this object
        self._attrs: Optional[SimpleNamespace] = None
        self._storage = storage

        logger.info(f"Created reference for %s", self)
        logger.debug(f"{self._obj_path=},{self._meta_path=},{self._attrs_path=}")

    @property
    def path(self) -> StoragePath:
        """
        Get the S3Path of this Cloud Object

        :return: S3Path for this Cloud Object
        """
        return self._obj_path

    @property
    def meta_path(self) -> StoragePath:
        """
        Get the S3Path of the metadata of this Cloud Object

        :return: S3Path of the metadata for this Cloud Object
        """
        return self._meta_path

    @property
    def size(self) -> int:
        """
        Returns the data size of this Cloud Object

        :return: Size in bytes of this Cloud Object
        """
        if not self._obj_headers:
            self.fetch()
        return int(self._obj_headers["ContentLength"])

    @property
    def meta_size(self) -> int:
        """
        Returns the size of the metadata object of this Cloud Object

        :return: Size in bytes of the metadata object of this Cloud Object
        """
        if self._meta_headers is None or "ContentLength" not in self._meta_headers:
            raise AttributeError()
        return int(self._meta_headers["ContentLength"])

    @property
    def storage(self) -> S3Client:
        """
        Return a reference of the storage client
        :return:
        """
        return self._storage

    @property
    def attributes(self) -> Any:
        return self._attrs

    def open(self, *args, **kwargs):
        """
        Open cloud object content as a file
        """
        logger.debug("Opening %s as file", self._obj_path.as_uri())
        return self.storage._open_as_file(self._obj_path.bucket, self._obj_path.key, *args, **kwargs)

    def open_metadata(self, *args, **kwargs):
        """
        Open cloud object metadata content as a file using smart_open
        """
        logger.debug("Opening %s as file", self._meta_path.as_uri())
        return self.storage._open_as_file(self._meta_path.bucket, self._meta_path.key, *args, **kwargs)

    @classmethod
    def from_path(
            cls,
            data_format: CloudDataFormat,
            storage_uri: str,
            storage_config: Optional[dict] = None,
            fetch: Optional[bool] = True,
            metadata_bucket: Optional[str] = None,
    ) -> CloudObject:
        match = re.match(r"^([a-zA-Z0-9]+)://", storage_uri)
        if match:
            storage = match.group(1)
            prefix = match.group()
        else:
            raise ValueError(f"{storage_uri} is not an uri")

        path = storage_uri[len(prefix):]
        storage_client = create_client(storage, storage_config or {})
        obj_path = storage_client._parse_full_path(path)

        if metadata_bucket is None:
            metadata_bucket = obj_path.bucket + ".meta"
        metadata_path = StoragePath.from_bucket_key(obj_path.storage, metadata_bucket, obj_path.key)
        attributes_path = StoragePath.from_bucket_key(obj_path.storage, metadata_bucket, obj_path.key + ".attrs")
        co = cls._create(
            data_format=data_format,
            object_path=obj_path,
            meta_path=metadata_path,
            attributes_path=attributes_path,
            storage=storage_client,
        )
        if fetch:
            co.fetch(enforce_obj=True)
        return co

    @classmethod
    def from_bucket_key(
            cls,
            data_format: CloudDataFormat,
            storage: str,
            bucket: str,
            key: str,
            metadata_bucket: Optional[str] = None,
            storage_config: Optional[dict] = None,
            fetch: bool = True,
    ) -> CloudObject:
        obj_path = StoragePath.from_bucket_key(storage, bucket, key)
        if metadata_bucket is None:
            metadata_bucket = obj_path.bucket + ".meta"
        metadata_path = StoragePath.from_bucket_key(obj_path.storage, metadata_bucket, obj_path.key)
        attributes_path = StoragePath.from_bucket_key(obj_path.storage, metadata_bucket, obj_path.key + ".attrs")
        storage_client = create_client(storage, storage_config or {})
        co = cls._create(
            data_format=data_format,
            object_path=obj_path,
            meta_path=metadata_path,
            attrs_path=attributes_path,
            storage_client=storage_client,
        )
        if fetch:
            co.fetch(enforce_obj=True)
        return co

    def exists(self) -> bool:
        if not self._obj_headers:
            self.fetch()
        return bool(self._obj_headers)

    def is_preprocessed(self) -> bool:
        self.fetch()
        return self._meta_headers is not None or self._attrs_headers is not None

    def fetch(
            self, enforce_obj: bool = True, enforce_meta: bool = False
    ) -> Tuple[Optional[Dict[str, str]], Optional[Dict[str, str]], Optional[Dict[str, str]]]:
        """
        Get object metadata from storage with HEAD object request

        :param enforce_obj: True to raise KeyError exception if object key is not found in storage
        :param enforce_meta: True to raise KeyError exception if metadata key is not found in storage

        :return: Tuple of (data_object metadata, meta_object metadata, attrs_objet metadata)
        """
        logger.info("Fetching object %s", self)

        if not self._obj_headers:
            self._obj_headers = head_object(self._storage, self._obj_path.bucket, self._obj_path.key)
            if enforce_obj and self._obj_headers is None:
                raise KeyError(f"Object {self._obj_path.as_uri()} not found")

        if not self._meta_headers:
            self._meta_headers = head_object(self._storage, self._meta_path.bucket, self._meta_path.key)
            if enforce_meta and elf._meta_headers is None:
                raise KeyError(f"Metadata object {self._meta_path.as_uri()} not found")

        if not self._attrs_headers:
            self._attrs_headers = head_object(self._storage, self._attrs_path.bucket, self._attrs_path.key)
            if enforce_meta and self._attrs_headers is None:
                raise KeyError(f"Attributes object {self._attrs_path.as_uri()} not found")
            if self._attrs_headers is not None:
                get_res = self.storage.get_object(Bucket=self._attrs_path.bucket, Key=self._attrs_path.key)
                attrs_dict = pickle.load(get_res["Body"])
                # Get default attributes from the class,
                # so we can have default attributes different from None set in the Class
                base_attrs = {k: v for k, (v, _) in self._format._default_attrs.items()}
                # Replace attributes that have been set in the preprocessing stage
                base_attrs.update(attrs_dict)
                # Create namedtuple so that the attributes object is immutable
                co_named_tuple = namedtuple(self._format._wrappee.__name__ + "Attributes", base_attrs.keys())
                self._attrs = co_named_tuple(**base_attrs)

        return self._obj_headers, self._meta_headers, self._attrs_headers

    def preprocess(
            self, preprocessor_backend: PreprocessorBackendBase, force: bool = False, ignore: bool = False, *args,
            **kwargs
    ):
        """
        Manually launch the preprocessing job for this cloud object on the specified preprocessing backend

        :param preprocessor_backend: Preprocessor backend instance on to execute the preprocessing job
        :param force: Forces pre-processing on this cloud object, even if it is already preprocessed
        :param ignore: Ignore pre-processing if object is already pre-processed (does not raise Exception)
        :param args: Optional arguments to pass to the preprocessing job
        :param kwargs:Optional keyword arguments to pass to the preprocessing job
        """
        # Check if meta bucket exists
        try:
            meta_bucket_head = self.storage.head_bucket(Bucket=self.meta_path.bucket)
        except botocore.exceptions.ClientError as error:
            if error.response["Error"]["Code"] != "404":
                raise error
            meta_bucket_head = None

        if not meta_bucket_head:
            logger.info("Creating meta bucket... (%s)", self.meta_path.bucket)
            try:
                create_bucket_response = self.storage.create_bucket(Bucket=self.meta_path.bucket)
            except botocore.exceptions.ClientError as error:
                logger.error("Metadata bucket %s not found -- Also failed to create it", self.meta_path.bucket)
                raise error

        if self.is_preprocessed() and not force:
            raise Exception("Object is already pre-processed")
        if self.is_preprocessed() and ignore:
            logging.info("Object %s is already pre-processed, ignoring pre-processing job...", self)
            return None

        # TODO implement this properly
        if issubclass(self._format._preprocessor, BatchPreprocessor):
            batch_preprocessor: BatchPreprocessor = self._format._preprocessor(*args, **kwargs)
            future = preprocessor_backend.run_batch_job(batch_preprocessor, self)
            return future
        elif issubclass(self._format._preprocessor, MapReducePreprocessor):
            mapreduce_preprocessor: MapReducePreprocessor = self._format._preprocessor(*args, **kwargs)

            # Check mapreduce parameters
            if mapreduce_preprocessor.map_chunk_size is not None and mapreduce_preprocessor.num_mappers is not None:
                raise Exception('Setting both "map_chunk_size" and "num_mappers" is not allowed')

            if mapreduce_preprocessor.map_chunk_size is not None and mapreduce_preprocessor.num_mappers is None:
                # Calculate number of mappers from mapper chunk size
                mapreduce_preprocessor.num_mappers = math.ceil(self.size / mapreduce_preprocessor.map_chunk_size)
            elif mapreduce_preprocessor.map_chunk_size is None and mapreduce_preprocessor.num_mappers is not None:
                # Calculate mappers chunk size from number of mappers
                mapreduce_preprocessor.map_chunk_size = round(self.size / mapreduce_preprocessor.num_mappers)
            else:
                raise Exception(
                    f'At least "map_chunk_size" or "num_mappers" parameter is required for {MapReducePreprocessor.__class__.__name__}'
                )
            future = preprocessor_backend.run_mapreduce_job(mapreduce_preprocessor, self)
            return future
        else:
            raise Exception("This object cannot be preprocessed")

    def get_attribute(self, key: str) -> Any:
        """
        Get an attribute of this cloud object. Must be preprocessed first. Raises AttributeError if the
        specified key does not exist.

        :param key: Attribute key

        :return: Attribute
        """
        return getattr(self._attrs, key)

    def partition(self, strategy: PartitioningStrategy, *args, **kwargs) -> List[CloudObjectSlice]:
        """
        Apply partitioning strategy on this cloud object.

        :param strategy: Partitioning strategy
        :param args: Optional arguments to pass to the partitioning strategy functions
        :param kwargs: Optional key-words arguments to pass to the partitioning strategy
        """
        if not self.is_preprocessed():
            raise Exception("Object is not pre-processed")

        if strategy._data_format is not self._format:
            raise Exception("Partitioning strategy is not compatible with this CloudObject")

        slices = strategy._func(self, *args, **kwargs)
        # Store a reference to this CloudObject instance in the slice
        for s in slices:
            s.cloud_object = self
        return slices

    def __getitem__(self, item):
        return self._attrs.__getattribute__(item)

    def __repr__(self):
        return f"{self.__class__.__name__}<{self._format._wrappee.__name__}>({self.path.as_uri()})"
