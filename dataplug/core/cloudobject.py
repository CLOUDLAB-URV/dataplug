from __future__ import annotations

import logging
import math
import pickle
from collections import namedtuple
from copy import deepcopy
from functools import partial
from types import SimpleNamespace
from typing import TYPE_CHECKING

import botocore.exceptions
import smart_open

from dataplug.core.dataslice import CloudObjectSlice
from dataplug.core.formattemplate import CloudDataFormatTemplate
from dataplug.preprocessing import (
    BatchPreprocessor,
    MapReducePreprocessor,
    PreprocessorBackendBase,
)
from dataplug.storage.storage import StoragePath, PickleableS3ClientProxy
from dataplug.util import split_s3_path, head_object

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from typing import List, Tuple, Dict, Optional, Any
    from dataplug.preprocessing.backendbase import PreprocessingJobFuture
else:
    S3Client = object

logger = logging.getLogger(__name__)


class CloudObject:
    """
    Reference to a Cloud Object.

    :param data_format_template: Cloud-Native data type for this object.
    :param storage_path: Full S3 URI (s3://bucket/key) for this object.
    :param storage_config: Extra S3 config.
    """

    def __init__(
            self,
            data_format_template: CloudDataFormatTemplate,
            object_path: StoragePath,
            meta_path: StoragePath,
            storage_backend: str,
            storage_config: Optional[dict] = None
    ):
        self._obj_headers: Optional[Dict[str, str]] = None  # Storage headers of the data object
        self._meta_headers: Optional[Dict[str, str]] = None  # Storage headers of the metadata object
        self._attrs_headers: Optional[Dict[str, str]] = None  # Storage headers of the attributes object

        self._obj_path = object_path
        # S3 Path for the metadata object. Located in bucket suffixed
        # with .meta with the same key as original data object

        self._meta_path = meta_path
        # S3 Path for the attributes object. Located in bucket suffixed
        # with .meta with key as original data object suffixed with .attrs

        self._cls: CloudDataFormatTemplate = data_format_template  # cls reference for the CloudDataType of this object

        if s3_client is None:
            storage_config = storage_config or {}
            self._s3: PickleableS3ClientProxy = PickleableS3ClientProxy(
                aws_access_key_id=storage_config.get("aws_access_key_id"),
                aws_secret_access_key=storage_config.get("aws_secret_access_key"),
                region_name=storage_config.get("region_name"),
                endpoint_url=storage_config.get("endpoint_url"),
                botocore_config_kwargs=storage_config.get("s3_config_kwargs"),
                use_token=storage_config.get("use_token"),
                role_arn=storage_config.get("role_arn"),
                token_duration_seconds=storage_config.get("token_duration_seconds"),
            )
        else:
            self._s3 = s3_client

        self._attrs: Optional[SimpleNamespace] = None

        logger.debug(f"{self._obj_path=},{self._meta_path=}")
        logger.info(f"Created reference for %s", self)

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
        return self._s3

    @property
    def attributes(self) -> Any:
        return self._attrs

    @property
    def open(self) -> smart_open.smart_open:
        """
        Open cloud object content as a file using smart_open
        """
        logger.debug("Creating new smart_open client for uri %s", self.path.as_uri())
        client = self.storage._new_client()
        return partial(smart_open.open, self.path.as_uri(), transport_params={"client": client})

    @property
    def open_metadata(self) -> smart_open.smart_open:
        """
        Open cloud object metadata content as a file using smart_open
        """
        logger.debug("Creating new smart_open client for uri %s", self.path.as_uri())
        client = self.storage._new_client()
        return partial(smart_open.open, self.meta_path.as_uri(), transport_params={"client": client})

    @classmethod
    def from_s3(
            cls,
            cloud_object_class: CloudDataFormatTemplate,
            s3_path: str,
            s3_config: dict = None,
            fetch: bool = True,
            metadata_bucket: str = None
    ) -> CloudObject:
        return cls.from_storage(cloud_object_class, s3_path, s3_config, fetch, metadata_bucket)

    @classmethod
    def from_storage(
            cls,
            cloud_object_class: CloudDataFormatTemplate,
            storage_uri: str,
            storage_config: Optional[dict] = None,
            fetch: Optional[bool] = True,
            metadata_bucket: Optional[str] = None
    ) -> CloudObject:
        obj_path = StoragePath.from_uri(storage_uri)
        if metadata_bucket is None:
            metadata_bucket = obj_path.bucket + ".meta"
        metadata_path = StoragePath.from_storage_bucket_key(obj_path.storage, metadata_bucket, obj_path.key)
        attributes_path = StoragePath.from_storage_bucket_key(obj_path.storage, metadata_bucket,
                                                              obj_path.key + ".attrs")
        co = cls(cloud_object_class, obj_path, metadata_path, obj_path.storage, storage_config)
        if fetch:
            co.fetch(enforce_obj=True)
        return co

    @classmethod
    def from_bucket_key(cls, cloud_object_class, storage, bucket, key, storage_config=None, fetch=True) -> CloudObject:
        co = cls(cloud_object_class, storage_uri, storage_config)
        if fetch:
            co.fetch(enforce_obj=True)
        return co

    @classmethod
    def new_from_file(cls, cloud_object_class, file_path, cloud_path, s3_config=None) -> "CloudObject":
        co_instance = cls(cloud_object_class, cloud_path, s3_config)

        if co_instance.exists():
            raise Exception("Object already exists!")

        bucket, key = split_s3_path(cloud_path)

        co_instance._s3.upload_file(Filename=file_path, Bucket=bucket, Key=key)
        return co_instance

    def exists(self) -> bool:
        if not self._obj_headers:
            self.fetch()
        return bool(self._obj_headers)

    def is_preprocessed(self) -> bool:
        try:
            head_object(self.storage, bucket=self._meta_path.bucket, key=self._meta_path.key)
            return True
        except KeyError:
            return False

    def fetch(
            self, enforce_obj: bool = True, enforce_meta: bool = False
    ) -> Tuple[Optional[Dict[str, str]], Optional[Dict[str, str]], Optional[Dict[str, str]]]:
        """
        Get object metadata from storage with HEAD object request

        :param enforce_obj: True to raise KeyError exception if object key is not found in storage
        :param enforce_meta: True to raise KeyError exception if metadata key is not found in storage

        :return: Tuple of (data_object metadata, meta_object metadata, attrs_objet metadata)
        """
        logger.info("Fetching object from S3")

        if not self._obj_headers:
            try:
                res, _ = head_object(self._s3, self._obj_path.bucket, self._obj_path.key)
                self._obj_headers = res
            except KeyError as e:
                self._obj_headers = None
                if enforce_obj:
                    raise e

        if not self._meta_headers:
            try:
                # TODO check if dataplug version metadata from meta object matches local dataplug version
                res, _ = head_object(self._s3, self._meta_path.bucket, self._meta_path.key)
                self._meta_headers = res
            except KeyError as e:
                self._meta_headers = None
                if enforce_meta:
                    raise e

        if not self._attrs_headers:
            try:
                # TODO check if dataplug version metadata from attrs object matches local dataplug version
                res, _ = head_object(self._s3, self._attrs_path.bucket, self._attrs_path.key)
                self._attrs_headers = res
                get_res = self.storage.get_object(Bucket=self._attrs_path.bucket, Key=self._attrs_path.key)
                try:
                    attrs_dict = pickle.load(get_res["Body"])
                    # Get default attributes from the class,
                    # so we can have default attributes different from None set in the Class
                    base_attrs = deepcopy(self._cls.cls_attributes)
                    # Replace attributes that have been set in the preprocessing stage
                    base_attrs.update(attrs_dict)
                    # Create namedtuple so that the attributes object is immutable
                    co_named_tuple = namedtuple(self._cls.co_class.__name__ + "Attributes", base_attrs.keys())
                    self._attrs = co_named_tuple(**base_attrs)
                except Exception as e:
                    logger.error(e)

            except KeyError as e:
                self._meta_headers = None
                if enforce_meta:
                    raise e

        return self._obj_headers, self._meta_headers, self._attrs_headers

    def preprocess(
            self, preprocessor_backend: PreprocessorBackendBase, force: bool = False, ignore: bool = False, *args,
            **kwargs
    ):
        future = self.async_preprocess(preprocessor_backend, force, ignore, *args, **kwargs)
        future.check_result()
        self.fetch()

    def async_preprocess(
            self, preprocessor_backend: PreprocessorBackendBase, force: bool = False, ignore: bool = False, *args,
            **kwargs
    ) -> Optional[PreprocessingJobFuture]:
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
            if error.response['Error']['Code'] != '404':
                raise error
            meta_bucket_head = None

        if not meta_bucket_head:
            logger.info("Creating meta bucket... (%s)".format(self.meta_path.bucket))
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

        # FIXME implement this properly
        if issubclass(self._cls.preprocessor, BatchPreprocessor):
            batch_preprocessor: BatchPreprocessor = self._cls.preprocessor(*args, **kwargs)
            preprocessor_backend.setup()
            future = preprocessor_backend.submit_batch_job(batch_preprocessor, self)
            return future
        elif issubclass(self._cls.preprocessor, MapReducePreprocessor):
            mapreduce_preprocessor: MapReducePreprocessor = self._cls.preprocessor(*args, **kwargs)

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

            preprocessor_backend.setup()
            future = preprocessor_backend.submit_mapreduce_job(mapreduce_preprocessor, self)
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

    def partition(self, strategy, *args, **kwargs) -> List[CloudObjectSlice]:
        """
        Apply partitioning strategy on this cloud object.

        :param strategy: Partitioning strategy
        :param args: Optional arguments to pass to the partitioning strategy functions
        :param kwargs: Optional key-words arguments to pass to the partitioning strategy
        """
        slices = strategy(self, *args, **kwargs)
        # Store a reference to this CloudObject instance in the slice
        for s in slices:
            s.cloud_object = self
        return slices

    def __getitem__(self, item):
        return self._attrs.__getattribute__(item)

    def __repr__(self):
        return f"{self.__class__.__name__}<{self._cls.co_class.__name__}>({self.path.as_uri()})"
