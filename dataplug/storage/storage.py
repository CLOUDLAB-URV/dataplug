from __future__ import annotations

import importlib
import logging
from typing import TYPE_CHECKING, Union, IO, Any

from ..util import NoPublicConstructor

if TYPE_CHECKING:
    from botocore.response import StreamingBody
    from mypy_boto3_s3.type_defs import (
        AbortMultipartUploadOutputTypeDef,
        CreateBucketOutputTypeDef,
        CreateMultipartUploadOutputTypeDef,
        CompleteMultipartUploadOutputTypeDef,
        DeleteObjectOutputTypeDef,
        DeleteObjectsOutputTypeDef,
        GetObjectOutputTypeDef,
        HeadObjectOutputTypeDef,
        ListBucketsOutputTypeDef,
        ListMultipartUploadsOutputTypeDef,
        ListObjectsOutputTypeDef,
        ListObjectsV2OutputTypeDef,
        ListPartsOutputTypeDef,
        PutObjectOutputTypeDef,
        UploadPartOutputTypeDef,
        EmptyResponseMetadataTypeDef,
        DeleteTypeDef,
    )

logger = logging.getLogger(__name__)

STORAGE_BACKENDS = {
    "s3": "dataplug.storage.backends.aws_s3.PickleableS3Client",
    "file": "dataplug.storage.backends.filesystem.PosixFileSystemClient",
}


class StoragePath(metaclass=NoPublicConstructor):
    def __init__(self, prefix: str, bucket: str, key: str):
        self.__prefix = prefix
        self.__bucket = bucket
        self.__key = key

    @classmethod
    def from_bucket_key(cls, prefix: str, bucket: str, key: str) -> StoragePath:
        """
        from_bucket_key class method create a class instance from bucket, key pair's
        """
        return cls._create(prefix, bucket, key)

    @property
    def storage(self) -> str:
        """
        The object storage prefix
        """
        return self.__prefix

    @property
    def bucket(self) -> str:
        """
        The object storage bucket name, or ''
        """
        return self.__bucket

    @property
    def key(self) -> str:
        """
        The object storage key name, or ''
        """
        return self.__key

    def as_uri(self) -> str:
        """
        Return the path as a 's3' URI.
        """
        return "{}://{}/{}".format(self.storage, self.bucket, self.key)

    def __repr__(self) -> str:
        return "{}(bucket={},key={})".format(self.__class__.__name__, self.bucket, self.key)


class S3ObjectStorage:
    """
    Amazon S3 API interface for abstracting the storage backend
    """

    def _parse_full_path(self, path: str) -> StoragePath:
        raise NotImplementedError()

    def _open_as_file(self, Bucket: str, Key: str, *args, **kwargs) -> IO[Any]:
        raise NotImplementedError()

    def abort_multipart_upload(
            self, Bucket: str, Key: str, UploadId: str, *args, **kwargs
    ) -> AbortMultipartUploadOutputTypeDef:
        raise NotImplementedError()

    def complete_multipart_upload(
            self, Bucket: str, Key: str, UploadId: str, *args, **kwargs
    ) -> CompleteMultipartUploadOutputTypeDef:
        raise NotImplementedError()

    def create_bucket(self, Bucket: str, *args, **kwargs) -> CreateBucketOutputTypeDef:
        raise NotImplementedError()

    def create_multipart_upload(self, Bucket: str, Key: str, *args, **kwargs) -> CreateMultipartUploadOutputTypeDef:
        raise NotImplementedError()

    def delete_object(self, Bucket: str, Key: str, *args, **kwargs) -> DeleteObjectOutputTypeDef:
        raise NotImplementedError()

    def delete_objects(self, Bucket: str, Delete: DeleteTypeDef, *args, **kwargs) -> DeleteObjectsOutputTypeDef:
        raise NotImplementedError()

    def download_file(self, Bucket: str, Key: str, Filename: str, *args, **kwargs) -> None:
        raise NotImplementedError()

    def download_fileobj(self, Bucket: str, Key: str, Fileobj: Union[IO[Any], StreamingBody], *args, **kwargs) -> None:
        raise NotImplementedError()

    def get_object(self, Bucket: str, Key: str, *args, **kwargs) -> GetObjectOutputTypeDef:
        raise NotImplementedError()

    def head_bucket(self, Bucket: str, *args, **kwargs) -> EmptyResponseMetadataTypeDef:
        raise NotImplementedError()

    def head_object(self, Bucket: str, Key: str, *args, **kwargs) -> HeadObjectOutputTypeDef:
        raise NotImplementedError()

    def list_buckets(self) -> ListBucketsOutputTypeDef:
        raise NotImplementedError()

    def list_multipart_uploads(self, Bucket: str, *args, **kwargs) -> ListMultipartUploadsOutputTypeDef:
        raise NotImplementedError()

    def list_objects(self, Bucket: str, *args, **kwargs) -> ListObjectsOutputTypeDef:
        raise NotImplementedError()

    def list_objects_v2(self, Bucket: str, *args, **kwargs) -> ListObjectsV2OutputTypeDef:
        raise NotImplementedError()

    def list_parts(self, Bucket: str, Key: str, UploadId: str, *args, **kwargs) -> ListPartsOutputTypeDef:
        raise NotImplementedError()

    def put_object(self, Bucket: str, Key: str, *args, **kwargs) -> PutObjectOutputTypeDef:
        raise NotImplementedError()

    def upload_file(self, Bucket: str, Key: str, Filename: str, *args, **kwargs) -> None:
        raise NotImplementedError()

    def upload_fileobj(self, Bucket: str, Key: str, Fileobj: Union[IO[Any], StreamingBody], *args, **kwargs) -> None:
        raise NotImplementedError()

    def upload_part(
            self, Bucket: str, Key: str, PartNumber: int, UploadId: str, *args, **kwargs
    ) -> UploadPartOutputTypeDef:
        raise NotImplementedError()


def create_client(prefix, storage_config):
    """
    Create a client for the specified prefix
    """
    if prefix not in STORAGE_BACKENDS:
        raise ValueError(f"Invalid storage prefix: {prefix}")

    module_name, class_name = STORAGE_BACKENDS[prefix].rsplit(".", 1)
    module = importlib.import_module(module_name)
    storage_backend = getattr(module, class_name)
    print(storage_config)
    return storage_backend(**storage_config)
