from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING, Union, IO, Any

from .backends.aws_s3 import PickleableS3ClientProxy

import boto3
import botocore.client

if TYPE_CHECKING:
    from typing import Optional

logger = logging.getLogger(__name__)


class StoragePosixFlavour(_PosixFlavour):
    is_supported = True
    prefix = "s3"

    def parse_parts(self, parts):
        drv, root, parsed = super().parse_parts(parts)
        for part in parsed[1:]:
            if part == "..":
                index = parsed.index(part)
                parsed.pop(index - 1)
                parsed.remove(part)
        return drv, root, parsed

    def make_uri(self, path):
        uri = super().make_uri(path)
        return uri.replace("file:///", f"{self.prefix}://")

    def patch_prefix(self, prefix):
        self.prefix = prefix


class StoragePath(PurePath):
    """
    PurePath subclass for object storage
    Object stores are not a file-system, but we can browse them like a POSIX system

    Source: https://github.com/liormizr/s3path
    """
    __slots__ = ()
    _flavour = StoragePosixFlavour()

    @classmethod
    def from_uri(cls, uri: str) -> StoragePath:
        """
        from_uri class method create a class instance from uri
        """
        match = re.match(r'^([a-zA-Z0-9]+)://', uri)
        if match:
            prefix = match.group(1)
        else:
            raise ValueError("Provided URI does not seem to be an URI! (e.g. s3://my-bucket/my-key is a correct URI)")

        c = cls(uri[len(prefix) + 1:])
        c._flavour.patch_prefix(prefix)
        return c

    @classmethod
    def from_storage_bucket_key(cls, storage: str, bucket: str, key: str) -> StoragePath:
        """
        from_bucket_key class method create a class instance from bucket, key pair's
        """
        bucket = cls(storage, bucket)
        if len(bucket.parts) != 2:
            raise ValueError("bucket argument contains more then one path element: {}".format(bucket))
        key = cls(storage, key)
        if key.is_absolute():
            key = key.relative_to("/")

        return bucket / key

    @property
    def storage(self) -> str:
        """
        The object storage prefix
        """
        self._absolute_path_validation()
        return self._flavour.prefix

    @property
    def bucket(self) -> str:
        """
        The object storage bucket name, or ''
        """
        self._absolute_path_validation()
        with suppress(ValueError):
            _, bucket, *_ = self.parts
            return bucket
        return ""

    @property
    def key(self) -> str:
        """
        The object storage key name, or ''
        """
        self._absolute_path_validation()
        key = self._flavour.sep.join(self.parts[2:])
        return key

    @property
    def virtual_directory(self) -> str:
        """
        The parent virtual directory of a key
        Example: foo/bar/baz -> foo/bar
        """
        vdir, _ = self.key.rsplit("/", 1)
        return vdir

    def as_uri(self) -> str:
        """
        Return the path as a 's3' URI.
        """
        return super().as_uri()

    def _absolute_path_validation(self):
        if not self.is_absolute():
            raise ValueError("relative path have no bucket, key specification")

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
