from __future__ import annotations

import json
import logging
import re
import time
import uuid

from typing import TYPE_CHECKING
from contextlib import suppress
from pathlib import PurePath, _PosixFlavour

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
