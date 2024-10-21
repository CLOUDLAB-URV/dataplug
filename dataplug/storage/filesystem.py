from __future__ import annotations

import io
import os
import pathlib
import shutil

import botocore
from typing import TYPE_CHECKING, Union, IO, Any
from botocore.response import StreamingBody


if TYPE_CHECKING:
    from mypy_boto3_s3.type_defs import DeleteTypeDef


class FileSystemS3API:
    def __init__(self):
        self.__file_handles = {}

    def __del__(self):
        for path, fh in self.__file_handles.items():
            fh.close()

    def _build_path(self, Bucket: str, Key: str) -> pathlib.Path:
        return pathlib.Path(Bucket).joinpath(Key)

    def abort_multipart_upload(
        self, Bucket: str, Key: str, UploadId: str, *args, **kwargs
    ):
        raise NotImplementedError()

    def complete_multipart_upload(
        self, Bucket: str, Key: str, UploadId: str, *args, **kwargs
    ):
        raise NotImplementedError()

    def create_bucket(self, Bucket: str, *args, **kwargs):
        pathlib.Path(Bucket).mkdir(parents=True, exist_ok=True)

    def create_multipart_upload(self, Bucket: str, Key: str, *args, **kwargs):
        raise NotImplementedError()

    def delete_object(self, Bucket: str, Key: str, *args, **kwargs):
        path = self._build_path(Bucket, Key)
        if path.exists() and path.is_file():
            path.unlink()
        else:
            raise botocore.exceptions.ClientError(
                error_response={"Error": {"Code": "404"}}, operation_name="DeleteObject"
            )

    def delete_objects(self, Bucket: str, Delete: DeleteTypeDef, *args, **kwargs):
        for obj in Delete["Objects"]:
            self.delete_object(Bucket, obj["Key"])

    def download_file(self, Bucket: str, Key: str, Filename: str, *args, **kwargs):
        with self._open_as_file(Bucket, Key, "rb") as f1:
            with open(Filename, "wb") as f2:
                shutil.copyfileobj(f1, f2)

    def download_fileobj(
        self,
        Bucket: str,
        Key: str,
        Fileobj: Union[IO[Any], StreamingBody],
        *args,
        **kwargs,
    ):
        with self._open_as_file(Bucket, Key, "rb") as f:
            shutil.copyfileobj(f, Fileobj)

    def get_object(self, Bucket: str, Key: str, *args, **kwargs):
        path = self._build_path(Bucket, Key)
        if path.exists() and path.is_file():
            size = path.stat().st_size
            if "Range" in kwargs:
                range_0, range_1 = kwargs["Range"].replace("bytes=", "").split("-")
                range_0, range_1 = int(range_0), int(range_1) + 1
                with path.open("rb") as f:
                    # Read all chunk into memory, as we could seek after the intended range if we just open the file
                    f.seek(range_0)
                    chunk = f.read(range_1 - range_0)
                buff = io.BytesIO(chunk)
                return {
                    "Body": StreamingBody(
                        raw_stream=buff, content_length=range_1 - range_0
                    ),
                    "ContentLength": range_1 - range_0,
                    "ResponseMetadata": {"HTTPStatusCode": 206},
                }
            else:
                return {
                    "Body": StreamingBody(
                        raw_stream=path.open("rb"), content_length=size
                    ),
                    "ContentLength": size,
                    "ResponseMetadata": {"HTTPStatusCode": 200},
                }
        else:
            raise botocore.exceptions.ClientError(
                error_response={"Error": {"Code": "404"}}, operation_name="GetObject"
            )

    def head_bucket(self, Bucket: str, *args, **kwargs):
        # check if directory exists
        path = pathlib.Path(Bucket)
        if path.exists() and path.is_dir():
            return {}
        else:
            raise botocore.exceptions.ClientError(
                error_response={"Error": {"Code": "404"}}, operation_name="HeadBucket"
            )

    def head_object(self, Bucket: str, Key: str, *args, **kwargs):
        path = pathlib.Path(os.path.join(Bucket, Key))
        if path.exists() and path.is_file():
            size = path.stat().st_size
            return {"ContentLength": size}
        else:
            raise botocore.exceptions.ClientError(
                error_response={"Error": {"Code": "404"}}, operation_name="HeadObject"
            )

    def list_buckets(self):
        raise NotImplementedError()

    def list_multipart_uploads(self, Bucket: str, *args, **kwargs):
        raise NotImplementedError()

    def list_objects(self, Bucket: str, *args, **kwargs):
        prefix = kwargs.get("Prefix", "")
        path = self._build_path(Bucket, prefix)

        if path.exists() and path.is_file():
            return {"Contents": [{"Key": path.relative_to(path.parent).as_posix()}]}
        if path.exists() and path.is_dir():
            return {
                "Contents": [
                    {"Key": p.relative_to(path).as_posix()}
                    for p in path.glob("**/*")
                    if p.is_file()
                ]
            }
        else:
            # List parent if it is an incomplete path
            parent = path.parent
            keys = []
            for p in parent.glob("**/*"):
                if p.is_file() and p.relative_to(parent).as_posix().startswith(prefix):
                    keys.append(p.as_posix())
            return {"Contents": [{"Key": k} for k in keys]}

    def list_objects_v2(self, Bucket: str, *args, **kwargs):
        return self.list_objects(Bucket, *args, **kwargs)

    def list_parts(self, Bucket: str, Key: str, UploadId: str, *args, **kwargs):
        raise NotImplementedError()

    def put_object(self, Bucket: str, Key: str, *args, **kwargs):
        if "Body" in kwargs:
            path = self._build_path(Bucket, Key)
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("wb") as f:
                if hasattr(kwargs["Body"], "read"):
                    shutil.copyfileobj(kwargs["Body"], f)
                else:
                    f.write(kwargs["Body"])
        else:
            path = self._build_path(Bucket, Key)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()

    def upload_file(
        self, Bucket: str, Key: str, Filename: str, *args, **kwargs
    ) -> None:
        with open(Filename, "rb") as f:
            self.put_object(Bucket, Key, Body=f, *args, **kwargs)

    def upload_fileobj(
        self,
        Bucket: str,
        Key: str,
        Fileobj: Union[IO[Any], StreamingBody],
        *args,
        **kwargs,
    ) -> None:
        self.put_object(Bucket, Key, Body=Fileobj, *args, **kwargs)

    def upload_part(
        self, Bucket: str, Key: str, PartNumber: int, UploadId: str, *args, **kwargs
    ):
        raise NotImplementedError()


class FilePath:
    """Virtual bucket-key pair that links to a local FS path."""

    def __init__(self, bucket_path, key_path):
        self._bucket_path = bucket_path
        self._key_path = key_path

    @classmethod
    def from_bucket_key(cls, bucket: str, key: str) -> "FilePath":
        bucket_path = pathlib.PurePath(bucket)
        key_path = pathlib.PurePath(key)
        if key_path.is_absolute():
            key_path = key_path.relative_to(bucket_path)
        path = cls(bucket_path, key_path)
        return path

    @property
    def bucket(self) -> str:
        """
        The virtual Bucket name
        """
        return self._bucket_path

    @property
    def key(self) -> str:
        """
        The virtual Key name
        """
        return self._key_path

    @property
    def full(self) -> str:
        """
        The full path name
        """
        return self._bucket_path / self._key_path

    # @property
    # def virtual_directory(self) -> str:
    #     """
    #     The parent virtual directory of a key
    #     Example: foo/bar/baz -> foo/baz
    #     """
    #     vdir, _ = self.key.rsplit("/", 1)
    #     return vdir

    def as_uri(self) -> str:
        """
        Return the path as a URI.
        """
        return self.full.as_uri()

    # def _absolute_path_validation(self):
    #     if not self.is_absolute():
    #         raise ValueError("relative path have no bucket, key specification")

    def __repr__(self) -> str:
        return "{}(bucket={},key={})".format(
            self.__class__.__name__, self.bucket, self.key
        )
