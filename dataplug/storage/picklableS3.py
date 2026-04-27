from __future__ import annotations

import json
import logging
import posixpath
import time
import uuid
from typing import TYPE_CHECKING

import boto3
import botocore.client

if TYPE_CHECKING:
    from typing import Optional
    from mypy_boto3_s3.type_defs import DeleteObjectOutputTypeDef, DeleteObjectsOutputTypeDef

logger = logging.getLogger(__name__)

S3_FULL_ACCESS_POLICY = json.dumps(
    {
        "Id": "BucketPolicy",
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllAccess",
                "Action": "s3:*",
                "Effect": "Allow",
                "Resource": ["arn:aws:s3:::*", "arn:aws:s3:::*/*"],
                "Principal": "*",
            }
        ],
    }
)


class PickleableS3ClientProxy:
    """
    A Pickleable S3 client proxy that can be pickled and unpickled.

    Dataplug requires having an S3 client that can be pickled and sent remotely to workers, so that
    remote workers can access S3 objects. This class is a proxy to an S3 client that can be pickled.
    For it, we request temporary credentials to S3 using STS. To request temporary credentials, we
    authenticate using the locally configured credentials (through the environment variables or the
    AWS configuration file) and assume a role with full access to S3. The temporary credentials are
    saved in the instance and used to create an S3 client, and to create a new client when the object
    is unpickled.
    """

    def __init__(
            self,
            region_name: Optional[str] = None,
            endpoint_url: Optional[str] = None,
            credentials: Optional[dict] = None,
            role_arn: Optional[str] = None,
            token_duration_seconds: Optional[int] = None,
            botocore_config_kwargs: Optional[dict] = None,
    ):
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.botocore_config_kwargs = botocore_config_kwargs or {}
        self.role_arn = role_arn
        self.session_name = None
        self.token_duration_seconds = token_duration_seconds or 86400  # 24 hours

        logger.debug("Requesting temporary credentials for S3 authentication")
        sts_admin = boto3.client(
            "sts",
            region_name=region_name,
            endpoint_url=endpoint_url,
            config=botocore.client.Config(**self.botocore_config_kwargs),
        )

        if credentials:
            # check if credentials are valid
            if not all(
                    key in credentials for key in ["AccessKeyId", "SecretAccessKey"]
            ):
                raise ValueError(
                    "Invalid credentials. AccessKeyId and SecretAccessKey are required if credentials are provided."
                )
            self.credentials = credentials

        elif role_arn is not None:
            self.session_name = "-".join(
                ["dataplug", str(int(time.time())), uuid.uuid4().hex]
            )
            logger.debug(
                "Assuming role %s with generated session name %s",
                self.role_arn,
                self.session_name,
            )

            response = sts_admin.assume_role(
                RoleArn=self.role_arn,
                RoleSessionName=self.session_name,
                Policy=S3_FULL_ACCESS_POLICY,
                DurationSeconds=self.token_duration_seconds,
            )
            self.credentials = response["Credentials"]

        else:
            logger.debug("Getting session token")
            response = sts_admin.get_session_token(
                DurationSeconds=self.token_duration_seconds
            )
            self.credentials = response["Credentials"]

        self.__client = boto3.client(
            "s3",
            aws_access_key_id=self.credentials["AccessKeyId"],
            aws_secret_access_key=self.credentials["SecretAccessKey"],
            aws_session_token=self.credentials.get("SessionToken"),
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            config=botocore.client.Config(**self.botocore_config_kwargs),
        )

    def _new_client(self):
        session = boto3.Session(
            aws_access_key_id=self.credentials["AccessKeyId"],
            aws_secret_access_key=self.credentials["SecretAccessKey"],
            aws_session_token=self.credentials.get("SessionToken"),
            region_name=self.region_name,
        )
        return session.client(
            "s3",
            endpoint_url=self.endpoint_url,
            config=botocore.client.Config(**self.botocore_config_kwargs),
        )

    def __getstate__(self):
        logger.debug("Pickling S3 client")
        return {
            "credentials": self.credentials,
            "endpoint_url": self.endpoint_url,
            "region_name": self.region_name,
            "botocore_config_kwargs": self.botocore_config_kwargs,
            "role_arn": self.role_arn,
            "session_name": self.session_name,
            "token_duration_seconds": self.token_duration_seconds,
        }

    def __setstate__(self, state):
        logger.debug("Restoring S3 client")
        self.credentials = state["credentials"]
        self.endpoint_url = state["endpoint_url"]
        self.region_name = state["region_name"]
        self.botocore_config_kwargs = state["botocore_config_kwargs"]
        self.role_arn = state["role_arn"]
        self.session_name = state["session_name"]
        self.token_duration_seconds = state["token_duration_seconds"]

        self.__client = boto3.client(
            "s3",
            aws_access_key_id=self.credentials["AccessKeyId"],
            aws_secret_access_key=self.credentials["SecretAccessKey"],
            aws_session_token=self.credentials.get("SessionToken"),
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            config=botocore.client.Config(**self.botocore_config_kwargs),
        )

    def abort_multipart_upload(self, *args, **kwargs):
        response = self.__client.abort_multipart_upload(*args, **kwargs)
        logger.debug("%s", response.get("ResponseMetadata", {}))
        return response

    def complete_multipart_upload(self, *args, **kwargs):
        response = self.__client.complete_multipart_upload(*args, **kwargs)
        logger.debug("%s", response.get("ResponseMetadata", {}))
        return response

    def create_multipart_upload(self, *args, **kwargs):
        response = self.__client.create_multipart_upload(*args, **kwargs)
        logger.debug("%s", response.get("ResponseMetadata", {}))
        return response

    def download_file(self, *args, **kwargs):
        self.__client.download_file(*args, **kwargs)
        logger.debug("%s", {"HTTP-Status": "200"})

    def download_fileobj(self, *args, **kwargs):
        self.__client.download_fileobj(*args, **kwargs)
        logger.debug("%s", {"HTTP-Status": "200"})

    def generate_presigned_post(self, *args, **kwargs):
        response = self.__client.generate_presigned_post(*args, **kwargs)
        logger.debug("%s", response.get("ResponseMetadata", {}))
        return response

    def generate_presigned_url(self, *args, **kwargs):
        response = self.__client.generate_presigned_url(*args, **kwargs)
        logger.debug("%s", response)
        return response

    def get_object(self, *args, **kwargs):
        response = self.__client.get_object(*args, **kwargs)
        logger.debug("%s", response.get("ResponseMetadata", {}))
        return response

    def delete_object(self, *args, **kwargs) -> DeleteObjectOutputTypeDef:
        response = self.__client.delete_object(*args, **kwargs)
        logger.debug("%s", response.get("ResponseMetadata", {}))
        return response

    def delete_objects(self, *args, **kwargs) -> DeleteObjectsOutputTypeDef:
        response = self.__client.delete_objects(*args, **kwargs)
        logger.debug("%s", response.get("ResponseMetadata", {}))
        return response

    def head_bucket(self, *args, **kwargs):
        response = self.__client.head_bucket(*args, **kwargs)
        logger.debug("%s", response.get("ResponseMetadata", {}))
        return response

    def head_object(self, *args, **kwargs):
        response = self.__client.head_object(*args, **kwargs)
        logger.debug("%s", response.get("ResponseMetadata", {}))
        return response

    def list_buckets(self):
        response = self.__client.list_buckets()
        logger.debug("%s", response.get("ResponseMetadata", {}))
        return response

    def list_multipart_uploads(self, *args, **kwargs):
        response = self.__client.list_multipart_uploads(*args, **kwargs)
        logger.debug("%s", response.get("ResponseMetadata", {}))
        return response

    def list_objects(self, *args, **kwargs):
        response = self.__client.list_objects(*args, **kwargs)
        logger.debug("%s", response.get("ResponseMetadata", {}))
        return response

    def list_objects_v2(self, *args, **kwargs):
        response = self.__client.list_objects_v2(*args, **kwargs)
        logger.debug("%s", response.get("ResponseMetadata", {}))
        return response

    def list_parts(self, *args, **kwargs):
        response = self.__client.list_parts(*args, **kwargs)
        logger.debug("%s", response.get("ResponseMetadata", {}))
        return response

    def put_object(self, *args, **kwargs):
        response = self.__client.put_object(*args, **kwargs)
        logger.debug("%s", response.get("ResponseMetadata", {}))
        return response

    def upload_file(self, *args, **kwargs):
        self.__client.upload_file(*args, **kwargs)
        logger.debug("%s", {"HTTP-Status": "200"})

    def upload_fileobj(self, *args, **kwargs):
        self.__client.upload_fileobj(*args, **kwargs)
        logger.debug("%s", {"HTTP-Status": "200"})

    def upload_part(self, *args, **kwargs):
        response = self.__client.upload_part(*args, **kwargs)
        logger.debug("%s", response.get("ResponseMetadata", {}))
        return response

    def create_bucket(self, *args, **kwargs):
        response = self.__client.create_bucket(*args, **kwargs)
        logger.debug("%s", response.get("ResponseMetadata", {}))
        return response


class S3Path:
    """
    Path-like class for AWS S3 URIs.
    Source: https://github.com/liormizr/s3path
    S3 is not a file-system but we can look at it like a POSIX system.

    This implementation uses the standard-library ``posixpath`` module for
    all joining and normalisation, avoiding any dependency on ``pathlib``
    internals that break across Python versions.
    """

    _sep = "/"
    __slots__ = ("_path",)

    def __init__(self, *args):
        segments = []
        for arg in args:
            if isinstance(arg, S3Path):
                segments.append(arg._path)
            else:
                segments.append(str(arg))
        self._path = posixpath.normpath(posixpath.join(*segments))

    @classmethod
    def from_uri(cls, uri: str) -> "S3Path":
        """
        from_uri class method create a class instance from url

        >> from s3path import PureS3Path
        >> PureS3Path.from_url('s3://<bucket>/<key>')
        << PureS3Path('/<bucket>/<key>')
        """
        if not uri.startswith("s3://"):
            raise ValueError("Provided uri seems to be no S3 URI!")
        return cls(uri[4:])

    @classmethod
    def from_bucket_key(cls, bucket: str, key: str) -> "S3Path":
        """
        from_bucket_key class method create a class instance from bucket, key pair's

        >> from s3path import PureS3Path
        >> PureS3Path.from_bucket_key(bucket='<bucket>', key='<key>')
        << PureS3Path('/<bucket>/<key>')
        """
        bucket = cls(cls._sep, bucket)
        if len(bucket.parts) != 2:
            raise ValueError(
                "bucket argument contains more then one path element: {}".format(bucket)
            )
        key = cls(key)
        if key.is_absolute():
            key = key.relative_to("/")
        return bucket / key

    @property
    def parts(self):
        if self._path == self._sep:
            return (self._sep,)
        if self._path.startswith(self._sep):
            split = self._path.split(self._sep)
            return (self._sep,) + tuple(p for p in split[1:] if p)
        return tuple(p for p in self._path.split(self._sep) if p)

    def is_absolute(self):
        return self._path.startswith(self._sep)

    @property
    def bucket(self) -> str:
        """
        The AWS S3 Bucket name, or ''
        """
        self._absolute_path_validation()
        parts = self.parts
        return parts[1] if len(parts) > 1 else ""

    @property
    def key(self) -> str:
        """
        The AWS S3 Key name, or ''
        """
        self._absolute_path_validation()
        parts = self.parts
        return self._sep.join(parts[2:]) if len(parts) > 2 else ""

    @property
    def virtual_directory(self) -> str:
        """
        The parent virtual directory of a key
        Example: foo/bar/baz -> foo/baz
        """
        vdir, _ = self.key.rsplit("/", 1)
        return vdir

    def as_uri(self) -> str:
        """
        Return the path as a 's3' URI.
        """
        if not self.is_absolute():
            raise ValueError("relative path can't be expressed as a file URI")
        return f"s3://{self.bucket}/{self.key}"

    def _absolute_path_validation(self):
        if not self.is_absolute():
            raise ValueError("relative path have no bucket, key specification")

    def relative_to(self, other):
        other = self.__class__(other) if not isinstance(other, self.__class__) else other
        if not other.is_absolute():
            other = self.__class__(self._sep, str(other).lstrip(self._sep))

        self_parts = list(self.parts)
        other_parts = list(other.parts)

        if not self_parts[: len(other_parts)] == other_parts:
            raise ValueError(f"{str(self)!r} is not in the subpath of {str(other)!r}")

        rel_parts = self_parts[len(other_parts) :]
        if not rel_parts:
            return self.__class__(".")
        return self.__class__(self._sep.join(rel_parts))

    def __truediv__(self, other):
        if isinstance(other, S3Path):
            return self.__class__(posixpath.join(self._path, other._path))
        return self.__class__(posixpath.join(self._path, other))

    def __str__(self):
        return self._path

    def __repr__(self) -> str:
        return "{}(bucket={},key={})".format(
            self.__class__.__name__, self.bucket, self.key
        )

    def __eq__(self, other):
        if isinstance(other, S3Path):
            return self._path == other._path
        return NotImplemented

    def __hash__(self):
        return hash(self._path)
