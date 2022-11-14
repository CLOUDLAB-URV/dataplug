from __future__ import annotations

import json
import logging

from typing import TYPE_CHECKING, Optional, Union, IO, Any, Literal, Dict, Callable, Mapping, List
from contextlib import suppress
from pathlib import _PosixFlavour, PurePath

import boto3
import botocore.client

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
                "Resource": [
                    "arn:aws:s3:::*",
                    "arn:aws:s3:::*/*"
                ],
                "Principal": "*"
            }
        ]
    }
)


class PickleableS3ClientProxy:
    def __init__(self, aws_access_key_id: str, aws_secret_access_key: str,
                 region_name: str, endpoint_url: str, use_token: Optional[bool] = True,
                 config_kwargs: Optional[dict] = None):
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.config_kwargs = config_kwargs or {}

        if use_token:
            sts_admin = boto3.client('sts',
                                     aws_access_key_id=aws_access_key_id,
                                     aws_secret_access_key=aws_secret_access_key,
                                     region_name=region_name,
                                     endpoint_url=endpoint_url,
                                     config=botocore.client.Config(**self.config_kwargs))

            response = sts_admin.assume_role(RoleArn='arn:x:ignored:by:minio:',
                                             RoleSessionName='ignored-by-minio',
                                             Policy=S3_FULL_ACCESS_POLICY,
                                             DurationSeconds=86400)

            self.credentials = response['Credentials']

            self.client = boto3.client('s3',
                                       aws_access_key_id=self.credentials['AccessKeyId'],
                                       aws_secret_access_key=self.credentials['SecretAccessKey'],
                                       aws_session_token=self.credentials['SessionToken'],
                                       endpoint_url=self.endpoint_url,
                                       region_name=self.region_name,
                                       config=botocore.client.Config(**self.config_kwargs))
        else:
            logger.warning('Using user credentials is discouraged for security reasons -'
                           ' consider using token-based authentication instead')
            self.credentials = {
                'AccessKeyId': aws_access_key_id,
                'SecretAccessKey': aws_secret_access_key
            }
            self.client = boto3.client('s3',
                                       aws_access_key_id=self.credentials['AccessKeyId'],
                                       aws_secret_access_key=self.credentials['SecretAccessKey'],
                                       endpoint_url=self.endpoint_url,
                                       region_name=self.region_name)

    def __getstate__(self):
        return {
            'credentials': self.credentials,
            'endpoint_url': self.endpoint_url,
            'region_name': self.region_name
        }

    def __setstate__(self, state):
        self.credentials = state['credentials']
        self.endpoint_url = state['endpoint_url']
        self.region_name = state['region_name']

        self.client = boto3.client('s3',
                                   aws_access_key_id=self.credentials['AccessKeyId'],
                                   aws_secret_access_key=self.credentials['SecretAccessKey'],
                                   aws_session_token=self.credentials.get('SessionToken'),
                                   endpoint_url=self.endpoint_url,
                                   region_name=self.region_name)

    def abort_multipart_upload(self, *args, **kwargs):
        return self.client.abort_multipart_upload(*args, **kwargs)

    def complete_multipart_upload(self, *args, **kwargs):
        return self.client.complete_multipart_upload(*args, **kwargs)

    def create_multipart_upload(self, *args, **kwargs):
        return self.client.create_multipart_upload(*args, **kwargs)

    def download_file(self, *args, **kwargs):
        self.client.download_file(*args, **kwargs)

    def download_fileobj(self, *args, **kwargs):
        self.client.download_fileobj(*args, **kwargs)

    def generate_presigned_post(self, *args, **kwargs):
        return self.client.generate_presigned_post(*args, **kwargs)

    def generate_presigned_url(self, *args, **kwargs):
        return self.client.generate_presigned_url(*args, **kwargs)

    def get_object(self, *args, **kwargs):
        return self.client.get_object(*args, **kwargs)

    def head_bucket(self, *args, **kwargs):
        return self.client.head_bucket(*args, **kwargs)

    def head_object(self, *args, **kwargs):
        return self.client.head_object(*args, **kwargs)

    def list_buckets(self):
        return self.client.list_buckets()

    def list_multipart_uploads(self, *args, **kwargs):
        return self.client.list_multipart_uploads(*args, **kwargs)

    def list_objects(self, *args, **kwargs):
        return self.client.list_objects(*args, **kwargs)

    def list_objects_v2(self, *args, **kwargs):
        return self.client.list_objects_v2(*args, **kwargs)

    def list_parts(self, *args, **kwargs):
        return self.client.list_parts(*args, **kwargs)

    def put_object(self, *args, **kwargs):
        return self.client.put_object(*args, **kwargs)

    def upload_file(self, *args, **kwargs):
        self.client.upload_file(*args, **kwargs)

    def upload_fileobj(self, *args, **kwargs):
        self.client.upload_fileobj(*args, **kwargs)

    def upload_part(self, *args, **kwargs):
        return self.client.upload_part(*args, **kwargs)


class _S3Flavour(_PosixFlavour):
    is_supported = True

    def parse_parts(self, parts):
        drv, root, parsed = super().parse_parts(parts)
        for part in parsed[1:]:
            if part == '..':
                index = parsed.index(part)
                parsed.pop(index - 1)
                parsed.remove(part)
        return drv, root, parsed

    def make_uri(self, path):
        uri = super().make_uri(path)
        return uri.replace('file:///', 's3://')


class PureS3Path(PurePath):
    """
    PurePath subclass for AWS S3 service.
    Source: https://github.com/liormizr/s3path
    S3 is not a file-system but we can look at it like a POSIX system.
    """
    _flavour = _S3Flavour()
    __slots__ = ()

    @classmethod
    def from_uri(cls, uri: str) -> 'PureS3Path':
        """
        from_uri class method create a class instance from url

        >> from s3path import PureS3Path
        >> PureS3Path.from_url('s3://<bucket>/<key>')
        << PureS3Path('/<bucket>/<key>')
        """
        if not uri.startswith('s3://'):
            raise ValueError('Provided uri seems to be no S3 URI!')
        return cls(uri[4:])

    @classmethod
    def from_bucket_key(cls, bucket: str, key: str) -> 'PureS3Path':
        """
        from_bucket_key class method create a class instance from bucket, key pair's

        >> from s3path import PureS3Path
        >> PureS3Path.from_bucket_key(bucket='<bucket>', key='<key>')
        << PureS3Path('/<bucket>/<key>')
        """
        bucket = cls(cls._flavour.sep, bucket)
        if len(bucket.parts) != 2:
            raise ValueError('bucket argument contains more then one path element: {}'.format(bucket))
        key = cls(key)
        if key.is_absolute():
            key = key.relative_to('/')
        return bucket / key

    @property
    def bucket(self) -> str:
        """
        The AWS S3 Bucket name, or ''
        """
        self._absolute_path_validation()
        with suppress(ValueError):
            _, bucket, *_ = self.parts
            return bucket
        return ''

    @property
    def key(self) -> str:
        """
        The AWS S3 Key name, or ''
        """
        self._absolute_path_validation()
        key = self._flavour.sep.join(self.parts[2:])
        return key

    @property
    def virtual_directory(self) -> str:
        """
        The parent virtual directory of a key
        Example: foo/bar/baz -> foo/baz
        """
        vdir, _ = self.key.rsplit('/', 1)
        return vdir

    def as_uri(self) -> str:
        """
        Return the path as a 's3' URI.
        """
        return super().as_uri()

    def _absolute_path_validation(self):
        if not self.is_absolute():
            raise ValueError('relative path have no bucket, key specification')
