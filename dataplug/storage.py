from __future__ import annotations

import json
import logging
import time
import uuid

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
                 region_name: str, endpoint_url: str, use_token: Optional[bool] = None,
                 role_arn: Optional[str] = None, token_duration_seconds: Optional[int] = None,
                 botocore_config_kwargs: Optional[dict] = None):
        self.region_name = region_name
        self.endpoint_url = endpoint_url
        self.botocore_config_kwargs = botocore_config_kwargs or {}
        self.role_arn = role_arn
        self.session_name = None
        self.token_duration_seconds = token_duration_seconds or 86400
        use_token = use_token if use_token is not None else True

        if use_token:
            logger.debug('Using token for S3 authentication')
            sts_admin = boto3.client('sts',
                                     aws_access_key_id=aws_access_key_id,
                                     aws_secret_access_key=aws_secret_access_key,
                                     region_name=region_name,
                                     endpoint_url=endpoint_url,
                                     config=botocore.client.Config(**self.botocore_config_kwargs))

            if role_arn is not None:
                self.session_name = '-'.join(['dataplug', str(int(time.time())), uuid.uuid4().hex])
                logger.debug('Assuming role %s with generated session name %s', self.role_arn, self.session_name)

                response = sts_admin.assume_role(RoleArn=self.role_arn,
                                                 RoleSessionName=self.session_name,
                                                 Policy=S3_FULL_ACCESS_POLICY,
                                                 DurationSeconds=self.token_duration_seconds)
            else:
                logger.debug('Getting session token')
                response = sts_admin.get_session_token(DurationSeconds=self.token_duration_seconds)

            self.credentials = response['Credentials']

            self.__client = boto3.client('s3',
                                         aws_access_key_id=self.credentials['AccessKeyId'],
                                         aws_secret_access_key=self.credentials['SecretAccessKey'],
                                         aws_session_token=self.credentials['SessionToken'],
                                         endpoint_url=self.endpoint_url,
                                         region_name=self.region_name,
                                         config=botocore.client.Config(**self.botocore_config_kwargs))
        else:
            logger.warning('Using user credentials is discouraged for security reasons! '
                           'Consider using token-based authentication instead')
            self.credentials = {
                'AccessKeyId': aws_access_key_id,
                'SecretAccessKey': aws_secret_access_key
            }
            self.__client = boto3.client('s3',
                                         aws_access_key_id=self.credentials['AccessKeyId'],
                                         aws_secret_access_key=self.credentials['SecretAccessKey'],
                                         endpoint_url=self.endpoint_url,
                                         region_name=self.region_name,
                                         config=botocore.client.Config(**self.botocore_config_kwargs))

    def __getstate__(self):
        logger.debug('Pickling S3 client')
        return {
            'credentials': self.credentials,
            'endpoint_url': self.endpoint_url,
            'region_name': self.region_name,
            'botocore_config_kwargs': self.botocore_config_kwargs,
            'role_arn': self.role_arn,
            'session_name': self.session_name,
            'token_duration_seconds': self.token_duration_seconds
        }

    def __setstate__(self, state):
        logger.debug('Restoring S3 client')
        self.credentials = state['credentials']
        self.endpoint_url = state['endpoint_url']
        self.region_name = state['region_name']
        self.botocore_config_kwargs = state['botocore_config_kwargs']
        self.role_arn = state['role_arn']
        self.session_name = state['session_name']
        self.token_duration_seconds = state['token_duration_seconds']

        self.__client = boto3.client('s3',
                                     aws_access_key_id=self.credentials['AccessKeyId'],
                                     aws_secret_access_key=self.credentials['SecretAccessKey'],
                                     aws_session_token=self.credentials.get('SessionToken'),
                                     endpoint_url=self.endpoint_url,
                                     region_name=self.region_name,
                                     config=botocore.client.Config(**self.botocore_config_kwargs))

    def abort_multipart_upload(self, *args, **kwargs):
        response = self.__client.abort_multipart_upload(*args, **kwargs)
        logger.debug('%s', response.get('ResponseMetadata', {}))
        return response

    def complete_multipart_upload(self, *args, **kwargs):
        response = self.__client.complete_multipart_upload(*args, **kwargs)
        logger.debug('%s', response.get('ResponseMetadata', {}))
        return response

    def create_multipart_upload(self, *args, **kwargs):
        response = self.__client.create_multipart_upload(*args, **kwargs)
        logger.debug('%s', response.get('ResponseMetadata', {}))
        return response

    def download_file(self, *args, **kwargs):
        self.__client.download_file(*args, **kwargs)
        logger.debug('%s', {'HTTP-Status': '200'})

    def download_fileobj(self, *args, **kwargs):
        self.__client.download_fileobj(*args, **kwargs)
        logger.debug('%s', {'HTTP-Status': '200'})

    def generate_presigned_post(self, *args, **kwargs):
        response = self.__client.generate_presigned_post(*args, **kwargs)
        logger.debug('%s', response.get('ResponseMetadata', {}))
        return response

    def generate_presigned_url(self, *args, **kwargs):
        response = self.__client.generate_presigned_url(*args, **kwargs)
        logger.debug('%s', response.get('ResponseMetadata', {}))
        return response

    def get_object(self, *args, **kwargs):
        response = self.__client.get_object(*args, **kwargs)
        logger.debug('%s', response.get('ResponseMetadata', {}))
        return response

    def head_bucket(self, *args, **kwargs):
        response = self.__client.head_bucket(*args, **kwargs)
        logger.debug('%s', response.get('ResponseMetadata', {}))
        return response

    def head_object(self, *args, **kwargs):
        response = self.__client.head_object(*args, **kwargs)
        logger.debug('%s', response.get('ResponseMetadata', {}))
        return response

    def list_buckets(self):
        response = self.__client.list_buckets()
        logger.debug('%s', response.get('ResponseMetadata', {}))
        return response

    def list_multipart_uploads(self, *args, **kwargs):
        response = self.__client.list_multipart_uploads(*args, **kwargs)
        logger.debug('%s', response.get('ResponseMetadata', {}))
        return response

    def list_objects(self, *args, **kwargs):
        response = self.__client.list_objects(*args, **kwargs)
        logger.debug('%s', response.get('ResponseMetadata', {}))
        return response

    def list_objects_v2(self, *args, **kwargs):
        response = self.__client.list_objects_v2(*args, **kwargs)
        logger.debug('%s', response.get('ResponseMetadata', {}))
        return response

    def list_parts(self, *args, **kwargs):
        response = self.__client.list_parts(*args, **kwargs)
        logger.debug('%s', response.get('ResponseMetadata', {}))
        return response

    def put_object(self, *args, **kwargs):
        response = self.__client.put_object(*args, **kwargs)
        logger.debug('%s', response.get('ResponseMetadata', {}))
        return response

    def upload_file(self, *args, **kwargs):
        self.__client.upload_file(*args, **kwargs)
        logger.debug('%s', {'HTTP-Status': '200'})

    def upload_fileobj(self, *args, **kwargs):
        self.__client.upload_fileobj(*args, **kwargs)
        logger.debug('%s', {'HTTP-Status': '200'})

    def upload_part(self, *args, **kwargs):
        response = self.__client.upload_part(*args, **kwargs)
        logger.debug('%s', response.get('ResponseMetadata', {}))
        return response


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

    def __repr__(self) -> str:
        return '{}(bucket={},key={})'.format(self.__class__.__name__, self.bucket, self.key)
