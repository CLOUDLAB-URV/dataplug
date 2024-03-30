from __future__ import annotations

import logging
import re
import os
import pickle
import shutil

import botocore

from typing import Type, Any, TypeVar, TYPE_CHECKING

if TYPE_CHECKING:
    from dataplug import CloudObject

logger = logging.getLogger(__name__)

S3_PATH_REGEX = re.compile(r"^\w+://.+/.+$")

T = TypeVar("T")


class NoPublicConstructor(type):
    """
    Metaclass that ensures a private constructor
    If a class uses this metaclass like this:

        class SomeClass(metaclass=NoPublicConstructor):
            pass

    If you try to instantiate your class (`SomeClass()`),
    a `TypeError` will be thrown.
    Source: https://stackoverflow.com/questions/8212053/private-constructor-in-python
    """

    def __call__(cls, *args, **kwargs):
        raise TypeError(f"{cls.__module__}.{cls.__qualname__} has no public constructor")

    def _create(cls: Type[T], *args: Any, **kwargs: Any) -> T:
        return super().__call__(*args, **kwargs)  # type: ignore


def setup_logging(level=logging.INFO):
    root_logger = logging.getLogger("dataplug")
    root_logger.setLevel(level)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s [%(name)s.%(funcName)s:%(lineno)d] %(message)s")
    ch.setFormatter(formatter)
    root_logger.addHandler(ch)


def split_s3_path(path):
    if not S3_PATH_REGEX.fullmatch(path):
        raise ValueError(f"Path must satisfy regex {S3_PATH_REGEX}")

    bucket, key = path.replace("s3://", "").split("/", 1)
    return bucket, key


def force_delete_path(path):
    if os.path.exists(path):
        if os.path.isfile(path):
            os.remove(path)
        elif os.path.isdir(path):
            shutil.rmtree(path)


def fully_qualified_name(thing):
    if thing is not None:
        return thing.__module__ + "." + thing.__qualname__
    else:
        return None


def head_object(s3client, bucket, key):
    try:
        head_res = s3client.head_object(Bucket=bucket, Key=key)
        if "ResponseMetadata" in head_res:
            del head_res["ResponseMetadata"]
        return head_res
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return None


def list_all_objects(s3client, bucket, prefix):
    paginator = s3client.get_paginator("list_objects_v2")
    response_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    for response in response_iterator:
        for content in response.get("Contents", []):
            yield content


def patch_object(cloud_object: CloudObject):
    head = cloud_object.storage.head_object(Bucket=cloud_object.meta_path.bucket, Key=cloud_object.meta_path.key)
    print(head)
    metadata = head.get("Metadata", {})
    print(metadata)
    attrs_bin = pickle.dumps(metadata)
    cloud_object.storage.put_object(
        Body=attrs_bin,
        Bucket=cloud_object._attrs_path.bucket,
        Key=cloud_object._attrs_path.key,
        Metadata={"dataplug": "dev"},
    )
    cloud_object.fetch()
