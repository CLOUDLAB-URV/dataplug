from __future__ import annotations

import inspect
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
from dataplug.preprocessing import (
    BatchPreprocessor,
    MapReducePreprocessor,
    PreprocessorBackendBase,
)
from dataplug.storage.storage import StoragePath, PickleableS3ClientProxy
from dataplug.util import split_s3_path, head_object

if TYPE_CHECKING:
    from mypy_boto3_s3 import S3Client
    from typing import List, Tuple, Dict, Optional, Any, Union, Type
    from dataplug.preprocessing.backendbase import PreprocessingJobFuture
else:
    S3Client = object


class CloudDataFormatTemplate:
    def __init__(
            self,
            preprocessor: Union[Type[BatchPreprocessor], Type[MapReducePreprocessor]] = None,
            inherit_from: Type["CloudDataFormatTemplate"] = None,
    ):
        self.co_class: object = None
        self.__preprocessor: Union[Type[BatchPreprocessor], Type[MapReducePreprocessor]] = preprocessor
        self.__parent: "CloudDataFormatTemplate" = inherit_from
        self.cls_attributes = {}

    @property
    def preprocessor(
            self,
    ) -> Union[Type[BatchPreprocessor], Type[MapReducePreprocessor]]:
        if self.__preprocessor is not None:
            return self.__preprocessor
        elif self.__parent is not None:
            return self.__parent.preprocessor
        else:
            raise Exception("There is not preprocessor")

    def __call__(self, cls):
        # Get attribute names, types and initial values from decorated class
        if hasattr(cls, "__annotations__"):
            # Get annotated attributes
            for attr_key, _ in cls.__annotations__.items():
                self.cls_attributes[attr_key] = None
        # Get attributes with value
        for attr_key in filter(lambda attr: not attr.startswith("__") and not attr.endswith("__"), dir(cls)):
            attr_val = getattr(cls, attr_key)
            self.cls_attributes[attr_key] = attr_val

        if not inspect.isclass(cls):
            raise TypeError(f"CloudObject expected to use with class type, not {type(cls)}")

        if self.co_class is None:
            self.co_class = cls
        else:
            raise Exception(f"Can't overwrite decorator, now is {self.co_class}")

        return self
