from __future__ import annotations

import inspect
import logging
from enum import Enum
from pprint import pprint
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from typing import Callable
    from .cloudobject import CloudObject

logger = logging.getLogger(__name__)


class PreprocessingType(Enum):
    MONOLITHIC = "monolithic"
    MAPREDUCE = "mapreduce"


class CloudDataFormat:
    def __init__(self, preprocessing_function: Callable = None, finalizer_function: Callable = None):
        self.co_class: object = None

        self.preprocessing_function = preprocessing_function
        self.finalizer_function = finalizer_function
        self.attrs_types = {}
        self.default_attrs = {}

    def __call__(self, cls):
        # Get attribute names, types and initial values from decorated class
        if hasattr(cls, "__annotations__"):
            # Get annotated attributes
            for attr_key, attr_type in cls.__annotations__.items():
                self.attrs_types[attr_key] = attr_type
        # Get attributes with value
        for attr_key in filter(lambda attr: not attr.startswith("__") and not attr.endswith("__"), dir(cls)):
            attr_val = getattr(cls, attr_key)
            self.attrs_types[attr_key] = type(attr_val)
            self.default_attrs[attr_key] = attr_val

        if not inspect.isclass(cls):
            raise TypeError(f"CloudObject expected to use with class type, not {type(cls)}")

        if self.co_class is None:
            self.co_class = cls
        else:
            raise Exception(f"Can't overwrite decorator, now is {self.co_class}")

        return self

    def debug(self):
        pprint({
            "co_class": self.co_class,
            "preprocessing_function": self.preprocessing_function,
            "finalizer_function": self.finalizer_function,
            "attrs_types": self.attrs_types,
            "default_attrs": self.default_attrs,
        })


class CloudObjectSlice:
    def __init__(self, range_0=None, range_1=None):
        self.range_0: Optional[int] = range_0
        self.range_1: Optional[int] = range_1
        self.cloud_object: Optional[CloudObject] = None

    def get(self):
        raise NotImplementedError()


class PartitioningStrategy:
    """
    Decorator class for defining partitioning strategies
    """

    def __init__(self, dataformat):
        self._data_format = dataformat
        self._func = None

    def __call__(self, func):
        def strategy_wrapper(cloud_object: CloudObject, *args, **kwargs):
            assert cloud_object._format_cls.co_class == self._data_format.co_class
            return func(cloud_object, *args, **kwargs)

        return strategy_wrapper
