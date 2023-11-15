from __future__ import annotations

import inspect
import logging
from copy import deepcopy
from pprint import pprint
from typing import TYPE_CHECKING, Optional

from .util import fully_qualified_name

if TYPE_CHECKING:
    from dataplug.cloudobject import CloudObject

logger = logging.getLogger(__name__)


class CloudDataFormat:
    def __init__(self, cls):
        self.__format_class = cls
        self.__attrs = {}
        self.__preprocessor = None
        self.__slice_classes = []
        self.__strategies = []

        if hasattr(cls, "__annotations__"):
            # Get annotated attributes
            for attr_key, attr_type in cls.__annotations__.items():
                self.__attrs[attr_key] = attr_type, None
        # Get attributes with value
        for attr_key in filter(lambda attr: not attr.startswith("__") and not attr.endswith("__"), dir(cls)):
            attr_val = getattr(cls, attr_key)
            self.__attrs[attr_key] = type(attr_val), attr_val

        if not inspect.isclass(cls):
            raise TypeError(f"CloudObject expected to use with class type, not {type(cls)}")

    @property
    def _wrappee(self):
        return self.__format_class

    def _set_preprocessor(self, preprocessor):
        if self.__preprocessor is not None:
            logger.warning("Overwriting preprocessor, previous was %s, new is %s", self.__preprocessor, preprocessor)
        self.__preprocessor = preprocessor

    @property
    def _preprocessor(self):
        return self.__preprocessor

    def _set_slice_class(self, slice_class):
        self.__slice_classes.append(slice_class)

    def _set_partitioning_strategy(self, strategy):
        self.__strategies.append(strategy)

    @property
    def _default_attrs(self):
        return deepcopy(self.__attrs)

    def check(self, doprint=True):
        dump = {
            "format_class": fully_qualified_name(self.__format_class),
            "attributes": self.__attrs,
            "preprocessor": fully_qualified_name(self.__preprocessor),
            "slices": [fully_qualified_name(slise) for slise in self.__slice_classes],
            "partitioning_strategies": [fully_qualified_name(strat) for strat in self.__strategies],
        }
        if doprint:
            pprint(dump)
        return dump


class FormatPreprocessor:
    def __init__(self, target):
        self.__target: CloudDataFormat = target
        pass

    def __call__(self, cls):
        self.__target._set_preprocessor(cls)
        return self


class CloudObjectSlice:
    def __init__(self, range_0=None, range_1=None):
        self.range_0: Optional[int] = range_0
        self.range_1: Optional[int] = range_1
        self.cloud_object: Optional[CloudObject] = None

    def get(self):
        raise NotImplementedError()


class PartitioningStrategy:
    def __init__(self, target: CloudDataFormat):
        self.__target: CloudDataFormat = target
        self.__strategy = None
        pass

    def __call__(self, cls):
        self.__target._set_partitioning_strategy(cls)
        self.__strategy = cls
        return self

    @property
    def _data_format(self):
        return self.__target

    @property
    def _func(self):
        return self.__strategy
