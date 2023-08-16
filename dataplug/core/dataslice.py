from __future__ import annotations

import inspect
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from dataplug.core.cloudobject import CloudObject


class CloudObjectSlice:
    def __init__(self, range_0=None, range_1=None):
        self.range_0: Optional[int] = range_0
        self.range_1: Optional[int] = range_1
        self.cloud_object: Optional[CloudObject] = None

    def get(self):
        raise NotImplementedError()


class DataSlice:
    """
    Decorator class for defining Data Slices
    """

    def __init__(self, cls):
        # print(cls)
        pass

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


class PartitioningStrategy:
    """
    Decorator class for defining partitioning strategies
    """

    def __init__(self, dataformat):
        self._data_format = dataformat
        self._cls = None
        print(self._data_format)

    def __call__(self, cls):
        if self._cls is None:
            self._cls = cls
        else:
            raise Exception()
        return self
