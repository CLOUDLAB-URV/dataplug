import re
import logging
import functools

from lithops.storage import Storage as LithopsStorage
from lithops.storage.utils import StorageNoSuchKeyError


def partitioner_strategy(func):
    @functools.wraps(func)
    def with_logging(*args, **kwargs):
        print(func.__name__ + " was called")
        return func(*args, **kwargs)

    return with_logging


key_regex = re.compile(r'^\w+://.+/.+$')

logger = logging.getLogger(__name__)


class CloudObjectBase:
    def __init__(self, bucket, storage, key):
        self.bucket = bucket
        self.storage = storage
        self.key = key


class CloudObject:
    def __init__(self, cls, path):
        if not key_regex.match(path):
            raise Exception(f'CloudObject path must satisfy regex {key_regex.pattern}')
        self._meta = {}
        self._path = path
        self._cls = cls

        self._backend, _ = path.split('://')
        self._bucket, self._key = path.split('://')[1].split('/', 1)
        self._meta_key = self._key + '.meta'

        self._storage = LithopsStorage(backend=self._backend, config={'bucket': self._bucket})
        self._obj_meta = self._storage.head_object(bucket=self._bucket, key=self._key)

        try:
            self._storage.head_object(bucket=self._bucket, key=self._meta_key)
            self._staged = True
            logger.info('Object meta found')
        except StorageNoSuchKeyError:
            logger.info('Object meta not found')
            self._staged = False

        self._child = cls(self._bucket, self._storage, self._key)

    def __getstate__(self):
        return self._meta

    def __setstate__(self, state):
        self._meta = state

    def preprocess(self):
        if self._staged:
            logger.info('Object already staged')
        logger.info('Object meta not prepared')
        self._child.preprocess()

    def partition(self, strategy, **kwargs):
        print(strategy.__name__)
        return self

    def apply_parallel(self, func):
        pass
