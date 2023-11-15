from __future__ import annotations

import fnmatch
import re
import logging

from .storage.storage import create_client
from .util import NoPublicConstructor

logger = logging.getLogger(__name__)


class CloudDataset(metaclass=NoPublicConstructor):
    def __init__(self, data_format, keys):
        self.__data_format = data_format
        self.__keys = keys

    @classmethod
    def from_glob(cls, data_format, glob_pattern, storage_config=None):
        match = re.match(r"^([a-zA-Z0-9]+)://", glob_pattern)
        if match:
            storage = match.group(1)
            prefix = match.group()
        else:
            raise ValueError(f"{glob_pattern} is not an uri")

        full_path = glob_pattern[len(prefix):]
        storage_client = create_client(storage, storage_config or {})

        # We find the first glob character and split at it to find a prefix
        # that can be used to list only the objects we are interested of listing all
        match = re.search(r"[\*\?\[\]]", glob_pattern)
        if match:
            glob_idx = match.start()
        else:
            glob_idx = len(glob_pattern)
        path_no_glob = glob_pattern[len(prefix):glob_idx]
        b2, k2 = storage_client._parse_full_path(path_no_glob)
        keys = storage_client.list_objects(Bucket=b2, Prefix=k2)

        # Filter keys that match the glob pattern
        matching = fnmatch.filter((c["Key"] for c in keys["Contents"]), full_path)
        logger.info("Found %d matching keys for pattern %s", len(matching), glob_pattern)
        cls._create(data_format, matching)
