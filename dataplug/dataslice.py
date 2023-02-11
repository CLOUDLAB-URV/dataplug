from copy import deepcopy
from typing import TYPE_CHECKING, Optional, Dict

if TYPE_CHECKING:
    from dataplug.cloudobject import CloudObject
    from dataplug.storage import PureS3Path
    from mypy_boto3_s3 import S3Client


class CloudObjectSlice:
    def __init__(self, range_0=None, range_1=None):
        self.range_0: Optional[int] = range_0
        self.range_1: Optional[int] = range_1
        self.cloud_object: Optional[CloudObject] = None

    def get(self):
        raise NotImplementedError()
