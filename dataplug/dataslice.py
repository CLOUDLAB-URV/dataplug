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
        self.s3: Optional[S3Client] = None
        self.obj_path: Optional[PureS3Path] = None
        self.meta_path: Optional[PureS3Path] = None
        self.size: Optional[int] = None
        self.attributes: Optional[Dict[str, str]] = None

    def contextualize(self, cloud_object: "CloudObject"):
        self.s3 = cloud_object.s3
        self.obj_path = cloud_object.path
        self.meta_path = cloud_object.meta_path
        self.size = cloud_object.size
        self.attributes = deepcopy(cloud_object._obj_attrs)

    def get(self):
        raise NotImplementedError()
