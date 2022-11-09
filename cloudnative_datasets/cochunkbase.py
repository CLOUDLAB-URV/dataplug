from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .cobase import CloudObject
    from .storage import PureS3Path
    from mypy_boto3_s3 import S3Client


class CloudObjectSlice:
    def __init__(self, range_0, range_1):
        self.range_0 = range_0
        self.range_1 = range_1
        self.s3: Optional[S3Client] = None
        self.obj_path: Optional[PureS3Path] = None
        self.meta_path: Optional[PureS3Path] = None
        self.size: Optional[int] = None

    def contextualize(self, cloud_object: 'CloudObject'):
        self.s3 = cloud_object.s3
        self.obj_path = cloud_object.path
        self.meta_path = cloud_object.meta_path
        self.size = cloud_object.size
        # self.attributes = cloud_object._obj_attrs

    def get(self):
        res = self.s3.get_object(Bucket=self.obj_path.bucket, Key=self.obj_path.key,
                                 Range=f'bytes={self.range_0}-{self.range_1}')
        body = res['Body']
        return body.read()
