class CloudObjectSlice:
    def __init__(self, range_0, range_1):
        self.range_0 = range_0
        self.range_1 = range_1

    def _populate(self, cloud_object):
        self.s3 = cloud_object._s3
        self.obj_path = cloud_object._obj_path
        self.meta_path = cloud_object._meta_path
        self.obj_size = cloud_object.size
        # self.attributes = cloud_object._obj_attrs

    def get(self):
        res = self.s3.get_object(Bucket=self.obj_path.bucket, Key=self.obj_path.key,
                                 Range=f'bytes={self.range_0}-{self.range_1}')
        body = res['Body']
        return body.read()
