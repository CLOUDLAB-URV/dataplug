class CloudObjectChunk:
    def __init__(self, cloud_object, range_0, range_1):
        self.range_0 = range_0
        self.range_1 = range_1

        self.s3 = cloud_object._s3
        self.meta_bucket = cloud_object._meta_bucket
        self.meta_key = cloud_object._meta_key
        self.object_bucket = cloud_object._object_bucket
        self.object_key = cloud_object._object_key
        self.attributes = cloud_object._obj_attrs

    def get(self, stream=False):
        res = self.s3.get_object(Bucket=self.object_bucket, Key=self.object_key,
                                 Range=f'bytes={self.range_0}-{self.range_1}')
        body = res['Body']
        if stream:
            return body
        else:
            return body.read()
