from ..cobase import CloudObjectWrapper

from ..preprocessers import BatchPreprocesser


class LiDARPreprocesser(BatchPreprocesser):
    def __init__(self):
        super().__init__()

    @staticmethod
    def preprocess(data_stream, meta):
        super().preprocess(data_stream, meta)


@CloudObjectWrapper(preprocesser=LiDARPreprocesser)
class LiDARPointCloud:
    def __init__(self, cloud_object):
        self.cloud_object = cloud_object
