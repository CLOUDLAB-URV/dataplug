class AsyncPreprocesser:
    def __init__(self):
        pass

    @staticmethod
    def preprocess(data_stream, key, s3):
        pass


class MapReducePreprocesser:
    def __init__(self):
        pass

    @staticmethod
    def map(data_stream, worker_id, key, chunk_size, obj_size, partitions, s3):
        raise NotImplementedError()

    @staticmethod
    def reduce(results, s3):
        raise NotImplementedError()
