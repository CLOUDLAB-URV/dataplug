class AsyncPreprocesser:
    def __init__(self):
        pass

    @staticmethod
    def preprocess():
        pass


class MapReducePreprocesser:
    def __init__(self):
        pass

    @staticmethod
    def map(data_stream, worker_id, key, chunk_size, obj_size, partitions):
        raise NotImplementedError()

    @staticmethod
    def reduce(results):
        raise NotImplementedError()
