class AsyncPreprocesser:
    def __init__(self):
        pass

    @staticmethod
    def preprocess(data_stream, meta):
        pass


class MapReducePreprocesser:
    def __init__(self):
        pass

    @staticmethod
    def map(data_stream, meta):
        raise NotImplementedError()

    @staticmethod
    def reduce(results, meta):
        raise NotImplementedError()
