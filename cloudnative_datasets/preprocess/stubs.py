class BatchPreprocessor:
    def __init__(self):
        pass

    @staticmethod
    def preprocess(data_stream, meta):
        pass


class MapReducePreprocessor:
    def __init__(self):
        pass

    @staticmethod
    def map(data_stream, meta):
        raise NotImplementedError()

    @staticmethod
    def reduce(results, meta):
        raise NotImplementedError()
