from ..compressed.gzipped import GZippedText


class FASTQGZip(GZippedText):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
