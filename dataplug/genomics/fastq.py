from ..cloudobject import CloudObjectWrapper
from ..compressed.gzipped import GZipText


@CloudObjectWrapper(inherit_from=GZipText)
class FASTQGZip:
    def __init__(self, *args, **kwargs):
        super().__init__()
        # super().__init__(*args, **kwargs)
