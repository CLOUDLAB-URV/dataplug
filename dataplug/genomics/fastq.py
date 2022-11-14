from ..cloudobject import CloudDataType
from ..compressed.gzipped import GZipText


@CloudDataType(inherit_from=GZipText)
class FASTQGZip:
    def __init__(self, *args, **kwargs):
        super().__init__()
        # super().__init__(*args, **kwargs)
