from ..cobase import CloudObjectWrapper
from ..compressed.gzipped import GZipText


@CloudObjectWrapper(inherit=GZipText)
class FASTQGZip:
    def __init__(self, *args, **kwargs):
        super().__init__()
        # super().__init__(*args, **kwargs)
