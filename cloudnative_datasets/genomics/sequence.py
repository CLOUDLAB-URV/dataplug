from typing import BinaryIO, Tuple, Dict

from ..compressed.gzipped import GZippedText
from ..cobase import CloudObjectBase


class FASTQGZip(GZippedText):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class FASTA(CloudObjectBase):
    def preprocess(self, object_stream: BinaryIO) -> Tuple[bytes, Dict[str, str]]:
        pass
