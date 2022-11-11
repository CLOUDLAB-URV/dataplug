from ..base import PreprocessorBackendBase


class LithopsPreprocessor(PreprocessorBackendBase):
    def __init__(self, lithops_config):
        self.lithops_config = lithops_config

    def do_preprocess(self):
        ...
