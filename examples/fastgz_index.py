import logging

from lithops_datasets import CloudObject
from lithops_datasets.genomics import FASTQGZip

logging.basicConfig(level=logging.INFO)

co = CloudObject(FASTQGZip, 'localhost://data/1c-12S_S96_L001/1c-12S_S96_L001_R1_001.fastq.gz')

co.preprocess()


def worker(chunk):
    pass


co.partition(FASTQGZip.even_lines, chunks=10).apply_parallel(worker)
