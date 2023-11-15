import pathlib

from dataplug import CloudDataset
from dataplug.formats.generic.csv import CSV, batches_partition_strategy
from dataplug.preprocessing import LocalPreprocessor

from dataplug.util import setup_logging

setup_logging("DEBUG")

if __name__ == "__main__":
    CSV.check()

    path = pathlib.Path("./sample_data/dataset/customers?.csv").absolute().as_posix()
    ds = CloudDataset.from_glob(CSV, "file://" + path)
    # ds.preprocess()
    #
    # backend = LocalPreprocessor()
    # co.preprocess(backend, force=True)
    #
    # co.fetch()
    # print(co.attributes.columns)
    #
    # data_slices = co.partition(batches_partition_strategy, num_batches=10)
    #
    # # data_slices[-1].as_pandas_dataframe()
    #
    # for data_slice in data_slices:
    #     x = data_slice.as_pandas_dataframe()
    #     print(x)
