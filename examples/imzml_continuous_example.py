from dataplug import CloudObject
from dataplug.formats.metabolomics.imzml import ImzML, partition_chunks_strategy

import numpy as np

if __name__ == "__main__":
    aws_config = {
        "credentials": {
            "AccessKeyId": "",
            "SecretAccessKey": "",
        },
        "region_name": "",
        "endpoint_url": ""
    }

    # Sample: https://ms-imaging.org/wp-content/uploads/2019/03/S042_Continuous_imzML1.1.1.zip
    co = CloudObject.from_s3(ImzML, "s3://samples/S042_Continuous.ibd", s3_config=aws_config)

    co.preprocess(force=True)

    # Using a small chunk size since this dataset is small
    data_slices = co.partition(partition_chunks_strategy, chunk_size=10 * 1024**2)

    for data_slice in data_slices:
        print(f"Slice first spectrum index {data_slice.spectrum_index} ({len(data_slice.int_offsets)} spectra)")
        raw_data = data_slice.get()

        # read the common m/z array
        mz_length_bytes = data_slice.mz_lengths[0] * data_slice.cloud_object.attributes.mz_size
        mz_array = np.frombuffer(raw_data[0:mz_length_bytes], dtype=data_slice.cloud_object.attributes.mz_precision)

        for i in range(len(data_slice.int_offsets)):
            spectrum_index = data_slice.spectrum_index + i
            coordinates = data_slice.cloud_object.attributes.coordinates[spectrum_index]
            print(f"Spectrum index {spectrum_index}, pixel coordinates: {coordinates}")

            # read intensity array
            int_length_bytes = data_slice.int_lengths[i] * data_slice.cloud_object.attributes.int_size
            int_array = np.frombuffer(
                raw_data[data_slice.int_offsets[i] : data_slice.int_offsets[i] + int_length_bytes],
                dtype=data_slice.cloud_object.attributes.int_precision,
            )
