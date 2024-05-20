from __future__ import annotations

import pickle
from typing import TYPE_CHECKING

from boto3.s3.transfer import TransferConfig

from ..util import force_delete_path
from ..version import __version__

if TYPE_CHECKING:
    from ..cloudobject import CloudObject
    from typing import List


def joblib_handler(args):
    # Joblib delayed function expect only one argument, so we need to unpack the arguments
    preprocessing_function, parameters = args
    co = parameters["cloud_object"]
    if "chunk_data" in parameters and parameters["chunk_data"] is None:
        chunk_id = parameters.get("chunk_id", 0)
        chunk_size = parameters.get("chunk_size", co.size)
        num_chunks = parameters.get("num_chunks", 1)

        range_0 = chunk_id * chunk_size
        range_1 = co.size if chunk_size == num_chunks - 1 \
            else (chunk_id + 1) * chunk_size
        get_res = co.storage.get_object(
            Bucket=co.path.bucket, Key=co.path.key, Range=f"bytes={range_0}-{range_1 - 1}"
        )
        parameters["chunk_data"] = get_res["Body"]

    metadata = preprocessing_function(**parameters)
    if all((metadata.metadata, metadata.metadata_file_path)):
        raise Exception("Choose one for object preprocessing result: metadata or metadata_file_path")

    # Upload metadata body to meta bucket with the same key as the original object
    if metadata.metadata is not None:
        if hasattr(metadata.metadata, "read"):
            co.storage.upload_fileobj(
                Fileobj=metadata.metadata,
                Bucket=co.meta_path.bucket,
                Key=co.path.key,
                ExtraArgs={"Metadata": {"dataplug": __version__}},
                Config=TransferConfig(use_threads=True, max_concurrency=256),
            )
            if hasattr(metadata.metadata, "close"):
                metadata.metadata.close()
        else:
            co.storage.put_object(
                Body=metadata.metadata,
                Bucket=co.meta_path.bucket,
                Key=co.path.key,
                Metadata={"dataplug": __version__},
            )

    if metadata.metadata_file_path is not None:
        co.storage.upload_file(
            Filename=metadata.metadata_file_path,
            Bucket=co.meta_path.bucket,
            Key=co.path.key,
            ExtraArgs={"Metadata": {"dataplug": __version__}},
            Config=TransferConfig(use_threads=True, max_concurrency=256),
        )
        force_delete_path(metadata.metadata_file_path)

    if metadata.metadata is None and metadata.metadata_file_path is None:
        # Upload empty metadata object to meta bucket, to signal that the preprocessing was successful
        co.storage.put_object(
            Body=b"",
            Bucket=co.meta_path.bucket,
            Key=co.path.key,
            Metadata={"dataplug": __version__},
        )

    # Upload attributes to meta bucket
    if metadata.attributes is not None:
        attrs_bin = pickle.dumps(metadata.attributes)
        co.storage.put_object(
            Body=attrs_bin,
            Bucket=co._attrs_path.bucket,
            Key=co._attrs_path.key,
            Metadata={"dataplug": __version__},
        )

