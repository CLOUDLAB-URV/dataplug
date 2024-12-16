from __future__ import annotations

import pickle
from typing import TYPE_CHECKING

from boto3.s3.transfer import TransferConfig

from ..util import force_delete_path
from ..version import __version__

if TYPE_CHECKING:
    pass


def monolith_joblib_handler(args):
    # Joblib delayed function expect only one argument, so we need to unpack the arguments
    preprocessing_function, parameters = args
    co = parameters["cloud_object"]

    metadata = preprocessing_function(**parameters)
    if all((metadata.metadata, metadata.metadata_file_path)):
        raise Exception("Choose one for object preprocessing result: metadata or metadata_file_path")

    upload_metadata(co, metadata)


def map_joblib_handler(args):
    # Joblib delayed function expect only one argument, so we need to unpack the arguments
    preprocessing_function, parameters = args

    co = parameters["cloud_object"]
    chunk_id = parameters["chunk_id"]
    chunk_size = parameters["chunk_size"]
    num_chunks = parameters["num_chunks"]

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
    key = f"{co.path.key}.chunk{str(chunk_id).zfill(3)}"
    partial_meta_bin = pickle.dumps(metadata)
    co.storage.put_object(
        Body=partial_meta_bin,
        Bucket=co.meta_path.bucket,
        Key=key,
        Metadata={"dataplug": __version__},
    )

    return chunk_id, key


def reduce_joblib_handler(args):
    # Joblib delayed function expect only one argument, so we need to unpack the arguments
    finalizer_function, parameters = args
    co = parameters["cloud_object"]

    def _partial_metadata_generator(cloud_object, partial_results):
        for chunk_id, key in partial_results:
            res = cloud_object.storage.get_object(Bucket=cloud_object.meta_path.bucket, Key=key)
            m = pickle.loads(res["Body"].read())
            cloud_object.storage.delete_object(Bucket=cloud_object.meta_path.bucket, Key=key)
            yield m

    chunk_metadata = _partial_metadata_generator(co, parameters["partial_results"])

    metadata = finalizer_function(co, chunk_metadata)
    if all((metadata.metadata, metadata.metadata_file_path)):
        raise Exception("Choose one for object preprocessing result: metadata or metadata_file_path")

    upload_metadata(co, metadata)


def upload_metadata(cloud_object, metadata):
    if metadata.metadata is not None:
        if hasattr(metadata.metadata, "read"):
            cloud_object.storage.upload_fileobj(
                Fileobj=metadata.metadata,
                Bucket=cloud_object.meta_path.bucket,
                Key=cloud_object.path.key,
                ExtraArgs={"Metadata": {"dataplug": __version__}},
                Config=TransferConfig(use_threads=True, max_concurrency=256),
            )
            if hasattr(metadata.metadata, "close"):
                metadata.metadata.close()
        else:
            cloud_object.storage.put_object(
                Body=metadata.metadata,
                Bucket=cloud_object.meta_path.bucket,
                Key=cloud_object.path.key,
                Metadata={"dataplug": __version__},
            )

    if metadata.metadata_file_path is not None:
        cloud_object.storage.upload_file(
            Filename=metadata.metadata_file_path,
            Bucket=cloud_object.meta_path.bucket,
            Key=cloud_object.path.key,
            ExtraArgs={"Metadata": {"dataplug": __version__}},
            Config=TransferConfig(use_threads=True, max_concurrency=256),
        )
        force_delete_path(metadata.metadata_file_path)

    if metadata.metadata is None and metadata.metadata_file_path is None:
        # Upload empty metadata object to meta bucket, to signal that the preprocessing was successful
        cloud_object.storage.put_object(
            Body=b"",
            Bucket=cloud_object.meta_path.bucket,
            Key=cloud_object.path.key,
            Metadata={"dataplug": __version__},
        )

    # Upload attributes to meta bucket
    if metadata.attributes is not None:
        attrs_bin = pickle.dumps(metadata.attributes)
        cloud_object.storage.put_object(
            Body=attrs_bin,
            Bucket=cloud_object._attrs_path.bucket,
            Key=cloud_object._attrs_path.key,
            Metadata={"dataplug": __version__},
        )
