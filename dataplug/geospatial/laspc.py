from __future__ import annotations

import json
import tempfile
import subprocess
import logging
from typing import TYPE_CHECKING

import tqdm

from ..cloudobject import CloudDataType, CloudObject
from ..preprocessing import BatchPreprocessor, PreprocessingMetadata
from ..util import force_delete_path

try:
    import pdal
    import laspy.copc
except ModuleNotFoundError:
    pass

logger = logging.getLogger(__name__)

CHUNK_SIZE = 65536


def _get_lasindex_path():
    proc = subprocess.run(["which", "lasindex"], check=True, capture_output=True, text=True)
    path = proc.stdout.rstrip("\n")
    logger.debug("Using lasindex located in %s", path)
    return path


class LiDARPreprocessor(BatchPreprocessor):
    def __init__(self):
        try:
            import pdal
            import laspy.copc
        except ModuleNotFoundError as e:
            logger.error("Missing Geospatial packages!")
            raise e
        super().__init__()

    def preprocess(self, cloud_object: CloudObject) -> PreprocessingMetadata:
        lasindex = _get_lasindex_path()

        # lasindex tool requires index terminated with .lax
        tmp_index_file_path = tempfile.mktemp() + '.lax'

        try:
            force_delete_path(tmp_index_file_path)

            obj_res = cloud_object.s3.get_object(Bucket=cloud_object.path.bucket, Key=cloud_object.path.key)
            assert obj_res.get("ResponseMetadata", {}).get("HTTPStatusCode") == 200
            data_stream = obj_res["Body"]

            cmd = [lasindex, "-stdin", "-o", tmp_index_file_path]
            logger.debug(' '.join(cmd))
            index_proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            with tqdm.tqdm(total=cloud_object.size) as pb:
                try:
                    chunk = data_stream.read(CHUNK_SIZE)
                    while chunk != b"":
                        # logger.debug('Writing %d bytes to Pipe STDIN', len(chunk))
                        index_proc.stdin.write(chunk)
                        pb.update(len(chunk))
                        chunk = data_stream.read(CHUNK_SIZE)
                    if hasattr(data_stream, "close"):
                        data_stream.close()
                    # index_proc.stdin.close()
                except BrokenPipeError as e:
                    stdout, stderr = index_proc.communicate()
                    logger.error(stdout.decode("utf-8"))
                    logger.error(stderr.decode("utf-8"))
                    raise e

            stdout, stderr = index_proc.communicate()
            logger.debug(stdout.decode("utf-8"))
            logger.debug(stderr.decode("utf-8"))

            # Get attributes
            with cloud_object.open("rb") as input_file:
                las_file = laspy.open(source=input_file, closefd=False)
                las_meta = {
                    'mins': las_file.header.mins,
                    'maxs': las_file.header.maxs,
                    'point_count': las_file.header.point_count
                }

            with open(tmp_index_file_path, 'rb') as index_file:
                lax_data = index_file.read()

            return PreprocessingMetadata(metadata=lax_data, attributes=las_meta)
        finally:
            force_delete_path(tmp_index_file_path)


@CloudDataType(preprocessor=LiDARPreprocessor)
class LiDARPointCloud:
    def __init__(self, cloud_object):
        self.cloud_object = cloud_object
