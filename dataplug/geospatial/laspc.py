import json
import tempfile
import shutil
import logging
from typing import TYPE_CHECKING

from dataplug.cloudobject import CloudDataType, CloudObject
from dataplug.preprocess import BatchPreprocessor
from dataplug.util import force_delete_path

if TYPE_CHECKING:
    from typing import BinaryIO, Tuple, Dict

try:
    import pdal
    import laspy.copc
except ModuleNotFoundError:
    pass

logger = logging.getLogger(__name__)


class LiDARPreprocessor(BatchPreprocessor):
    def __init__(self):
        try:
            import pdal
            import laspy.copc
        except ModuleNotFoundError as e:
            logger.error("Missing Geospatial packages!")
            raise e
        super().__init__()

    def preprocess(self, cloud_object: CloudObject) -> Tuple[BinaryIO, Dict[str, str]]:
        input_file_path = tempfile.mktemp()
        output_file_path = tempfile.mktemp()

        try:
            force_delete_path(input_file_path)
            force_delete_path(output_file_path)

            with open(input_file_path, "wb") as input_file:
                shutil.copyfileobj(data_stream, input_file)

            pipeline_json = [
                {"type": "readers.las", "filename": input_file_path},
                {"type": "writers.copc", "filename": output_file_path},
            ]

            pipeline = pdal.Pipeline(json.dumps(pipeline_json))
            pipeline.execute()

            # with open(output_file_path, 'rb') as copc_file:
            #     copc_reader = laspy.copc.CopcReader(copc_file)
            #     copc_meta = {
            #         'points': str(copc_reader.header.point_count),
            #         'x_scale': str(copc_reader.header.x_scale),
            #         'y_scale': str(copc_reader.header.y_scale),
            #         'z_scale': str(copc_reader.header.z_scale),
            #         'x_offset': str(copc_reader.header.x_offset),
            #         'y_offset': str(copc_reader.header.y_offset),
            #         'z_offset': str(copc_reader.header.z_offset),
            #         'x_max': str(copc_reader.header.x_max),
            #         'y_max': str(copc_reader.header.y_max),
            #         'z_max': str(copc_reader.header.z_max),
            #         'x_min': str(copc_reader.header.x_min),
            #         'y_min': str(copc_reader.header.y_min),
            #         'z_min': str(copc_reader.header.z_min),
            #         'root_offset': str(copc_reader.copc_info.hierarchy_root_offset),
            #         'root_size': str(copc_reader.copc_info.hierarchy_root_size)
            #     }
            copc_meta = {}

            return open(output_file_path, "rb"), copc_meta
        finally:
            force_delete_path(input_file_path)
            force_delete_path(output_file_path)


@CloudDataType(preprocessor=LiDARPreprocessor)
class LiDARPointCloud:
    def __init__(self, cloud_object):
        self.cloud_object = cloud_object
