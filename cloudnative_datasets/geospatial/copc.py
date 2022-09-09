import json

import pdal
import laspy.copc as lasp
import json
import tempfile
import shutil

from ..cobase import CloudObjectWrapper
from ..preprocessers import BatchPreprocesser
from ..util import force_delete_path


class LiDARPreprocesser(BatchPreprocesser):
    def __init__(self):
        super().__init__()

    @staticmethod
    def preprocess(data_stream, meta):
        input_file_path = tempfile.mktemp()
        output_file_path = tempfile.mktemp()

        try:
            force_delete_path(input_file_path)
            force_delete_path(output_file_path)

            with open(input_file_path, 'wb') as input_file:
                shutil.copyfileobj(data_stream, input_file)

            pipeline_json = [
                {
                    "type": "readers.las",
                    "filename": input_file_path
                },
                {
                    "type": "writers.copc",
                    "filename": output_file_path
                }
            ]

            pipeline = pdal.Pipeline(json.dumps(pipeline_json))
            pipeline.execute()

            with open(output_file_path, 'rb') as copc_file:
                copc_reader = lasp.CopcReader(copc_file)
                copc_meta = {
                    'points': str(copc_reader.header.point_count),
                    'x_scale': str(copc_reader.header.x_scale),
                    'y_scale': str(copc_reader.header.y_scale),
                    'z_scale': str(copc_reader.header.z_scale),
                    'x_offset': str(copc_reader.header.x_offset),
                    'y_offset': str(copc_reader.header.y_offset),
                    'z_offset': str(copc_reader.header.z_offset),
                    'x_max': str(copc_reader.header.x_max),
                    'y_max': str(copc_reader.header.y_max),
                    'z_max': str(copc_reader.header.z_max),
                    'x_min': str(copc_reader.header.x_min),
                    'y_min': str(copc_reader.header.y_min),
                    'z_min': str(copc_reader.header.z_min),
                    'root_offset': str(copc_reader.copc_info.hierarchy_root_offset),
                    'root_size': str(copc_reader.copc_info.hierarchy_root_size)
                }

                return open(output_file_path, 'rb'), copc_meta
        finally:
            force_delete_path(input_file_path)
            force_delete_path(output_file_path)



@CloudObjectWrapper(preprocesser=LiDARPreprocesser)
class LiDARPointCloud:
    def __init__(self, cloud_object):
        self.cloud_object = cloud_object
