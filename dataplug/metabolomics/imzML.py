
import logging
from math import ceil
from typing import BinaryIO, List
import pandas as pd
from ..cloudobject import CloudDataType
from ..dataslice import CloudObjectSlice
from pyimzml.ImzMLParser import ImzMLParser
import tempfile
logger = logging.getLogger(__name__)
import numpy as np


@CloudDataType()
class IMZML:
    def __init__(self):
        pass

class imzMLSlice(CloudObjectSlice):
    def __init__(self, coordinate: tuple, *args, **kwargs):
        self.coordinate = coordinate
        super().__init__(*args, **kwargs)
        

    def get_mz_info_point(self):
        """Get the mz info of one specific point in our data.
        Returns:
            Dict[str, Union[dict, str]]: All mz info.
        """
        body_imzml = self.s3.get_object(Bucket=self.obj_path.bucket, Key=self.obj_path.key)['Body']
        tmp = tempfile.NamedTemporaryFile()
        tmp.write(body_imzml.read())
        parser = ImzMLParser(tmp.name, ibd_file=None)
        position = parser.coordinates.index(self.coordinate)

        # Get precision in understandable words.
        for key, value in parser.precisionDict.items():
            if parser.mzPrecision == value:
                precision_aux_mz_point = key

        info = {"mz Offsets": parser.mzOffsets[position], "mz Group Id": parser.mzGroupId,
                "mz Precision": precision_aux_mz_point, "mz Lengths": parser.mzLengths[position]}
        return info


    def get_coordinate(self):
        """Get the coordinate specified for the slice
        Returns:
            Tuple: Coordinates
        """
        return self.coordinate
    
    def get_intensity_info_point(self):
        """Get the intensity info of one specific point in our data.
        Returns:
                Dict[str, Union[dict, str]]: All intensity info.
        """
        precision_aux_intensity_point = 0
        body_imzml = self.s3.get_object(Bucket=self.obj_path.bucket, Key=self.obj_path.key)['Body']
        tmp = tempfile.NamedTemporaryFile()
        
        tmp.write(body_imzml.read())
        parser = ImzMLParser(tmp.name, ibd_file=None)
        position = parser.coordinates.index(self.coordinate)
        # Get precision in understandable words.
        for key, value in parser.precisionDict.items():
            if parser.intensityPrecision == value:
                precision_aux_intensity_point = key

        info = {"Intensity Offsets": parser.intensityOffsets[position], "Intensity Precision":
                precision_aux_intensity_point, "Intensity Lengths": parser.intensityLengths[position]}

        return info
 

    def get_data_point_cloud(self, key_ibd: str):
        """Get all data in one specific point in our data, this method does not need the entire downloaded ibd file,
        it uses ranges to download a specific part.
        Returns:
            Mz array (list): Array with our mz info.
            Intensity array (list): Array with our intensity info.
        """
        return self.get_mz_array_point(key_ibd), self.get_intensity_array_point(key_ibd)

    def get_mz_array_point(self, key_ibd: str):
        """Get mz array in one specific point in our data, this method does not need the entire downloaded ibd file,
        it uses ranges to download a specific part.
        Returns:
            List: Mz array of our point.
        """
        body_imzml = self.s3.get_object(Bucket=self.obj_path.bucket, Key=self.obj_path.key)['Body']
        tmp = tempfile.NamedTemporaryFile()
        tmp.write(body_imzml.read())
        parser = ImzMLParser(tmp.name, ibd_file=None)
        position = parser.coordinates.index(self.coordinate)
        mzOffset = parser.mzOffsets[position]
        mzSize = (parser.sizeDict.get(parser.mzPrecision) * parser.mzLengths[position]) - 1
        # Download our specific data.
        header_request = self.s3.get_object(Bucket=self.obj_path.bucket, Key=key_ibd,
                                                 Range='bytes={}-{}'.format(mzOffset,
                                                                            mzOffset + mzSize))
        #mzArray = [np.frombuffer(i, dtype=parser.intensityPrecision) for i in header_request['Body']]
        mzArray = []
        for i in header_request['Body']:
            mzArray = np.append(mzArray, np.frombuffer(i, dtype=parser.intensityPrecision))

        return mzArray

    def get_intensity_array_point(self, key_ibd: str):
        """Get intensity array in one specific point in our data, this method not needs the entire downloaded ibd file,
        it uses ranges to download a specific part.
        The imz file needs to be loaded first.
        Returns:
            List: Intensity array of our point.
        """
        body_imzml = self.s3.get_object(Bucket=self.obj_path.bucket, Key=self.obj_path.key)['Body']
        tmp = tempfile.NamedTemporaryFile()
        tmp.write(body_imzml.read())
        parser = ImzMLParser(tmp.name, ibd_file=None)
        position = parser.coordinates.index(self.coordinate)
        intensityOffset = parser.intensityOffsets[position]
        intensitySize = (parser.sizeDict.get(parser.intensityPrecision) * parser.intensityLengths[position]) - 1
        header_request = self.s3.get_object(Bucket=self.obj_path.bucket, Key=key_ibd,
                                                 Range='bytes={}-{}'.format(intensityOffset,
                                                                            intensityOffset + intensitySize))
        #intensityArray = [np.frombuffer(i, dtype=parser.intensityPrecision) for i in header_request['Body']]
        intensityArray = []
        for i in header_request['Body']:
            intensityArray = np.append(intensityArray, np.frombuffer(i, dtype=parser.intensityPrecision))
  
        return intensityArray

    
def pt_strat(cloud_object: IMZML):
    body_imzml = cloud_object.s3.get_object(Bucket=cloud_object._obj_path.bucket, Key=cloud_object._obj_path.key)['Body']
    tmp = tempfile.NamedTemporaryFile()
    tmp.write(body_imzml.read())
    parser = ImzMLParser(tmp.name, ibd_file=None)
    position = parser.coordinates
    slices = [imzMLSlice(tuple) for tuple in position]
    
    return slices

    