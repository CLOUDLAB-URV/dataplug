
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
    def __init__(self, cloud_object):
        pass

class imzMLSlice(CloudObjectSlice):
    def __init__(self,coordinate: tuple, *args, **kwargs):
        self.key_ibd = ""
        self.coordinate = coordinate
        super().__init__(*args, **kwargs)

    def get_spectrum(self,index: int):
        """Split the imz file into small parts, the number of parts is defined by the number of coordinates.
        The imz and ibd file needs to be loaded first.
        Returns:
            List[tuple[ndarray, ndarray]]: MzArray and IntensityArray
        """

        self.key_ibd = self.obj_path.key.split('.')[0] + ".ibd"
        body = self.s3.get_object(Bucket=self.obj_path.bucket, Key=self.obj_path.key)['Body']
        body_ibd = self.s3.get_object(Bucket=self.obj_path.bucket, Key=self.key_ibd)['Body']
        tmp_ibd = tempfile.NamedTemporaryFile()
        tmp = tempfile.NamedTemporaryFile()
        with open(tmp.name, 'wb') as f:
            f.write(body.read())
        with open(tmp_ibd.name, 'wb') as f:
            f.write(body_ibd.read())
        parser = ImzMLParser(tmp.name, ibd_file=tmp_ibd.name)


    
        # Get the position of our coordinate.
        return parser.getspectrum(index)



    def get_data_point_local(self):
        """Get data of one specific point in our data, this method needs the entire downloaded ibd file.
        The imz and ibd file needs to be loaded first.
        Returns:
            Tuple[ndarray, ndarray]: Data of the specific point.
        """
        self.key_ibd = self.obj_path.key.split('.')[0] + ".ibd"
        body = self.s3.get_object(Bucket=self.obj_path.bucket, Key=self.obj_path.key)['Body']
        tmp = tempfile.NamedTemporaryFile()
        with open(tmp.name, 'wb') as f:
            f.write(body.read())
        parser = ImzMLParser(tmp.name, ibd_file=None)
        position = parser.coordinates.index(self.coordinate)

        info = parser.getspectrum(position)
        
        return info

    

    def get_data_point_cloud(self):
        """Get all data in one specific point in our data, this method not needs the entire downloaded ibd file,
        it uses ranges to download a specific part.
        The imz file needs to be loaded first.
        Returns:
            Mz array (list): Array with our mz info.
            Intensity array (list): Array with our intensity info.
        """
        return self.get_mz_array_point(self.coordinate), self.get_intensity_array_point(self.coordinate)

    def get_mz_array_point(self):
        """Get mz array in one specific point in our data, this method not needs the entire downloaded ibd file,
        it uses ranges to download a specific part.
        The imz file needs to be loaded first.
        Returns:
            List: Mz array of our point.
        """
        self.key_ibd = self.obj_path.key.split('.')[0] + ".ibd"
        body = self.s3.get_object(Bucket=self.obj_path.bucket, Key=self.obj_path.key)['Body']
        tmp = tempfile.NamedTemporaryFile()
        with open(tmp.name, 'wb') as f:
            f.write(body.read())
        parser = ImzMLParser(tmp.name, ibd_file=None)
        position = parser.coordinates.index(self.coordinate)

        mzOffset = parser.mzOffsets[position]
        mzSize = (parser.sizeDict.get(parser.mzPrecision) * parser.mzLengths[position]) - 1

        # Download our specific data.
        header_request = self.s3.get_object(Bucket=self.obj_path.bucket, Key=self.key_ibd,
                                                 Range='bytes={}-{}'.format(mzOffset,
                                                                            mzOffset + mzSize))
        mzArray = []
        for i in header_request['Body']:
            mzArray = np.append(mzArray, np.frombuffer(i, dtype=parser.intensityPrecision))

        f.close()
        return mzArray

    def get_intensity_array_point(self):
        self.key_ibd = self.obj_path.key.split('.')[0] + ".ibd"
        body = self.s3.get_object(Bucket=self.obj_path.bucket, Key=self.obj_path.key)['Body']
        tmp = tempfile.NamedTemporaryFile()
        with open(tmp.name, 'wb') as f:
            f.write(body.read())
        parser = ImzMLParser(tmp.name, ibd_file=None)
        position = parser.coordinates.index(self.coordinate)
        intensityOffset = parser.intensityOffsets[position]
        intensitySize = (parser.sizeDict.get(parser.intensityPrecision) * parser.intensityLengths[position]) - 1

        header_request = self.s3.get_object(Bucket=self.obj_path.bucket, Key=self.key_ibd,
                                                 Range='bytes={}-{}'.format(intensityOffset,
                                                                            intensityOffset + intensitySize))
        intensityArray = []
        for i in header_request['Body']:
            intensityArray = np.append(intensityArray, np.frombuffer(i, dtype=parser.intensityPrecision))

        return intensityArray

    
def pt_strat(cloud_object: IMZML):
    
    body = cloud_object.s3.get_object(Bucket=cloud_object._obj_path.bucket, Key=cloud_object._obj_path.key)['Body']
    tmp = tempfile.NamedTemporaryFile()
    with open(tmp.name, 'wb') as f:
        f.write(body.read())
    parser = ImzMLParser(tmp.name, ibd_file=None)
    position = parser.coordinates
    slices = []
    for tuple in position:
        slices.append(imzMLSlice(tuple))
    
    return slices

    