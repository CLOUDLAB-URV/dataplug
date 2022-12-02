from pyimzml.ImzMLParser import ImzMLParser
import logging

import botocore

from dataplug import CloudObject
from dataplug.metabolomics import IMZML, pt_strat
from dataplug.util import setup_logging



if __name__ == '__main__':
    #unittest.main()
    config = {
        'aws_access_key_id': 'minioadmin',
        'aws_secret_access_key': 'minioadmin',
        'region_name': 'us-east-1',
        'endpoint_url': 'http://localhost:9000',
        'botocore_config_kwargs': {'signature_version': 's3v4'},
        'role_arn': 'arn:aws:iam::123456789012:role/S3Access'
    }
    
    #Local instance
    f = open('../sample_data/Example_Processed.imzML', 'rb')
    f1 = open('../sample_data/Example_Processed.ibd', 'rb')
    parser = ImzMLParser(f, ibd_file=f1)
    
    #Remote instance
    co = CloudObject.from_s3(IMZML,
                            's3://testdata/Example_Processed.imzML',
                            s3_config=config)


    slice = co.partition(pt_strat)

    #Example of the usage of all methods
    print(slice[0].get_mz_info_point())
    print(slice[0].get_coordinate())
    print(slice[0].get_intensity_info_point())
    print(slice[0].get_data_point_cloud('Example_Processed.ibd'))
    
    
    
    
    