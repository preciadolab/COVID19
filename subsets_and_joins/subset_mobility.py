"""
Function to subset the whole safegraph dataset to a list of geohashes of same length
"""
import numpy as np
import numpy.random as npr
import getopt, sys
import os
import pandas as pd
import gzip as gz
import subprocess
import time
import io
import pdb
import pyarrow.parquet as pq
import re
import json
from shapely.geometry import shape

sys.path.insert(0, '../auxiliary_functions/')
sys.path.insert(0, './')
from spatial_functions import polygons_to_geohash


def main():
    argument_list = sys.argv[1:]
    short_options = "d:m:k:"
    long_options = ["day", "month", "index"]

    try:
        arguments, values = getopt.getopt(argument_list, short_options, long_options)
    except getopt.error as err:
        # Output error, and return with an error code
        print (str(err))
        sys.exit(2)

    day = '10' #default values
    month = '05' #default values
    for current_argument, current_value in arguments:
        if current_argument in ("-d", "--day"):
            print ("Subsetting for day: " + current_value)
            day= current_value
        elif current_argument in ("-m", "--month"):
            print ("Subsetting for month: " + current_value)
            month = current_value
        elif current_argument in ("-k", "--index"):
            #find k-th day since April 1 
            if int(current_value) in list(range(1,31)):
                month = '04'
                day = str(int(current_value)).zfill(2)
            elif int(current_value) in list(range(31,62)):
                month = '05'
                day = str(int(current_value) - 30).zfill(2)
            print("Subsetting for {}-{} (mm-dd)".format(month, day))

    if int(month) > 5 or (int(month) >= 5 and int(day) >= 4): 
        cmd='aws s3 ls s3://safegraph-outgoing/verasetcovidmovementusa/2020/'+ month +'/'+day+'/ --profile veraset'
    else:
        cmd='aws s3 ls s3://safegraph-outgoing/verasetcovidmovementusa/backfill/2020/'+ month +'/'+day+'/ --profile veraset'

    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    result.check_returncode()

    file_list = [x.split(' ')[-1] for x in result.stdout.split('\n')]
    file_list = sorted([nam for nam in file_list if nam[:4] == 'part'])

    #Create local folder to output filtered data
    newfile_path = '../../veraset-42101/'+ month +'/'+ day +'/'
    os.makedirs(newfile_path, exist_ok=True)

    #find geohashes for Philadelphia and surrounding
    print("subsetting to places in: 42101, 42045, 42091")
    with open('../../core_places/censusBlockGroups.geojson') as fp:
        geojson = json.load(fp)
    cb_polygon_dict = {}
    for i in range(len(geojson['features'])):
        if geojson['features'][i]['properties']['GEOID'][:5] in ['42101','42045','42091']:
            cb_polygon_dict[geojson['features'][i]['properties']['GEOID']] =  geojson['features'][i]['geometry']

    cb_polygon_dict = { k:shape(v) for k,v in cb_polygon_dict.items()}
    polygon_list = [v for k,v in cb_polygon_dict.items()]
    precision = 5
    hashset = polygons_to_geohash(polygon_list = polygon_list,
                                  precision = precision)
    

    #Iterate through file_list
    print("Starting subset on {} chunks".format(len(file_list)))
    for file_name in file_list:
        if int(month) > 5 or (int(month) >= 5 and int(day) >= 4): 
            cmd='aws s3 cp s3://safegraph-outgoing/verasetcovidmovementusa/2020/'+ month +'/'+day+'/' + file_name + ' ./ --profile veraset' 
        else:
            cmd='aws s3 cp s3://safegraph-outgoing/verasetcovidmovementusa/backfill/2020/'+ month +'/'+day+'/' + file_name + ' ./ --profile veraset' 

        result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        result.check_returncode()

        newfile_name= re.sub(r'.snappy.parquet', '.csv', file_name) #remove the gzip extension?

        df = pq.read_table(file_name).to_pandas()
        df['subsetter'] = [s[:precision] for s in df['geo_hash']]

        df.reset_index(inplace=True)
        df.set_index('subsetter', drop = True, inplace = True)
        hashset_local = df.index.unique()
        hashset_local = [hash_ for hash_ in hashset if hash_ in hashset_local]
        df = df.loc[hashset_local]
        df.set_index('index', drop = True, inplace=True)

        #subset to hashset
        if np.sum([not isinstance(x,str) for x in df.geo_hash]) >0:
            sys.exit('--Error: {} contains non-strings in the geohash field'.format(newfile_name))

        df.to_csv(newfile_path + newfile_name, index = False)
        #Delete file
        cmd='rm '+file_name
        deleted = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        deleted.check_returncode()
        print('subsetted file {} succesfully'.format(newfile_name))

    e_time = np.round(time.time() - s_time, 0)
    print(e_time)

if __name__ == '__main__':
    main()