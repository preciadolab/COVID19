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
import pdb
import time
import io

def main():
      
    #List files in folder corresponding to month
    months_path = '../SocialDistancingMetrics/'
    month_list = sorted(os.listdir(months_path))

    #Loop through every month
    for month in month_list:
        day_list = sorted(os.listdir(months_path + month))
        for day in day_list:
            file_name = os.listdir(months_path+ '/' + month+'/'+day)[0]
            data = pd.read_csv(months_path+ '/' + month+'/'+day+'/'+file_name, dtype={'origin_census_block_group':str})
            #Subset to 5 digits and discard non Philadelphia
            indexes = [i for i, number in enumerate(data['origin_census_block_group']) if number[:5] == '42101']
            data = data.loc[indexes]
            data.reset_index()
            #Create column for census tract and column for census block
            census_tract = [number[5:11] for number in data['origin_census_block_group']]
            data['census_tract'] = census_tract
            #overwrite file 
            print('Overwriting file {}'.format(file_name))
            data.to_csv(path_or_buf= months_path+ '/' + month+'/'+day+'/'+file_name, compression='gzip', index=False)
if __name__ == '__main__':
    main()