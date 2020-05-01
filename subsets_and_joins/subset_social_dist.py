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
import json
import math

def parse_time_away_json(x):
    if not isinstance(x, str):
        return(0)
    y = json.loads(x)
    if '<60' in y.keys():
        return(y['<60'])
    else:
        return(0)

def parse_bucketed_distance_json(x):
    if not isinstance(x, str):
        return(0)
    y = json.loads(x)
    if '<1000' in y.keys():
        return(y['<1000'])
    else:
        return(0)

def parse_devices_day_json(x):
    if not isinstance(x, str):
        return(0)
    y = json.loads(x)
    return(np.median(y[7:20]))

def parse_devices_night_json(x):
    if not isinstance(x, str):
        return(0)
    y = json.loads(x)
    return(np.median(y[:7]+y[20:]))

def ratio_fun(series1, series2):
    series2 = series2 + 0.001
    return(np.round(series1/series2, 2))
def main():
      
    #List files in folder corresponding to month
    months_path = '../../safegraph_social_dist_data/'
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
            census_tract = [number[:-1] for number in data['origin_census_block_group']]
            data['census_tract'] = census_tract
            
            data['permanently_away_device_count'] = [parse_time_away_json(x) for x in data['bucketed_home_dwell_time']]
            
            data['pct_at_home'] = np.round(data['completely_home_device_count']/(data['device_count']-data['permanently_away_device_count']), 2)
            
            data['pct_within_neighborhood'] = np.round(([parse_bucketed_distance_json(x) for x in data['bucketed_distance_traveled']] +
                                                 data['completely_home_device_count'])/(data['device_count']-data['permanently_away_device_count']),2)

            #day defined as 7 am to 8 pm
            data['median_devices_home_day'] = [parse_devices_day_json(x) for x in data['at_home_by_each_hour']]
            data['median_devices_home_night'] = [parse_devices_night_json(x) for x in data['at_home_by_each_hour']]

            data['ratio_day_night_device_count'] = ratio_fun(data['median_devices_home_day'], data['median_devices_home_night'])
            #overwrite file 
            print('Overwriting file {}'.format(file_name))
            data.to_csv(path_or_buf= months_path+ '/' + month+'/'+day+'/'+file_name, compression='gzip', index=False)
if __name__ == '__main__':
    main()