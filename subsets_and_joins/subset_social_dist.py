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
sys.path.insert(0, '../auxiliary_functions/')
import json_code as jjj

def subset_social_dist(soc_dist_path, patterns_path, county, backfill = False):
      
    #List files in folder corresponding to month
    soc_dist_path_global = soc_dist_path + 'social_dist_global/'
    month_list = sorted(os.listdir(soc_dist_path_global))

    date = []
    norm_factor = []
    #Loop through every month
    for month in month_list:
        day_list = sorted(os.listdir(soc_dist_path_global + month))
        for day in day_list:
            file_name = os.listdir(soc_dist_path_global + month+'/'+day)[0]
            soc_dist_path_county = soc_dist_path + 'social_dist_{}/'.format(county)
            os.makedirs(soc_dist_path_county + '/' + month+'/'+day, exist_ok = True)
            #check if that day has already been subset
            if os.path.isfile(soc_dist_path_county + '/' + month+'/'+day+'/'+file_name) and not backfill:
                print("File {} exists already".format(file_name))
                data = pd.read_csv(soc_dist_path_county + '/' + month+'/'+day+'/'+file_name)
            else:
                data = pd.read_csv(soc_dist_path_global + month+'/'+day+'/'+file_name, dtype={'origin_census_block_group':str})
                #Subset to 5 digits and discard non Philadelphia
                indexes = [i for i, number in enumerate(data['origin_census_block_group']) if number[:5] == county]
                data = data.loc[indexes]
                data.reset_index()
                #Define new variables (only those that need to parse a JSON)            
                data['within_cbg_count'] = [jjj.parse_within_cbg(x, cbg) for (cbg, x) in zip(data['origin_census_block_group'], data['destination_cbgs'])]            
                data['within_neighborhood'] = [jjj.parse_bucketed_distance_json(x) for x in data['bucketed_distance_traveled']]

                #Create file for given county
                print('Writing file {}'.format(file_name))
                data.to_csv(path_or_buf= soc_dist_path_county + '/' + month+'/'+day+'/'+file_name, compression='gzip', index=False)
            
            date = date + [month + '-' + day]
            norm_factor = norm_factor + [np.sum(data['candidate_device_count'])/np.sum(data['device_count'])]

    norm_df = pd.DataFrame({'date':date, 'normalization_factor':norm_factor})
    os.makedirs(patterns_path+'normalization_stats-{}/'.format(county), exist_ok = True)
    norm_df.to_csv(patterns_path+'normalization_stats-{}/2020-daily.csv'.format(county), index=False)
    #Export relevant daily and weekly normalization stats
    #List the 168 hours with normalization factors for a given week
    pattern_dates = [x[5:10] for x in sorted(os.listdir(patterns_path+'main-file-'+ county +'/'))]
    for date in pattern_dates:
        #Find index for such date
        i = norm_df['date'].tolist().index(date)
        hourly_list = [ np.array([norm_df['normalization_factor'][j]]*24).reshape((24,1)) for j in range(i,i+7)]
        hourly_factors = pd.DataFrame(np.vstack(hourly_list), columns = ['normalization_factor'])
        hourly_factors.to_csv(patterns_path + 'normalization_stats-{}/2020-{}-normalization.csv'.format(county, date))
    return(0)
