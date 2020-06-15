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

def subset_social_dist(soc_dist_path, patterns_path, county, acs_path, backfill = False):
      
    #List files in folder corresponding to month
    month_list = sorted(os.listdir(soc_dist_path))

    #Load population of CBGs 
    df_pops = pd.read_csv(
        acs_path + 'aggregated_acs_cbg_subset.csv',
        dtype={'GEOID':str})
    df_pops.set_index('GEOID', inplace=True, drop=True)
    mask = [idx[:5] == county for idx in df_pops.index]
    df_pops = df_pops.loc[mask, ['B01003_001_Population']]
    df_pops = df_pops.loc[df_pops.B01003_001_Population > 0]
    cbg_pops = df_pops.B01003_001_Population
    cbg_pops.rename_axis('origin_census_block_group', inplace=True)

    date = []
    norm_factors = pd.DataFrame()
    #Loop through every month
    for month in month_list:
        day_list = sorted(os.listdir(soc_dist_path + month))
        for day in day_list:
            file_name = os.listdir(soc_dist_path + month+'/'+day)[0]
            soc_dist_path_county = soc_dist_path + '../social_dist_{}/'.format(county)
            os.makedirs(soc_dist_path_county + '/' + month+'/'+day, exist_ok = True)
            #check if that day has already been subset
            if os.path.isfile(soc_dist_path_county + '/' + month+'/'+day+'/'+file_name) and not backfill:
                print("File {} exists already".format(file_name))
                data = pd.read_csv(
                    soc_dist_path_county + '/' + month+'/'+day+'/'+file_name,
                    dtype = {'origin_census_block_group':str})
            else:
                data = pd.read_csv(soc_dist_path + month+'/'+day+'/'+file_name, dtype={'origin_census_block_group':str})
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
            
            ##Create norm factor, this should be affected by backfill
            #insert cbg_pops column            
            data.set_index('origin_census_block_group', drop=True, inplace=True)            
            cbg_pops = cbg_pops[ [x for x in cbg_pops.index if x in data.index] ]
            data = data.loc[ [x for x in data.index if x in cbg_pops.index] ]
            data.insert(
                loc= len(data.columns),
                column='total_population',
                value=cbg_pops)

            date = month + '-' + day
            norm_factor = data['total_population']/data['candidate_device_count']

            norm_frame = pd.DataFrame(
                {'date':[date]*len(data), 'norm_factor':norm_factor})
            norm_factors = pd.concat([norm_factors, norm_frame])
    
    os.makedirs(soc_dist_path+'../normalization/', exist_ok = True)
    norm_factors.to_csv(
        soc_dist_path+'../normalization/normalization_{}.csv'.format(county),
        index=True)
    return(0)

if __name__ == '__main__':
    print(subset_social_dist(soc_dist_path = '../../social_distancing/social_dist_42101/',
                             patterns_path = '../../weekly_patterns/',
                             county = '42101',
                             backfill = False,
                             acs_path = '../../acs_vars/'))