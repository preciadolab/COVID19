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

def parse_within_cbg(x, own_cbg):
	if not isinstance(x, str):
		return(0)
	y = json.loads(x)
	if own_cbg in y.keys():
		return(int(y[own_cbg]))
	return(0)

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
			#Define new variables (only those that need to parse a JSON)            
            data['within_cbg_count'] = [parse_within_cbg(x, cbg) for (cbg, x) in zip(data['origin_census_block_group'], data['destination_cbgs'])]            
            data['within_neighborhood'] = [parse_bucketed_distance_json(x) for x in data['bucketed_distance_traveled']]

            #overwrite file 
            print('Overwriting file {}'.format(file_name))
            data.to_csv(path_or_buf= months_path+ '/' + month+'/'+day+'/'+file_name, compression='gzip', index=False)
if __name__ == '__main__':
    main()