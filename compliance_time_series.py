"""
Preparation of time series of compliance metrics
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
    months_path = '../safegraph_social_dist_data/'
    month_list = sorted(os.listdir(months_path))

    #Create dataframes with no rows and single key column for right join
    ts_pct_at_home = pd.DataFrame(columns = ['origin_census_block_group'])
    ts_median_home_dwell_time = pd.DataFrame(columns = ['origin_census_block_group'])
    #Loop through every month
    for month in month_list:
        #Loop through every day
        day_list = sorted(os.listdir(months_path + month))
        for day in day_list:
            file_name = os.listdir(months_path+ '/' + month+'/'+day)[0]
            data = pd.read_csv(months_path+ '/' + month+'/'+day+'/'+file_name,
             dtype={'origin_census_block_group':str})
            var_name = month + '_' + day
            print(var_name)
            #subset to separate dataframes with variable to join
            data_pct_at_home = data[['origin_census_block_group', 'pct_at_home']]
            data_pct_at_home.columns = ['origin_census_block_group', var_name]
            ts_pct_at_home = ts_pct_at_home.merge(data_pct_at_home,
                                                  on = 'origin_census_block_group',
                                                  how = 'right')

            data_median_home_dwell_time = data[['origin_census_block_group', 'median_home_dwell_time']]
            data_median_home_dwell_time.columns = ['origin_census_block_group', var_name]
            ts_median_home_dwell_time = ts_median_home_dwell_time.merge(data_median_home_dwell_time,
                                                                        on = 'origin_census_block_group',
                                                                        how = 'right')

    ts_pct_at_home.to_csv(path_or_buf= 'stats/pct_at_home.csv', index=False)
    ts_median_home_dwell_time.to_csv(path_or_buf= 'stats/median_home_dwell_time.csv', index=False)
    #smoothen with three day average

    #smoothen with census tract average


if __name__ == '__main__':
    main()