"""
Function to obtain descriptive statistics at the observation level
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

def main():
    #List files in folder corresponding to month
    argument_list = sys.argv[1:]
    short_options = "a:s:f:"
    long_options = ["acs_path", "sg_path", "sg_file"]

    try:
        arguments, values = getopt.getopt(argument_list, short_options, long_options)
    except getopt.error as err:
        # Output error, and return with an error code
        print (str(err))
        sys.exit(2)

    for current_argument, current_value in arguments:
        if current_argument in ("-a", "--acs_path"):
            print ("Finding ACS data in: " + current_value)
            acs_path = str(current_value)
        elif current_argument in ("-s", "--sg_path"):
            print ("Finding Safegraph data in: " + current_value)
            #REJECT IF NOT IN STANDARD FORMAT Mmm
            safegraph_path = str(current_value)
        elif current_argument in ("-f", "--sg_file"):
            print ("Using Safegraph file: " + current_value)
            safegraph_file = str(current_value)
        else: #defaults
            acs_path = '../acs_vars/'
            safegraph_path = '../safegraph_social_dist_data/04/15/'
            safegraph_file = '2020-04-15-social-distancing.csv.gz'

    safegraph_data = pd.read_csv(safegraph_path + safegraph_file, dtype={'origin_census_block_group':str})
    #Create pct_at_home and discard rest
    safegraph_data['pct_home']= safegraph_data['completely_home_device_count']/safegraph_data['device_count']
    data = safegraph_data[['origin_census_block_group','pct_home']]
    #Loop through variables ending in CGB
    file_list = sorted([name for name in os.listdir(acs_path) if (name[-8:]=='_CBG.csv' or name[-7:]=='_CT.csv')])
    print('Looping through {} files for merge.'.format(len(file_list)))
    for file_name in file_list:
        #Get Variable Name
        if file_name[-8:]=='_CBG.csv':
            var_name = file_name[:-8]
        else:
            var_name = file_name[:-7]
        current_data = pd.read_csv( acs_path + file_name, dtype={'GEOID':str})
        current_data = current_data[['GEOID', 'estimate']]
        indexes = [i for i, geoid in enumerate(current_data['GEOID']) if geoid[:5]=='42101']
        current_data = current_data.loc[indexes]
        current_data = current_data.reset_index(drop=True)
        #Change variable names
        current_data.columns = ['origin_census_block_group', var_name]
        #Join with data (if number of rows is as expected)
        if len(current_data) > 1300:
            if file_name[-7:]=='_CT.csv':
                print('File {} has a swapped name'.format(file_name))
            data = pd.merge(data, current_data, how='left', on=['origin_census_block_group'])
        else:
            if file_name[-8:]=='_CBG.csv':
                print('File {} has a swapped name'.format(file_name))      
    print('Finished merging {} variables'.format(len(data.columns)-1))
    print(data)

    data.to_csv(path_or_buf= acs_path + 'joint_acs.csv', index=False)
    return 0

if __name__ == '__main__':
    main()