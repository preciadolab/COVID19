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

    # acs_path = '../acs_vars/'
    # safegraph_path = '../panel_target_sf_vars/'

    #For each of the target variable panels in safegraph_path we obtain an average of the last k days
    k=7
    data = pd.DataFrame(columns = ['origin_census_block_group'])
    for file_name in os.listdir(safegraph_path):
        safegraph_data = pd.read_csv(safegraph_path + file_name, dtype={'origin_census_block_group':str})
        #create new variable averaging over last k columns, with file name
        var_name = file_name[:-4]
        df = safegraph_data[safegraph_data.columns[-k:]]
        var = df.mean(axis=1)

        safegraph_data[var_name] = var
        safegraph_data = safegraph_data[['origin_census_block_group', var_name]]

        data = data.merge(safegraph_data, on = 'origin_census_block_group', how = 'right')
    #Create variables at the census tract level
    tract_geoid = [x[:-1] for x in data['origin_census_block_group']]
    data_ct = data.copy()
    data_ct['tract_geoid'] = tract_geoid
    data_ct.drop(columns = 'origin_census_block_group', inplace = True)
    data_ct.reset_index(drop=True)
    print("Finished parsing safegraph data")

    #Average aggregating at the tract_geoid level
    data_ct = data_ct.groupby(['tract_geoid'], as_index=False).mean()
    #Loop through acs variables and append to corresponding dataframes
    file_list = sorted([name for name in os.listdir(acs_path) if (name[-8:]=='_CBG.csv' or name[-7:]=='_CT.csv')])
    print('Looping through {} files for merge.'.format(len(file_list)))
    for file_name in file_list:
        #subset to Philadelphia
        current_data = pd.read_csv( acs_path + file_name, dtype={'GEOID':str})
        current_data = current_data[['GEOID', 'estimate']]
        indexes = [i for i, geoid in enumerate(current_data['GEOID']) if geoid[:5]=='42101']
        current_data = current_data.loc[indexes]
        current_data = current_data.reset_index(drop=True)
        #Get Variable Name
        if file_name[-8:]=='_CBG.csv':
            var_name = file_name[:-8]
            current_data.columns = ['origin_census_block_group', var_name]
            if len(current_data) < 1000:
                print("There is a problem with file {}, it only has {} rows for Philadelphia".format(file_name, len(current_data)))
            else:
                data = pd.merge(data, current_data, how='left', on=['origin_census_block_group'])
        else:
            var_name = file_name[:-7]
            current_data.columns = ['tract_geoid', var_name]
            if len(current_data) > 1000:
                print("There is a problem with file {}, it has {} rows for Philadelphia".format(file_name, len(current_data)))
            else:
                data_ct = pd.merge(data_ct, current_data, how='left', on=['tract_geoid'])

    print('Finished merging {} variables'.format(len(data.columns)-1))
    print(data)
    print(data_ct)
    #Remove columns full of NaN
    data = data.dropna(axis=1, how= 'all')
    data_ct = data_ct.dropna(axis=1, how= 'all')
    #Fill in NaN values with average (THIS NEEDS TO BE FIXED FOR CATEGORICAL VARS)
    data.fillna(data.mean())
    data_ct.fillna(data.mean())

    data.to_csv(path_or_buf= acs_path + 'CBG_joint.csv', index=False)
    data_ct.to_csv(path_or_buf= acs_path + 'CT_joint.csv', index=False)
    return 0

if __name__ == '__main__':
    main()