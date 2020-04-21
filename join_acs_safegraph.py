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

def main():
    #List files in folder corresponding to month
    acs_path = '../acs_vars/'
    safegraph_path = '../safegraph_social_dist_data/04/15/'

    #Load safegraph dataframe
    safegraph_file = '2020-04-15-social-distancing.csv.gz'
    safegraph_data = pd.read_csv(safegraph_path + safegraph_file, dtype={'origin_census_block_group':str})
    #Create pct_at_home and discard rest
    safegraph_data['pct_home']= safegraph_data['completely_home_device_count']/safegraph_data['device_count']
    data = safegraph_data[['origin_census_block_group','pct_home']]
    #Loop through variables ending in CGB
    file_list = sorted([name for name in os.listdir(acs_path) if name[-8:]=='_CBG.csv'])
    for file_name in file_list:
        #Get Variable Name
        var_name = file_name[:-8]
        current_data = pd.read_csv( acs_path + file_name, dtype={'GEOID':str})
        current_data = current_data[['GEOID', 'estimate']]
        indexes = [i for i, geoid in enumerate(current_data['GEOID']) if geoid[:5]=='42101']
        current_data = current_data.loc[indexes]
        current_data = current_data.reset_index()
        #Change variable names 
        current_data.columns = ['origin_census_block_group', var_name]
        #Join with data (if number of rows is as expected)
        if len(current_data) > 1300:
            data = pd.merge(data, current_data, how='left', on=['origin_census_block_group'])
        else:
            print('Homa made a mistake:')
            print(var_name)
    #Loop through files and lines in them
    for file_name in file_list:
        #open pipe to read gz file, in file_path
        p = subprocess.Popen(
            ["zcat", file_path + file_name],
            stdout=subprocess.PIPE
        )

        header = p.stdout.readline()
        for line in p.stdout:
            #parse user to 
            curr_user = line.decode().split(',')[0]
            curr_time = int(line.decode().split(',')[5])
            #Obtain user and compare to previous user
            if  curr_user != ref_user:
                user_id.append(curr_user) #add user to list
                if j!= 0:
                    num_obs.append(i) #add number of obs for previous user
                    if i!=1:
                        avg_times.append(cum_time/(i-1))
                    else:
                        avg_times.append('NaN')
                i=0 #user observations counter
                ref_time = curr_time #first time for user
                ref_user = curr_user
                cum_time = 0
            j = j+1
            i = i+1
            cum_time = cum_time + (curr_time - ref_time)
            ref_time = curr_time
        #Add avg_times and num_obs for last user
            num_obs.append(i) #add number of obs for previous user
            if i!=1:
                avg_times.append(cum_time/(i-1))
            else:
                avg_times.append('NaN')
        print('Finished parsing file: ' + file_name)

    with open(file_path + 'summary', 'w+') as resultfile:
        resultfile.write('Number of users: '+ str(len(user_id))+'\n')
        resultfile.write('Total observations: '+str(j)+'\n')
        resultfile.write('Avg. observations per user: '+ str(np.round(j/(len(user_id)),2))+'\n')
        avg_avg_time = np.mean([time_ for time_ in avg_times if time_ != 'NaN'])
        resultfile.write('Avg. interval per user: '+str(np.round(avg_avg_time,2))+'\n')
    return 0

if __name__ == '__main__':
    main()