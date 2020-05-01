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
    argument_list = sys.argv[1:]
    short_options = "d:m:"
    long_options = ["day", "month"]

    try:
        arguments, values = getopt.getopt(argument_list, short_options, long_options)
    except getopt.error as err:
        # Output error, and return with an error code
        print (str(err))
        sys.exit(2)

    for current_argument, current_value in arguments:
        if current_argument in ("-d", "--day"):
            print ("Subsetting for day: " + current_value)
            k= int(current_value)
            k = str('%02d' % k)
        elif current_argument in ("-m", "--month"):
            print ("Subsetting for month: " + current_value)
            #REJECT IF NOT IN STANDARD FORMAT Mmm
            month = 'Feb'

    #List files in folder corresponding to month
    file_path = '../Phl_Data/'+ month +'/'+ month + k +'/'
    file_list = os.listdir(file_path)
    file_list = sorted([nam for nam in file_list if nam[:4] == 'part'])

    #Keep track of observations, add new users to list, list times for given user, fill list of avg_times
    user_id = []
    avg_times = []
    num_obs = []

    #Initialize auxiliary variables
    j=0 #observations
    ref_user = -1
    ref_time = -1
    cum_time =  0
    i=0
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