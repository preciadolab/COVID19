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
    #Obtain safegraph adress as a function of the index
    cmd='aws s3 ls s3://safegraph-outgoing/movement-sample-global/feb2020/2020/02/'+ k +'/ --profile safegraph'
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    result.check_returncode()

    file_list = [x.split(' ')[-1] for x in result.stdout.split('\n')]
    file_list = sorted([nam for nam in file_list if nam[:4] == 'part'])

    #Define country codes to analyze
    country_code = 'US'

    #Keep track of observations, add new users to list, list times for given user, fill list of avg_times
    user_id = []
    num_obs = []

    #Initialize auxiliary variables
    j=0 #observations
    ref_user = -1
    ref_time = -1
    #Loop through files and lines in them
    for file_name in file_list:
        #Copy file from amazon bucket
        cmd='aws s3 cp s3://safegraph-outgoing/movement-sample-global/feb2020/2020/02/'+ k +'/' + file_name + ' ./ --profile safegraph'
        result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        result.check_returncode()

        p = subprocess.Popen(
            ["zcat", file_name],
            stdout=subprocess.PIPE
        )
        #get rid of header line
        header = p.stdout.readline()
        for line in p.stdout:
            #parse user to 
            curr_user = line.decode().split(',')[0]
            curr_country = line.decode().split(',')[7]
            #Obtain user and compare to previous user
            if  curr_user != ref_user:
                user_id.append(curr_user) #add user to list
                if j!= 0:
                    ref_user = curr_user
            j = j+1
        #Add avg_times and num_obs for last user
        print('Finished parsing file: ' + file_name)

    with open('summary', 'w+') as resultfile:
        resultfile.write('Number of users: '+ str(len(user_id))+'\n')
        resultfile.write('Total observations: '+str(j)+'\n')
        resultfile.write('Avg. observations per user: '+ str(np.round(j/(len(user_id)),2))+'\n')
    return 0

if __name__ == '__main__':
    main()