"""
Function to obtain descriptive statistics of the sparsity of measurements
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
    short_options = "m:"
    long_options = ["month"]
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dic']
    try:
        arguments, values = getopt.getopt(argument_list, short_options, long_options)
    except getopt.error as err:
        # Output error, and return with an error code
        print (str(err))
        sys.exit(2)

    for current_argument, current_value in arguments:
        if current_argument in ("-m", "--month"):
            print ("Subsetting for month: " + current_value)
            #REJECT IF NOT IN STANDARD FORMAT Mmm
            if current_value not in months:
                print("Error: Month must be in Mmm format")
                sys.exit(2)
            month = current_value

    #List files in folder corresponding to month
    file_path = '../../safegraph_social_dist_data/'+ str(months.index(month) + 1).zfill(2) +'/'
    file_list = os.listdir(file_path)
    file_list = sorted([day for day in file_list])
    #Iterate through days and obtain metrics
    for day in file_list:
        file_name = os.listdir(file_path + day + '/')[0]
        data = pd.read_csv( file_path + day + '/' + file_name)
        print([np.round(np.sum(data['device_count'] / np.sum(data['candidate_device_count'])), 2) ,
            np.round(np.sum(data['within_cbg_count'] / np.sum(data['candidate_device_count'])), 2), 
            np.round(np.sum(data['within_cbg_count'] / np.sum(data['device_count'])), 2)])


if __name__ == '__main__':
    main()