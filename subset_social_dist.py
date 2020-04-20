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
import time
import io

def main():
      
    #List files in folder corresponding to month
    months_path = '../SocialDistancingMetrics/'
    month_list = sorted(os.listdir(months_path))

    #Loop through every month
    for month in month_list:
        day_list = sorted(os.listdir(months_path + month))
        for day in day_list:
            file_name = os.listdir(months_path+ '/' + month+'/'+day)[0]
            print(file_name)

    #Iterate through file_list
    print("Starting subset on month {}, day {}.".format(month, day))
    j= 0
    for file_name in file_list:
        cmd='aws s3 cp s3://safegraph-outgoing/movement-sample-global/feb2020/2020/02/'+ k +'/' + file_name + ' ./ --profile safegraph'
        result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        result.check_returncode()

        newfile_name= file_name #remove the gzip extension
        #open pipe to read gz file, currently in working directory
        p = subprocess.Popen(
            ["zcat", file_name],
            stdout=subprocess.PIPE
        )
        with gz.open(newfile_path + newfile_name, 'wb+') as newfile:
            #read header write header
            header = p.stdout.readline()
            newfile.write(header)
            #counter for observations in hashlist
            for line in p.stdout:
                #parse geohash to num_digits
                if line.decode().split(',')[-2][0:num_digits] in hashlist:
                    newfile.write(line)
                    j= j+1
        #Delete file
        cmd='rm '+file_name

        deleted = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        deleted.check_returncode()

    with open(newfile_path + 'results', 'w+') as resultfile:
        resultfile.write('Elapsed seconds: '+str(e_time)+'\n')
        resultfile.write('Total observations: '+str(j)+'\n')

if __name__ == '__main__':
    main()