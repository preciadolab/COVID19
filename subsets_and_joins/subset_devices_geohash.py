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

    #Create destination folder for safegraph data (downloaded one at a time)
    #Obtain safegraph adress as a function of the index
    cmd='aws s3 ls s3://safegraph-outgoing/movement-sample-global/feb2020/2020/02/'+ k +'/ --profile safegraph'
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    result.check_returncode()

    file_list = [x.split(' ')[-1] for x in result.stdout.split('\n')]
    file_list = sorted([nam for nam in file_list if nam[:4] == 'part'])

    #Create local folder corresponding to day
    newfile_path = '../Phl_Data/'+ month +'/'+ month + k +'/'
    os.makedirs(newfile_path, exist_ok=True)

    #Verify that list of hashes is of the same length
    hashlist= set(['dr47p', 'dr47q', 'dr47r', 'dr47x', 'dr47z', 'd34e0', 'd34e1', 'd34e2', 'd34e3', 'd34e4',
                   'd34e5', 'd34e6', 'd34e7', 'd34e8', 'd34e9', 'd34eb', 'd34ec', 'd34ed', 'd34ee', 'd34ef',
                   'd34eg', 'd34es', 'd34es', 'd34et', 'd34eu', 'd34ev', 'd346z', 'd34db', 'd34dc', 'd34df',
                   'd34s0', 'd34s5', 'd34sh', 'd34sj', 'd34sn', 'd34sk', 'd34sm'])
    if len(set([len(x) for x in hashlist])) != 1:
        sys.exit("NameError: Geohashes are of different length")
    num_digits = len(list(hashlist)[0])

    #Iterate through file_list

    s_time=time.time()
    print("Starting subset on {} chunks".format(len(file_list)))
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

    e_time = np.round(time.time() - s_time, 0)
    with open(newfile_path + 'results', 'w+') as resultfile:
        resultfile.write('Elapsed seconds: '+str(e_time)+'\n')
        resultfile.write('Total observations: '+str(j)+'\n')

if __name__ == '__main__':
    main()