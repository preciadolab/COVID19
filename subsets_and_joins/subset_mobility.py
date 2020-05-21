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
import pdb
import pyarrow.parquet as pq
import re

def main():
    argument_list = sys.argv[1:]
    short_options = "d:m:k:"
    long_options = ["day", "month", "index"]

    try:
        arguments, values = getopt.getopt(argument_list, short_options, long_options)
    except getopt.error as err:
        # Output error, and return with an error code
        print (str(err))
        sys.exit(2)

    day = '10' #default values
    month = '05' #default values
    for current_argument, current_value in arguments:
        if current_argument in ("-d", "--day"):
            print ("Subsetting for day: " + current_value)
            day= current_value
        elif current_argument in ("-m", "--month"):
            print ("Subsetting for month: " + current_value)
            month = current_value
        elif current_argument in ("-k", "--index"):
            #find k-th day since April 1 
            if int(current_value) in list(range(1,31)):
                month = '04'
                day = str(int(current_value)).zfill(2)
            elif int(current_value) in list(range(31,62)):
                month = '05'
                day = str(int(current_value) - 30).zfill(2)
            print("Subsetting for {}-{} (mm-dd)".format(day, month))

    if int(month) > 5 or (int(month) >= 5 and int(day) >= 4): 
        cmd='aws s3 ls s3://safegraph-outgoing/verasetcovidmovementusa/2020/'+ month +'/'+day+'/ --profile veraset'
    else:
        cmd='aws s3 ls s3://safegraph-outgoing/verasetcovidmovementusa/backfill/2020/'+ month +'/'+day+'/ --profile veraset'

    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    result.check_returncode()

    file_list = [x.split(' ')[-1] for x in result.stdout.split('\n')]
    file_list = sorted([nam for nam in file_list if nam[:4] == 'part'])

    #Create local folder to output filtered data
    newfile_path = '../../veraset-42101/'+ month +'/'+ day +'/'
    os.makedirs(newfile_path, exist_ok=True)

    #Verify that list of hashes is of the same length
    hashlist= ['dr47p', 'dr47q', 'dr47r', 'dr47x', 'dr47z', 'd34e0', 'd34e1', 'd34e2', 'd34e3', 'd34e4',
                   'd34e5', 'd34e6', 'd34e7', 'd34e8', 'd34e9', 'd34eb', 'd34ec', 'd34ed', 'd34ee', 'd34ef',
                   'd34eg', 'd34es', 'd34es', 'd34et', 'd34eu', 'd34ev', 'd346z', 'd34db', 'd34dc', 'd34df',
                   'd34s0', 'd34s5', 'd34sh', 'd34sj', 'd34sn', 'd34sk', 'd34sm']
    if len(set([len(x) for x in hashlist])) != 1:
        sys.exit("NameError: Geohashes are of different length")
    num_digits = len(list(hashlist)[0])

    #Iterate through file_list
    s_time=time.time()
    print("Starting subset on {} chunks".format(len(file_list)))
    j= 0

    for file_name in file_list:
        if int(month) > 5 or (int(month) >= 5 and int(day) >= 4): 
            cmd='aws s3 cp s3://safegraph-outgoing/verasetcovidmovementusa/2020/'+ month +'/'+day+'/' + file_name + ' ./ --profile veraset' 
        else:
            cmd='aws s3 cp s3://safegraph-outgoing/verasetcovidmovementusa/backfill/2020/'+ month +'/'+day+'/' + file_name + ' ./ --profile veraset' 

        result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        result.check_returncode()

        newfile_name= re.sub(r'.snappy.parquet', '.csv', file_name) #remove the gzip extension?

        df = pq.read_table(file_name).to_pandas()
        if np.sum([not isinstance(x,str) for x in df.geo_hash]) >0:
            print(file_name)
            pdb.set_trace()

        df['subsetter'] = [s[:num_digits] for s in df['geo_hash']]

        df.reset_index(inplace=True)
        df.set_index('subsetter', drop = True, inplace = True)
        hashlist_local = df.index.unique()
        df_list = [df.loc[hash_] for hash_ in hashlist if hash_ in hashlist_local]
        
        df = pd.concat(df_list, ignore_index = True)
        df.set_index('index', drop = True, inplace=True)

        #subset to hashlist
        df.to_csv(newfile_path + newfile_name, index = False)
        #Delete file
        cmd='rm '+file_name
        deleted = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
        deleted.check_returncode()
        print('subsetted file {} succesfully'.format(newfile_name))

    e_time = np.round(time.time() - s_time, 0)
    print(e_time)

if __name__ == '__main__':
    main()