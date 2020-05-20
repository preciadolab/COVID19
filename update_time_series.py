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
import json
import re
import math

sys.path.insert(0, 'auxiliary_functions/')
sys.path.insert(0, 'subsets_and_joins/')
sys.path.insert(0, './')
from subset_core_places import subset_core_places
from subset_patterns import subset_patterns
from subset_social_dist import subset_social_dist
from compliance_time_series import *

def main():
    #Sync weekly_patterns files from Safegraph
    print('-- Synching weekly patterns')
    cmd='aws s3 sync s3://sg-c19-response/weekly-patterns/v1/ ../weekly_patterns/ --profile safegraph_consortium'
    result = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, universal_newlines=True)
    result.check_returncode()
    print('-- Finished synching weekly patterns')
    #change any files that say "corrected"
    filenames = [filename for filename in os.listdir('../weekly_patterns/main-file/') if 
                 re.search(r"-corrected" ,filename) is not None]
    newnames = [re.sub(r"-corrected", "", name) for name in filenames]
    for oldname, newname in zip(filenames, newnames):
        cmd='mv ../weekly_patterns/main-file/'+oldname +' ../weekly_patterns/main-file/' + newname
        result = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, universal_newlines=True)
        result.check_returncode()

    #Sync core_places files from Safegraph
    print('-- Synching core places')
    cmd= 'aws s3 sync s3://sg-c19-response/core/2020/'+ time.strftime("%m")+'/ ../core_places/ --profile safegraph_consortium '
    result = subprocess.run(cmd, shell=True, universal_newlines=True)
    result.check_returncode()    
    print('--Finished synching core places')
    #delete current core objects?
    zip_file = [x for x in os.listdir('../core_places/') if re.search(r'CorePlaces', x) is not None][0]
    cmd= 'unzip -o -j ../core_places/'+zip_file+' -d ../core_places/'
    result = subprocess.run(cmd, shell=True, universal_newlines=True)
    result.check_returncode()

    #Sync social distancing metrics 
    print('-- Synching social distancing data')
    cmd='aws s3 sync s3://sg-c19-response/social-distancing/v2/2020/ ../social_distancing/social_dist_global/ --profile safegraph_consortium'
    result = subprocess.run(cmd, shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, universal_newlines=True)
    result.check_returncode()
    print('-- Finished synching social distancing data')

    #Load counties list
    county_list = pd.read_csv('county_list.csv', dtype = {'FIPS':str})
    counties = county_list['FIPS']
    for county in counties:
        #subset core places for county
        print("subsetting core places for county {}".format(county))
        print(subset_core_places(core_path = '../core_places/',
                                 county = county,
                                 backfill=False))

        #subset patterns for county
        print("subsetting weekly patterns for county {}".format(county))
        print(subset_patterns(core_path = '../core_places/',
                              pattern_path = '../weekly_patterns/',
                              county= county,
                              backfill=False))

        #subset social distancing metrics for county
        print("subsetting social distancing metrics for county {}".format(county))
        print(subset_social_dist(soc_dist_path = '../social_distancing/',
                                 patterns_path = '../weekly_patterns/',
                                 county = county,
                                 backfill = True))

        #complete time series for county
        compliance_time_series(county = county,
                               core_path = '../core_places/',
                               patterns_path = '../weekly_patterns/',
                               backfill = True)

    print("Synching time series to bucket")
    cmd='aws s3 sync ./stats/time_series/ s3://edu-upenn-wattslab-covid'
    result = subprocess.run(cmd, shell=True, universal_newlines=True)
    result.check_returncode()   
    print('-- Finished synching time series to bucket')
if __name__ == '__main__':
    main()