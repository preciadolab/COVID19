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
from time import strftime
import io
import json
import re
import math

sys.path.insert(0, 'auxiliary_functions/')
sys.path.insert(0, 'subsets_and_joins/')
sys.path.insert(0, './')
from subset_core_places import subset_core_places
from data_structuring import split_time_series
from subset_patterns import subset_patterns
from subset_social_dist import subset_social_dist
from compliance_time_series import *
from sync_caseloads import sync_caseloads


month_names = ['January', 'February', 'March', 'April',
               'May', 'June', 'July', 'August',
               'September', 'October', 'November', 'December']
def main():
    #Sync weekly_patterns files from Safegraph
    print('-- Synching weekly patterns')
    #Only update 2020 pattern files

    cmd='aws s3 sync s3://sg-c19-response/weekly-patterns/v2/ ../weekly_patterns/ --exclude "*201*" --profile safegraph_consortium'
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
    core_list = os.listdir('../core_places/')
    #obtain filename of latest core places release
    cmd= 'aws s3 ls s3://sg-c19-response/core/2020/{}/ --profile safegraph_consortium '.format(
        strftime("%m"))
    result = subprocess.run(
        cmd,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True)
    result.check_returncode()
    core_filename = [x.split(' ')[-1] for x in result.stdout.split('\n')][0]
    if core_filename in core_list:
        print('-- Core places up to date')
    else:
        cmd= 'aws s3 sync s3://sg-c19-response/core/2020/{}/ ../core_places/ --profile safegraph_consortium '.format(strftime("%m"))
        result = subprocess.run(cmd, shell=True, universal_newlines=True)
        result.check_returncode()    
        print('--Finished synching core places')
        #delete current core objects?
        cmd= 'unzip -o -j ../core_places/'+core_filename+' -d ../core_places/'
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
        print(subset_social_dist(soc_dist_path = '../social_distancing/social_dist_global/',
                                 patterns_path = '../weekly_patterns/',
                                 county = county,
                                 backfill = False,
                                 acs_path = '../acs_vars/'))

        #complete time series for county
        compliance_time_series(county = county,

                               core_path = '../core_places/',
                               patterns_path = '../weekly_patterns/',
                               backfill = False,
                               GEOID_type = 'CT')

        print(split_time_series(county = county, GEOID_type = 'CT'))

        print(sync_caseloads(county = county))

        print('Synching time series for {} to bucket'.format(county))
        cmd='aws s3 sync ../stats/time_series/ s3://edu-upenn-wattslab-covid'
        result = subprocess.run(cmd, shell=True, universal_newlines=True)
        result.check_returncode()   
        print('-- Finished synching time series for {} to bucket'.format(county))
    print('--Updated all time series \n SUCCESS!')
if __name__ == '__main__':
    main()