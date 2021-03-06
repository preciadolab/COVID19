# -*- coding: utf-8 -*-
"""
Created on Fri May  8 19:27:47 2020

@author: Abhinav Ramkumar, Victoria Fethke, Yi-An Hsieh
"""

"""
Site Visit Identification:
    
This code takes in the shape files corresponding to sites,
and checks if a given user visited any of the sites during the specified windows
that it is open. If there is a visit, this code reports the user, the site,
and the time of the visit. 
"""

import json
import pandas as pd
import numpy as np
import pdb
import os
from findVisitFunctions import convertToEasternTime, checkVisits, point_to_circle_geohash, shp_into_dict
import getopt, sys

def findVisits(day, month, path_veraset, path_output, path_meals):
    """
    Read in the csv files and stores the geohash and time of the visit, sequentially
    """
    ############################################################

    all_files = os.listdir(path_veraset + month+'/'+day+'/')
    all_files = [name for name in all_files if re.search(r'part', name) is not None]
    
    '''
    Initialization Block
    '''
    shp_dict_all = shp_into_dict(path_meals + 'OtherMealSites_All')
    shp_dict_youths = shp_into_dict(path_meals + 'YouthMealSites_All')

    siteLocationTimes = point_to_circle_geohash(path_meals + 'OtherMealSites_All',50,8)
    visitorDict = { geohash:{'name':name,'visits':0,'visitors':[]} for name, geohash in zip(siteLocationTimes.index.tolist(),siteLocationTimes.geo_hash.tolist())} 
    siteVisits = {name:{'visits':0} for name in siteLocationTimes.index.unique().tolist()}
    print('Finding visits for {} locations'.format(len(visitorDict)))
    totalVisits = 0 # initialize total number of visits per day.
    
    for filename in all_files: # reads in each file and finds visitors and site visits
        userLocationTimes = pd.read_csv(path_veraset+month+'/'+day+'/'+filename , index_col = None)
        if np.sum([not isinstance(x,str) for x in userLocationTimes.geo_hash]) >0:
            sys.exit('--Error: {} contains non-strings in the geohash field'.format(filename))

        userLocationTimes['converted_times'] = convertToEasternTime(userLocationTimes)  # This part takes in the time from 1970 and converts it to a day-time object.        
        visitorDict, siteVisits, totalVisits = checkVisits(userLocationTimes,siteLocationTimes,visitorDict,siteVisits,totalVisits,8)

    os.makedirs(path_output, exist_ok=True)
    with open(path_output + 'food_visits_{}-{}.json'.format(month,day), 'w+') as fp:
        json.dump(visitorDict, fp)
    print('--Finished finding visits for {}-{}, found {} visits'.format(month, day, totalVisits))

def main():
    argument_list = sys.argv[1:]
    short_options = "k:"
    long_options = ["index"]

    try:
        arguments, values = getopt.getopt(argument_list, short_options, long_options)
    except getopt.error as err:
        # Output error, and return with an error code
        print (str(err))
        sys.exit(2)

    k = 1
    for current_argument, current_value in arguments:
        if current_argument in ("-k", "--index"):
            #find k-th day since April 1 
            if int(current_value) in list(range(1,31)):
                month = '04'
                day = str(int(current_value)).zfill(2)
            elif int(current_value) in list(range(31,62)):
                month = '05'
                day = str(int(current_value) - 30).zfill(2)
            print("Subsetting for {}-{} (mm-dd)".format(month, day))

    findVisits(day=day,
               month=month,
               path_veraset='../../veraset-42101/',
               path_meals='../../food_sites/',
               path_output='../../stats/findVisitsResultsPR/',
               k = k) 

 if __name__ == '__main__':
    main()

