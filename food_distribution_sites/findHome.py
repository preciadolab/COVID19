# -*- coding: utf-8 -*-
"""
Created on Fri May 15 09:42:11 2020

@author: Abhinav Ramkumar, Victoria Fethke, Yi-An Hsieh
"""
import json
import datetime
from dateutil import tz
import pandas as pd
import os
import re
import pdb

'''
Functions for program
'''

'''
Reads in night time limits (e.g. 8 pm - 8 am) in EST, converts them to 1970 timestamp for particular day
'''

def nightTimeStamp(t1,t2,month,day):
    # datetime format of opening hours
    begin = datetime.datetime(2020,int(month),int(day)-1, t1)
    end = datetime.datetime(2020,int(month),int(day), t2)
    
    # find timestamps of night limits on that day
    from_zone = tz.gettz('America/New_York')

    begin_timestamp = begin.replace(tzinfo=from_zone).timestamp()
    end_timestamp = end.replace(tzinfo=from_zone).timestamp()
    
    return [begin_timestamp, end_timestamp]

'''
Read ALL food_visits files (one by one) and obtain a long list of unique visitors
'''
    
def readInSiteVisitLists(path_json):
    all_files = os.listdir(path_json)
    unique_visitors = []
    for filename in all_files:
        with open(path_json + filename) as fp:
            visitorDict = json.load(fp)
        visitors = [x for y in [v['visitors'] for k,v in visitorDict.items()] for x in y]
        unique_visitors.append(list(set(visitors)))
    unique_visitors = [x for y in unique_visitors for x in y]
    unique_visitors = list(set(unique_visitors))
    return unique_visitors


def geoHashTimesForVisitor(visitor,entriesForVisitor,visitorDict,precision,begin_timestamp,end_timestamp):
    for i in range(len(entriesForVisitor)-1):
        entry1 = entriesForVisitor.iloc[i,:]
        entry2 = entriesForVisitor.iloc[i+1,:]
        if entry1.geo_hash[:precision] == entry2.geo_hash[:precision]:
            t1 = entry1.utc_timestamp
            t2 = entry2.utc_timestamp
            if  (t1 >= begin_timestamp and t1 <= end_timestamp) and (t2 >= begin_timestamp and t2 <= end_timestamp):
                geohash = entry1.geo_hash[:precision]
                if (visitor in visitorDict) == False:
                     visitorDict[visitor] = {}
                if (geohash in visitorDict[visitor]) == False:
                    visitorDict[visitor].update({geohash:(entry2.utc_timestamp - entry1.utc_timestamp)/60}) #minutes
                else:
                    visitorDict[visitor][geohash] = visitorDict[visitor][geohash] + (entry2.utc_timestamp - entry1.utc_timestamp)/60 #minutes
    #prune visits of less than a minute
    if visitor in visitorDict.keys():
        visitorDict[visitor] = { k:v for k, v in visitorDict[visitor].items() if v > 1}
        if len(visitorDict[visitor]) == 0:
            visitorDict.pop(visitor, None)
    return visitorDict


def findHome(month,day,path_veraset,path_json, path_output, precision = 7, t1 = 20, t2 = 5):
    unique_visitors = readInSiteVisitLists(path_json)
    begin_timestamp, end_timestamp = nightTimeStamp(t1,t2,month,day)    
    visitorDict = {}
    all_files = sorted(os.listdir(path_veraset+month+'/'+ day +'/'))
    all_files = sorted([name for name in all_files if re.search(r'part', name) is not None])
    
    for filename in all_files: # reads in each file and finds visitors and site visits
        userLocationTimes = pd.read_csv(path_veraset+month+'/'+ day +'/'+filename, index_col = 'caid')

        visitorsInFile = userLocationTimes.index.intersection(unique_visitors).unique()
        indexes = userLocationTimes.index.intersection(unique_visitors).unique().tolist()
        subsetOfFile = userLocationTimes.loc[indexes]

        for visitor in visitorsInFile:
            entriesForVisitor = subsetOfFile.loc[[visitor]]
            visitorDict = geoHashTimesForVisitor(visitor,entriesForVisitor,visitorDict,
                                                 precision,begin_timestamp,end_timestamp)
        print("Finished scanning chunk: {}".format(filename))

    os.makedirs(path_output, exist_ok=True)
    with open(path_output + 'home_freqs_{}-{}.json'.format(month,day), 'w+') as fp:
        json.dump(visitorDict, fp)
    print('--Finished finding home frequencies for {}-{}'.format(month, day))
    pdb.set_trace()
    return visitorDict

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
    k = None
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
            print("Subsetting for {}-{} (mm-dd)".format(month, day))

    findHome(month = month,
             day = day,
             path_veraset ='../../veraset-42101/',
             path_json ='../../stats/findVisitsResults/',
             path_output = '../../stats/findHomeResults/')

if __name__ == '__main__':
    main()
