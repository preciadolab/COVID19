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
    
def readInSiteVisitLists(path_to_json):
    all_files = os.listdir(path_to_json)
    unique_visitors = []
    for filename in all_files:
        with open(path_to_json + filename) as fp:
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
                    visitorDict[visitor].update({geohash:(entry2.utc_timestamp - entry1.utc_timestamp)/60})
                else:
                    visitorDict[visitor][geohash] = visitorDict[visitor][geohash] + (entry2.utc_timestamp - entry1.utc_timestamp)/60
    return visitorDict


def findHome(month,day,path_to_veraset,path_to_json, precision = 7, t1 = 20, t2 = 5, k = None):
    unique_visitors = readInSiteVisitLists(path_to_json)
    begin_timestamp, end_timestamp = nightTimeStamp(t1,t2,month,day-1)    
    visitorDict = {}
    all_files = os.listdir(path_to_veraset+month+'/'+ day +'/')
    all_files = [name for name in all_files if re.search(r'part', name) is not None]
    
    for filename in all_files: # reads in each file and finds visitors and site visits
        userLocationTimes = pd.read_csv(path_to_veraset+month+'/'+ day +'/'+filename, index_col = 'caid')

        visitorsInFile = userLocationTimes.index.intersection(unique_visitors).unique()
        subsetOfFile = userLocationTimes.loc[userLocationTimes.index.intersection(unique_visitors).unique()]

        pdb.set_trace()
        for i in range(len(visitorsInFile)):
            visitor = visitorsInFile[i]
            entriesForVisitor = subsetOfFile.loc[visitor]
            if len(entriesForVisitor.geo_hash) != 9:
                visitorDict = geoHashTimesForVisitor(visitor,entriesForVisitor,visitorDict,precision,begin_timestamp,end_timestamp)
        return visitorDict
    
if __name__ == '__main__':
    findHome(month = '05',
             day = '07',
             path_to_veraset ='../../veraset-42101/',
             path_to_json ='../../stats/findVisitsResults/')
        
