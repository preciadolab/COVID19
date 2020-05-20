# -*- coding: utf-8 -*-
"""
Created on Fri May 15 09:42:11 2020

@author: Abhinav Ramkumar
"""
import json
import datetime
from dateutil import tz
import pandas as pd
import glob
import os
import re

'''
Functions for program
'''

'''
Reads in night time limits (e.g. 8 pm - 8 am) in EST, converts them to 1970 timestamp for particular day
'''

def nightTimeStamp(t1,t2,month,day):
    
    # datetime format of opening hours
    begin = datetime.datetime(2020,int(month),day, t1)
    end = datetime.datetime(2020,int(month),day+1, t2)
    
    # find timestamps of night limits on that day
    from_zone = tz.gettz('America/New_York')

    begin_timestamp = begin.replace(tzinfo=from_zone).timestamp()
    end_timestamp = end.replace(tzinfo=from_zone).timestamp()
    
    return [begin_timestamp,end_timestamp]

'''
Read ALL food_visits files (one by one) and obtain a long list of unique visitors
'''
    
def readInSiteVisitLists(month, day_begin, day_end):
    unique_visitors = []
    for day in range(day_begin,day_end+1):
        filename = '..\stats\\findVisitsResults\\food_visits_{}-{}.json'.format(month,str(day))
        with open(filename) as fp:
            visitorDict = json.load(fp)
        visitors = [x for y in [v['visitors'] for k,v in visitorDict.items()] for x in y]
        #print(visitors)
        unique_visitors.append(list(set(visitors)))
    unique_visitors = [x for y in unique_visitors for x in y]
    unique_visitors = list(set(unique_visitors))
    return unique_visitors


def geoHashTimesForVisitor(visitor,entriesForVisitor,visitorDict,prec,begin_timestamp,end_timestamp):
    for i in range(len(entriesForVisitor)-1):
        entry1 = entriesForVisitor.iloc[i,:]
        entry2 = entriesForVisitor.iloc[i+1,:]
        if entry1.geo_hash[:prec] == entry2.geo_hash[:prec]:
            t1 = entry1.utc_timestamp
            t2 = entry2.utc_timestamp
            if  (t1 >= begin_timestamp and t1 <= end_timestamp) and (t2 >= begin_timestamp and t2 <= end_timestamp):
                geohash = entry1.geo_hash[:prec]
                if (visitor in visitorDict) == False:
                     visitorDict[visitor] = {}
                if (geohash in visitorDict[visitor]) == False:
                    visitorDict[visitor].update({geohash:(entry2.utc_timestamp - entry1.utc_timestamp)/60})
                else:
                    visitorDict[visitor][geohash] = visitorDict[visitor][geohash] + (entry2.utc_timestamp - entry1.utc_timestamp)/60
    return visitorDict

#main method
def readInVisitorLists(t1,t2,month,day_of_interest,day_begin,day_end,prec,path):
    unique_visitors = readInSiteVisitLists(month,day_begin,day_end)
    begin_timestamp, end_timestamp = nightTimeStamp(t1,t2,month,day_of_interest-1)    
    visitorDict = {}
    all_files = os.listdir(path+month+'\\'+str(day_of_interest)+'\\')
    all_files = [name for name in all_files if re.search(r'part', name) is not None]
    for filename in all_files: # reads in each file and finds visitors and site visits
        userLocationTimes = pd.read_csv(path+month+'\\'+str(day_of_interest)+'\\'+filename,index_col = 'caid', header = 0)
        #Traverse files row by row
        #subset to users in visitors
        visitorsInFile = userLocationTimes.index.intersection(unique_visitors).unique()
        subsetOfFile = userLocationTimes.loc[userLocationTimes.index.intersection(unique_visitors).unique()]
        #MAKE SURE THE RESULTING FILE IS SORTED BY USER AND THEN BY TIME
        #TRAVERSE ROW BY ROW and differenceDWELL EVENTS FROM COMMUTING
        for i in range(len(visitorsInFile)):
            visitor = visitorsInFile[i]
            entriesForVisitor = subsetOfFile.loc[visitor]
            if len(entriesForVisitor.geo_hash) != 9:
                visitorDict = geoHashTimesForVisitor(visitor,entriesForVisitor,visitorDict,prec,begin_timestamp,end_timestamp)
    return visitorDict

visitorDict = readInVisitorLists(20,5,'02',24,24,24,7,'..\data\Veraset\\')

#if __name__ == '__main__':
#    findHome(20,5,'02',24,24,24,7,'..\data\Veraset\\')
