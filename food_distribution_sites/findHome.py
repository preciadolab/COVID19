# -*- coding: utf-8 -*-
"""
Created on Fri May 15 09:42:11 2020

@author: Abhinav Ramkumar
"""

import datetime
import csv
from dateutil import tz
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import glob

'''
Functions for program
'''

'''
Reads in times in EST, converts them to 1970 timestamp
'''

def nightTimeStamp(t1,t2,month,day):
    
    # datetime format of opening hours
    begin = datetime.datetime(2020,month,day, t1)
    end = datetime.datetime(2020,month,day+1, t2)
    
    # find timestamps of night limits on that day
    from_zone = tz.gettz('America/New_York')

    begin_timestamp = begin.replace(tzinfo=from_zone).timestamp()
    end_timestamp = end.replace(tzinfo=from_zone).timestamp()
    
    return [begin_timestamp,end_timestamp]

def readInVisitorLists(t1,t2,month,day):
    begin_timestamp, end_timestamp = nightTimeStamp(t1,t2,month,day)
    siteVisitorsFile = '{}_visitorList.csv'.format(day)
    veraSetFileDir = r'C:\Users\abhin\Documents\Research\Covid-19-Research\COVID19_Free_Meal_Sites_PUBLICVIEW-shp\multiscale_epidemic-master\data\Veraset\Feb{}'.format(day)
    siteVisitors = pd.read_csv(siteVisitorsFile,index_col = None, header = 0)
    print(siteVisitors)
    return

readInVisitorLists(20,6,2,25)

# open json file
with open('{}_visitorDict.json'.format(day), 'w') as json_file:
    visitorDict = json.load(json_file)
print(visitorDict)



#    with open(siteVisitorsFile, newline='') as csvfile:
#        spamreader = csv.reader(csvfile, delimiter=' ', quotechar='|')
#        i = 0
#        for row in spamreader:
#            visitors.append(row)
#    
#    for i in range(len(visitors)):
#        if i % 4 == 2:
#            if visitors[i] != []:
#                j_prev = 0
#                for j in range(len(visitors[i][0])):
#                    if visitors[i][0][j] == ',':
#                        cleanedVisitors.append(visitors[i][0][j_prev:j])
#                        j_prev = j+1
#                    elif j == len(visitors[i][0])-1:
#                        cleanedVisitors.append(visitors[i][0][j_prev:-1])
#    
#uniqueVisitors = list(set([x for y in visitors for x in y]))
#uniqueVisitors = list(set(cleanedVisitors))
#uniqueVisitorsList = []
#for i in range(len(uniqueVisitors)):
#    uniqueVisitorsList.append([])
#path = 
#all_files = glob.glob(path+"/*.csv.gz")
#   # list containing visitors whose homes we want to find.
#for filename in all_files: # reads in each file and finds visitors and site visits
#    df = pd.read_csv (filename,index_col = None, header = 0)
#    loc_list = df.iloc[:,[0,5,6]]
#    fileList = loc_list.values.tolist()
#    for i in range(len(fileList)):
#        if fileList[i][0] in uniqueVisitors:
#            ind = uniqueVisitors.index(fileList[i][0])
#            uniqueVisitorsList[ind].append(fileList[i])
    