# -*- coding: utf-8 -*-
"""
Created on Tue May  5 15:33:25 2020

@author: Abhinav Ramkumar
"""

"""
This file reads in shape files and can be used to identify locations of Food Distribution Sites:
Put this file in the same directory as the files which you are reading in.
"""
import shapefile
# import gzip
import geohash as gh
import pandas as pd
from datetime import datetime

#with gzip.open("part-00000-tid-3624586920856034780-8555a0db-de4b-4ebb-a116-5f1270b9b3f4-216277-c000 (1).csv.gz", 'rb') as f:
#    file_content = f.read()
#    print(file_content)
"""
Read in the csv files and stores the geohash and time of the visit
"""
df = pd.read_csv ("part-00000-tid-3624586920856034780-8555a0db-de4b-4ebb-a116-5f1270b9b3f4-216277-c000 (1).csv.gz")
# print(df.iloc[0])
# print(df.iloc[:,[5,6]])

"""
This part takes in the latitude and longitude and finds the geohashes.
"""
loc_time = df.iloc[:,[5,6]];
loc_time_list = loc_time.values.tolist()

"""
This part takes in the time from 1970 and converts it to a day-time object.
"""
day_time = []
for i in range(7687):
    day_time.append(datetime.fromtimestamp(loc_time_list[i][0]))

"""
This part reads in the shape files with the individual food distribution sites, and converts
coordinates into geohashes.
"""
sf = shapefile.Reader("COVID19_FreeMealSites")
ghdist = [] # list of geohashes of distribution sites
times = [] # list of times

with sf as shp:
    #print(shp)
    #print(shp.fields)
    shape = shp.shapes()
    shprec = shp.shapeRecords()
    for i in range(149):
        #print(shprec[i].record[6:13])
        ghdist.append(gh.encode(shape[i].points[0][1],shape[i].points[0][0],precision = 9))
        times.append(shprec[i].record[6:13])