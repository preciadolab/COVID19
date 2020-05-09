# -*- coding: utf-8 -*-
"""
Created on Fri May  8 19:27:47 2020

@author: Abhinav Ramkumar, Victoria Fethke, Yi-An Hsieh
"""

"""
Food Distribution Site Visit Identification:
    
This code takes in the shape files corresponding to food distribution sites,
and checks if a given user visited any of the sites during the specified windows
that it is open. If there is a visit, this code reports the user, the site,
and the time of the visit. 

*** 
Next steps:
The next step to this code is to add a function that determines the home of the user,
wh
"""
import shapefile
import re
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
loc_time = df.iloc[:,[0,5,6]];
loc_time_list = loc_time.values.tolist()

"""
This part takes in the time from 1970 and converts it to a day-time object.
"""
day_time = []
for i in range(len(loc_time_list)):
    day_time.append(datetime.fromtimestamp(loc_time_list[i][1]))

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
    for i in range(len(shape)):
        #print(shprec[i].record[6:13])
        ghdist.append(gh.encode(shape[i].points[0][1],shape[i].points[0][0],precision = 9))
        date_time_str = times
        times.append(shprec[i].record[6:13])

"""
This function takes in 2 geohashes, and checks if they are identical to a specified
precision value. This outputs a boolean saying whether they are equal or not.
"""
def checkGeoHash(gh1,gh2,precision):
    if gh1[:precision] == gh2[:precision]:
        ghEqual = True
    else:
        ghEqual = False
    return ghEqual   

"""
This function takes in the day/time of the cellphone ping, and the time of visits corresponding
to the geohash of the site that the user visited. The algorithm in short is as follows:
    1: Finds the day of the week (MTWRFS) for the cellphone ping and denotes them as indices 0-6, 0 being Mon.
    2: Extracts the hour and minute of the visit, in integer format. Denote times as doubles. Eg, 12:15 --> 12.25, etc.
    3: Takes the food distribution site hours, matches it with the day of visit, and extracts the integer from the specified string.
    4: Based on this information, the algorithm assumes that any visit from 6 pm up to 7 am, it assumes that the user just happened to
        walk around the site, but didn't visit the site. The basis for this assumption is that among the sites, there was no site that
        opened until 9 am, and none that stayed open after 5 pm. So it is safe to make this assumption for this data set.
    5: If it wasn't in that time window, then we checked if the time was greater than 12, but less than 6 pm, and subtracted 12
        since it was in 24 hour time.
    6: Once that was done, we checked if the value of time (eg. 10.25) was between the opening and closing times of the given site.
This function returns a boolean with True (visit occurred) or False (if visit didn't occur).
"""
def checkSiteVisit(day_time,times):
    #print(loc_time_list[i][2],ghdist[j])
    """
    This block takes in inputs and decomposes them into time in the format
    of numbers, eg. 12:15 --> 12.25, or 10:35 --> 10.583333. This also takes 
    in site hours and checks if it is open during the given day that the ping
    occurs.
    """
    dayOfWeek = day_time[i].weekday()
    Hour = day_time[i].hour
    Minute = day_time[i].minute
    Time_Site = times[j][dayOfWeek]
    temp = re.findall(r'\d+', Time_Site)
    timesiteNum = list(map(int, temp))
    if len(timesiteNum) == 0:
        print("No opening hours")
        return False
    elif len(timesiteNum) == 2:
        timesiteNum.insert(1,0)
        timesiteNum.insert(3,0)
    elif len(timesiteNum) == 3:
        if timesiteNum[1] > 12:
            timesiteNum.insert(3,0)
        else:
            timesiteNum.insert(1,0)
    hourFrac1 = timesiteNum[1]/60
    hourFrac2 = timesiteNum[3]/60
    openHours = [timesiteNum[0]+hourFrac1,timesiteNum[2]+hourFrac2]
    #print(dayOfWeek,Hour,Minute,i,j,ghdist[j],Time_Site,timesiteNum,openHours)
    """
    This section checks if a visit indeed occurred during the day/time of the ping.
    """
    if Hour > 17 or Hour < 7:
        print('Hour {} is during off hours'.format(Hour))
        return False
    elif Hour > 12:
        Hour_12 = Hour - 12
    else:
        Hour_12 = Hour
    Hour_12_min = Hour_12 + Minute/60
    #print(Hour_12_min)
    if Hour_12_min >= openHours[0] and Hour_12_min <= openHours[1]:
        print("There was a visit")
        return True
    else:
        return False

"""
Run user cellphone pings against distribution site Geohashes and see if first 6
digits match:
Creates a visitsList with the user, time, and day of visit.
Creates a histogram with the number of visits by site.
"""
visitsList = []
numberOfVisits = []
for i in range(len(ghdist)):
    numberOfVisits.append([ghdist[i],0])
for i in range(len(loc_time_list)):
    for j in range(len(ghdist)):
        ghEqual = checkGeoHash(loc_time_list[i][2],ghdist[j],6)
        if ghEqual == True:
            visitOccured = checkSiteVisit(day_time,times)
            if visitOccured == True:
               visitsList.append([loc_time_list])
               numberOfVisits[j][1] = numberOfVisits[j][1] + 1
               