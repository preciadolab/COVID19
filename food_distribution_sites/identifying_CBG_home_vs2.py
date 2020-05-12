# -*- coding: utf-8 -*-
"""
Created on Sun May 10 15:15:16 2020

@author: Abhinav Ramkumar
"""

"""
Identifying CBG Homes for Cellphone Users:

Takes in a data file with cellphone pings, and finds the mode geohash to 6 digits for
a given user.
"""

import pandas as pd
import numpy as np
from datetime import datetime
from dateutil import tz
import glob

"""
Read in the csv files and stores the geohash and time of the visit
"""
li = []
for day in range(28,29):
    path = r'C:\Users\abhin\Documents\Research\Covid-19-Research\COVID19_Free_Meal_Sites_PUBLICVIEW-shp\multiscale_epidemic-master\data\Veraset\Feb{}'.format(day)
    all_files = glob.glob(path+"/*.csv.gz")
    for filename in all_files:
        df = pd.read_csv (filename,index_col = None, header = 0)
        li.append(df)

li = pd.concat(li,axis =0, ignore_index = True)

"""
This part takes in the latitude and longitude and finds the geohashes.
"""
loc_time = li.iloc[:,[0,5,6]];
loc_time_list = loc_time.values.tolist()

"""
Find all the unique users in the list:
"""

def findUsers(loc_time_list):
    userIDs = []
    for i in range(len(loc_time_list)):
        userIDs.append(loc_time_list[i][0])
    return list(set(userIDs))

"""
Make lists for individual users:
"""

def makeUserLists(loc_time_list):
    users = findUsers(loc_time_list)
    usersList = []
    for i in range(len(users)):
        userList = []
        for j in range(len(loc_time_list)):
            if loc_time_list[j][0] == users[i]:
                userList.append(loc_time_list[j])
        usersList.append(userList)
    return usersList
                
usersList = makeUserLists(loc_time_list)

"""
This part takes in the time from 1970 and converts it to a day-time object.
"""
def convertTime(userList):
    # find timezone codes
    from_zone = tz.gettz('UTC')
    to_zone = tz.gettz('America/New_York')
    timeList = []
    for i in range(len(userList)):
        utc = datetime.fromtimestamp(userList[i][1])
        utc = utc.replace(tzinfo=from_zone)
        eastern = utc.astimezone(to_zone)
        timeList.append(eastern)
    return timeList

"""
Find all unique geohashes for a given user:
"""
def findGeoHashes(userList,precision):
    geohashList = []
    for i in range(len(userList)):
        geohashList.append(userList[i][2][:precision])
    return list(set(geohashList))

"""
Update time spent at a geohash:
"""
def findTimeAtGeoHash(gh1,gh2,ghList,tD,tList):
    if gh1 == gh2:
         ind = ghList.index(gh1)
         tList[ind] = tList[ind] + tD;
    return tList

"""
Find home for each user:
"""
def findUserHome(userList,precision):
    #print(len(userList))
    timeList = convertTime(userList)
    geohashList = findGeoHashes(userList,precision)
    #print(geohashList)
    timeSpentAtGeoHash = np.zeros(len(geohashList))
    userHome = []
    userLocation = []
    for i in range(len(userList)-1):
        day = timeList[i].day
        dayNext = timeList[i+1].day
        hour = timeList[i].hour
        hourNext = timeList[i+1].hour
        minute = timeList[i].minute
        minuteNext = timeList[i+1].minute
        geohash = userList[i][2][:precision]
        geohashNext = userList[i+1][2][:precision]
        hour_min = hour + minute/60
        hour_minNext = hourNext + minuteNext/60
        if (hour_min > 19 or hour_min < 6) and (hour_minNext > 19 or hour_minNext < 6) and dayNext-day <= 1:
            timeDelta = userList[i+1][1] - userList[i][1]
            timeSpentAtGeoHash = findTimeAtGeoHash(geohash,geohashNext,geohashList,timeDelta,timeSpentAtGeoHash)
        elif (hour_min > 19 or hour_min < 6) and hour_minNext > 6:
            #print(day,hour_min,dayNext,hour_minNext)
            if timeSpentAtGeoHash == []:
                userLocation.append('None')
                userHome.append('None')
            elif not timeSpentAtGeoHash.any():
                userLocation.append('None')
                userHome.append('None')
            else:
                #print(timeSpentAtGeoHash)
                x = int(np.where(timeSpentAtGeoHash == np.amax(timeSpentAtGeoHash))[0])
                #print(x)
                userLocation.append(timeSpentAtGeoHash)
                userHome.append(geohashList[x])
                timeSpentAtGeoHash = []
    return userHome, userLocation

userLocations = []
userHomes = []
for i in range(len(usersList)):
    if len(usersList[i]) <= 5:
        userHomes.append('None')
        userLocations.append('None')
    else:
        #print(i)
        userHome, userLocation = findUserHome(usersList[i],6)
        userHomes.append(userHome)
        userLocations.append(userLocation)
        userHome = []
        userLocation = []
        print('User {} home is {}'.format(i,userHomes[i]))
