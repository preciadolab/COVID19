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
"""
import sys
import shapefile
import json
import re
import geohash as gh #which geohash?
import pandas as pd
import glob
import matplotlib.pyplot as plt
import numpy as np
import pdb
import os

from shapely.ops import transform
from shapely.geometry import shape
from functools import partial
import pyproj    

sys.path.insert(0, '..')
from auxiliary_functions.spatial_functions import polygon_area
"""
Useful Functions for Script:
"""

"""
Convert Time from 1970 time to EST
"""
def convertToEasternTime(userList):
    timeList = pd.to_datetime(userList.utc_timestamp, unit='s')
    timeList = timeList.dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
    return timeList

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
def checkVisitTimes(t1,t2):
    """
    Transform input times into hours and checks if it is open during the given day that the ping
    occurs.
    """
    temp = re.findall(r'\d+', t2)
    t2_upd = list(map(int, temp))
    if len(t2_upd) == 0: # open always
        return True
    elif len(t2_upd) == 2: # open at whole hours
        t2_upd.insert(1,0)
        t2_upd.insert(3,0)
    elif len(t2_upd) == 3: # open at one partial time
        if t2_upd[1] > 12:
            t2_upd.insert(3,0)
        else:
            t2_upd.insert(1,0)
    openHours = [t2_upd[0]+t2_upd[1]/60,t2_upd[2]+t2_upd[3]/60]
    t1_upd = t1.hour + t1.minute/60 + t1.second/3600
    if t1_upd > 12:
        t1_upd = t1_upd - 12
    """
    This section checks if a visit indeed occurred during the day/time of the ping.
    """
    if t1_upd >= openHours[0] and t1_upd <= openHours[1]:
        #print("There was a visit")
        return True
    else:
        return False

"""
Finds Geohash intersections between users and places. Sets geohash as index.
"""

def geohashIntersections(userLocationTimes,siteLocationTimes,prec):
    gh_places = pd.Index([x[:prec] for x in siteLocationTimes.geo_hash])
    gh_users = pd.Index([x[:prec] for x in userLocationTimes.geo_hash])
    userLocationTimes.geo_hash = gh_users
    siteLocationTimes.geo_hash = gh_places
    userLocationTimes.set_index(['geo_hash'],inplace = True)
    geoHashIntersections = gh_users.intersection(gh_places)
    return userLocationTimes, geoHashIntersections

'''
1. check if list of user pings' geohashes are equal to ANY site's geohash, returns indices of those user pings.
2. Loop through those user's pings, check to see which geohash they are equal to
3. search user using loc, and compare all those geohashes. If any equal, count occurences, and find final index time minus initial time.
4. based on criteria (eg. 3 occurences, or 1 minute at site), return dictionary of users who visited a given site.
5. report total number of visits, and also the total time of the visits (so total time divided by number of users, so later we can determine average time spent at a site).
'''

def checkVisits(userLocationTimes,siteLocationTimes,visitorDict,totalVisits,prec):
    userLocationTimes, geoHashIntersections = geohashIntersections(userLocationTimes,siteLocationTimes,prec)

    intersectionEntries = userLocationTimes.loc[geoHashIntersections]
    uniqueIDs = intersectionEntries.loc[geoHashIntersections].caid.unique()
    #print(uniqueIDs[0])
    for i in range(len(uniqueIDs)):
        firstLast = intersectionEntries[intersectionEntries.caid == uniqueIDs[i]].iloc[[0,-1],:]
        timeStamps = firstLast.utc_timestamp
        t1 = firstLast.iloc[0].converted_times
        t2 = siteLocationTimes[siteLocationTimes.geo_hash == firstLast.index.tolist()[0]].times[0][t1.dayofweek]
        visitOccured = checkVisitTimes(t1,t2)
        if visitOccured:
            elapsedTime = timeStamps.iloc[1] - timeStamps.iloc[0] #seconds
            if elapsedTime > 20:
                visitorDict[firstLast.index.tolist()[0]]['visitors'].append(uniqueIDs[i])
                visitorDict[firstLast.index.tolist()[0]]['visits']+=1
                totalVisits+=1
    return visitorDict,totalVisits


"""
This function reads in the shape files with the opening times and locations of sites, 
and converts coordinates into geohashes.
"""
def shp_into_dict(filename):
    with shapefile.Reader(filename) as sf:
        # name of fields
        fields = sf.fields[1:] 
        field_names = [field[0] for field in fields]
        shp_dict = {}
        #To do: read from proj file
        proj = partial(pyproj.transform, pyproj.Proj(init='epsg:3857'),
           pyproj.Proj(init='epsg:4326')) #to project back into lat, lon
        for r in sf.shapeRecords():
             atr = dict(zip(field_names, r.record))
             geom = r.shape.__geo_interface__ #Is in EPSG:4326
             geom = transform(proj, shape(geom)).__geo_interface__
             #No ID, so we use geohash9 of centroid
             geoid = gh.encode(
                shape(geom).centroid.coords[0][1],
                shape(geom).centroid.coords[0][0],
                precision = 9)
             shp_dict[geoid] = {'features':atr, 'geometry':geom}
      
    return(shp_dict)

def points_to_gh7(filename):
    """
    Maps point geometries into the geohash7 tile that contains them. 
    Key values are GEOID and fields include the geohash, as well as optional
    parameters like name and times of operation
    Params:
      shp_dict: dict
                dictionary with shapefiles in geo_interface format
    """
    sf = shapefile.Reader(filename)
    geohashes = [] # list of site geohashes
    times = [] # list of times
    names = [] # list of names of sites
    
    with sf as shp:
        shape = shp.shapes()
        shpRecords = shp.shapeRecords()
        #Take point, draw a radius ~
        for i in range(len(shape)):
            #also add lat and lon to dict
            #geohashes.append(gh.encode(shape[i].points[0][1],shape[i].points[0][0],precision = 7))
            times.append(shpRecords[i].record[6:13])
            names.append(shpRecords[i].record[1])
    pdb.set_trace()
    siteLocationTimes = pd.DataFrame(index = names)
    siteLocationTimes['geo_hash'] = geohashes
    siteLocationTimes['times'] = times
    return siteLocationTimes

"""
This function reads in the shape files with the opening times and locations of sites, 
and converts coordinates into a circle polygon with geohash 8.
"""

def point_to_circle_geohash_8(filename,rad,prec):
    sf = shapefile.Reader(filename)
    geohashes = [] # list of site geohashes
    times = [] # list of times
    names = [] # list of names of sites
    
    with sf as shp:
        shape = shp.shapes()
        shpRecords = shp.shapeRecords()
        for i in range(len(shape)):
            geohashes.append(polygons_to_geohash(point_to_circle(shape[i].points[0][1],shape[i].points[0][0],rad),precision=prec))
            times.append(shpRecords[i].record[6:13])
            names.append(shpRecords[i].record[1])
            
    siteLocationTimes = pd.DataFrame(index = names)
    siteLocationTimes['geo_hash'] = geohashes
    siteLocationTimes['times'] = times
    return siteLocationTimes

def findVisits(day, month, path_veraset, path_output, path_meals, k = None):
    """
    Read in the csv files and stores the geohash and time of the visit, sequentially
    """
    #Optional specification of month and day to use in parallel
    if k is not None:
        if k in list(range(1,31)):
            month = '04'
            day = str(k).zfill(2)
        elif k in list(range(31,62)):
            month = '05'
            day = str(k - 30).zfill(2)
    print("Finding visits for {}-{} (mm-dd)".format(month, day))
    ############################################################

    all_files = os.listdir(path_veraset + month+'/'+day+'/')
    all_files = [name for name in all_files if re.search(r'part', name) is not None]
    
    '''
    Initialization Block
    '''
    shp_dict_all = shp_into_dict(path_meals + 'OtherMealSites_All')
    shp_dict_youths = shp_into_dict(path_meals + 'YouthMealSites_All')

    pdb.set_trace()
    siteLocationTimes = points_to_gh7(path_meals + 'OtherMealSites_All')
    visitorDict = { geohash:{'name':name,'visits':0,'visitors':[]} for name, geohash in zip(siteLocationTimes.index.tolist(),siteLocationTimes.geo_hash.tolist())} 
    print('Finding visits for {} locations'.format(len(visitorDict)))
    totalVisits = 0 # initialize total number of visits per day.
    
    for filename in all_files: # reads in each file and finds visitors and site visits
        userLocationTimes = pd.read_csv(path_veraset+month+'/'+day+'/'+filename , index_col = None)
        if np.sum([not isinstance(x,str) for x in userLocationTimes.geo_hash]) >0:
            sys.exit('--Error: {} contains non-strings in the geohash field'.format(filename))

        userLocationTimes['converted_times'] = convertToEasternTime(userLocationTimes)  # This part takes in the time from 1970 and converts it to a day-time object.        
        visitorDict , totalVisits = checkVisits(userLocationTimes,siteLocationTimes,visitorDict,totalVisits,7)

    os.makedirs(path_output, exist_ok=True)
    with open(path_output + 'food_visits_{}-{}.json'.format(month,day), 'w+') as fp:
        json.dump(visitorDict, fp)
    print('--Finished finding visits for {}-{}, found {} visits'.format(month, day, totalVisits))

# if __name__ == '__main__':
#     daylist= list(range(18,26))+list(range(48, 56))
#     for k in daylist:
#         findVisits(day='04',
#                    month='05',
#                    path_veraset='../../veraset-42101/',
#                    path_meals='../../meal_sites/',
#                    path_output='../../stats/findVisitsResults/',
#                    k = k) 

if __name__ == '__main__':
    findVisits(day='04',
               month='05',
               path_veraset='../../veraset-42101/',
               path_meals='../../food_sites/',
               path_output='../../stats/findVisitsResults/',
               k = None) 

