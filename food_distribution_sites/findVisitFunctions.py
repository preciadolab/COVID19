# -*- coding: utf-8 -*-
"""
Created on Fri May  8 19:27:47 2020

@author: Abhinav Ramkumar, Victoria Fethke, Yi-An Hsieh
"""

"""
Food Distribution Site Visit Identification Functions:
    
This code provides functions in the shape files corresponding to food distribution sites,
and checks if a given user visited any of the sites during the specified windows
that it is open. If there is a visit, this code reports the user, the site,
and the time of the visit. 
"""

import shapefile
import geohash as gh
import re
import pandas as pd
import proximityhash

from shapely.ops import transform
from shapely.geometry import shape
from functools import partial
import pyproj  


def convertToEasternTime(userList):
    """
    Convert Time from 1970 time to EST
    """
    timeList = pd.to_datetime(userList.utc_timestamp,unit='s')
    timeList = timeList.dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
    return timeList


def checkVisitTimes(t1,t2):
    """
    OVERALL FUNCTIONALITY:
        1: Finds the day of the week (MTWRFS) for the cellphone ping and denotes them as indices 0-6, 0 being Mon.
        2: Extracts the hour and minute of the visit, in integer format. Denote times as doubles. Eg, 12:15 --> 12.25, etc.
        3: Takes the food distribution site hours, matches it with the day of visit, and extracts the integer from the specified string.
        4: Based on this information, the algorithm assumes that any visit from 6 pm up to 7 am, it assumes that the user just happened to
            walk around the site, but didn't visit the site. The basis for this assumption is that among the sites, there was no site that
            opened until 9 am, and none that stayed open after 5 pm. So it is safe to make this assumption for this data set.
        5: If it wasn't in that time window, then we checked if the time was greater than 12, but less than 6 pm, and subtracted 12
            since it was in 24 hour time.
        6: Once that was done, we checked if the value of time (eg. 10.25) was between the opening and closing times of the given site.
    INPUTS:
        1. day/time of the cellphone ping
        2. the time of visits
        
    OUTPUTS:
        boolean with True (visit occurred) or False (if visit didn't occur).
    """
    temp = re.findall(r'\d+', t2)
    t2_upd = list(map(int, temp))
    if len(t2_upd) == 0: # open always
        t2_upd.insert(0,7)
        t2_upd.insert(1,0)
        t2_upd.insert(2,4)
        t2_upd.insert(3,0)
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
    if t1_upd >= openHours[0] and t1_upd <= openHours[1]:
        #print("There was a visit")
        return True
    else:
        return False

def shp_into_dict(filename):
    """
    This function reads in the shape files with the opening times and locations of sites, 
    and converts coordinates into geohashes.
    """
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

def geohashIntersections(userLocationTimes,siteLocationTimes,prec):
    """
    OVERALL FUNCTIONALITY:
        Finds Geohash intersections between users and places. Sets geohash as index.
    INPUTS: 
        user list, site list, precision
    OUTPUTS:
        the initial lists, and the specific intersections
    """
    gh_places = pd.Index([x[:prec] for x in siteLocationTimes.geo_hash])
    gh_users = pd.Index([x[:prec] for x in userLocationTimes.geo_hash])
    userLocationTimes.geo_hash = gh_users
    siteLocationTimes.geo_hash = gh_places
    userLocationTimes.set_index(['geo_hash'],inplace = True)
    geoHashIntersections = gh_users.intersection(gh_places)
    return userLocationTimes, geoHashIntersections

def checkVisits(userLocationTimes,siteLocationTimes,visitorDict,siteVisits,totalVisits,prec):
    """
    OVERALL FUNCTIONALITY:
        1. check if list of user pings' geohashes are equal to ANY site's geohash, returns indices of those user pings.
        2. Find unique visitors in the intersections of userlist and sites.
        3. search user using loc, and compare all those geohashes. If any equal, count occurences, and find final index time minus initial time.
        4. based on criteria for time spent, return dictionary of users who visited a given site.
    
    INPUTS: 
        1. user lists (data files for a given day)
        2. site data (information about sites of interest)
        3. visitor dict (an initial visitor dictionary, in terms of individual geohashes)
        4. site visits (an initial number of visits to a whole site)
        5. total visits
        6. precision
    OUTPUTS:
        1. visitor dict -> updated
        2. site visits -> updated
        3. total vistis -> updated
    """
    userLocationTimes, geoHashIntersections = geohashIntersections(userLocationTimes,siteLocationTimes,prec)
    intersectionEntries = userLocationTimes.loc[geoHashIntersections]
    uniqueIDs = intersectionEntries.loc[geoHashIntersections].caid.unique()
    for i in range(len(uniqueIDs)):
        firstLast = intersectionEntries[intersectionEntries.caid == uniqueIDs[i]].iloc[[0,-1],:]
        timeStamps = firstLast.utc_timestamp
        t1 = firstLast.iloc[0].converted_times
        t2 = siteLocationTimes[siteLocationTimes.geo_hash == firstLast.index.tolist()[0]].times[0][t1.dayofweek]
        visitOccured = checkVisitTimes(t1,t2)
        if visitOccured:
            elapsedTime = (timeStamps.iloc[1] - timeStamps.iloc[0])/60
            if elapsedTime > 0.5:
                visitorDict[firstLast.index.tolist()[0]]['visitors'].append(uniqueIDs[i])
                visitorDict[firstLast.index.tolist()[0]]['visits']+=1
                siteVisits[visitorDict[firstLast.index.tolist()[0]]['name']]['visits']+=1
                totalVisits+=1
    return visitorDict,siteVisits, totalVisits


def point_to_geohash(filename,prec):
    """
    OVERALL FUNCTIONALITY:
        This function reads in the shape files with the opening times and locations of sites, 
        and converts coordinates into a SINGLE geohash of specified precision.
    
    INPUTS: 
        filename of site locations, and precision of geohash
    OUTPUTS: 
        returns dictionary with site location times, with names, geohashes, and opening times.
    """
    sf = shapefile.Reader(filename)
    geohashes = [] # list of site geohashes
    times = [] # list of times
    names = [] # list of names of sites
    
    with sf as shp:
        shape = shp.shapes()
        shpRecords = shp.shapeRecords()
        for i in range(len(shape)):
            geohashes.append(gh.encode(shape[i].points[0][1],shape[i].points[0][0],precision = prec))
            times.append(shpRecords[i].record[6:13])
            names.append(shpRecords[i].record[1])
            
    siteLocationTimes = pd.DataFrame(index = names)
    siteLocationTimes['geo_hash'] = geohashes
    siteLocationTimes['times'] = times
    return siteLocationTimes

def point_to_circle_geohash(filename,rad,prec):
    """
    OVERALL FUNCTIONALITY:
        This function reads in the shape files with the opening times and locations of sites, 
        and converts coordinates into a circle polygon with geohashes of specified precision.
    
    INPUTS: 
        filename of site locations, radius around point, and precision of geohash
    OUTPUTS: 
        returns dictionary with site location times, with names, geohashes, and opening times.
    """ 
    sf = shapefile.Reader(filename)
    geohashes = [] # list of site geohashes
    times = [] # list of times
    names = [] # list of names of sites
    
    with sf as shp:
        shape = shp.shapes()
        shpRecords = shp.shapeRecords()
        for i in range(len(shape)):
            geohash_circle =  proximityhash.create_geohash(shape[i].points[0][1],shape[i].points[0][0],rad,precision=prec)
            geohash_circle = geohash_circle.split(",")
            geohashes.append(geohash_circle)
            for j in range(len(geohash_circle)):
                times.append(shpRecords[i].record[6:13])
                names.append(shpRecords[i].record[1])
    
    geohashes = [item for sublist in geohashes for item in sublist]
    siteLocationTimes = pd.DataFrame(index = names)
    siteLocationTimes['geo_hash'] = geohashes
    siteLocationTimes['times'] = times
    return siteLocationTimes