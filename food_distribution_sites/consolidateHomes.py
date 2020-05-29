<<<<<<< HEAD
# -*- coding: utf-8 -*-
"""
Created on Fri May 15 09:42:11 2020
@author: Jorge Barreras, Abhinav Ramkumar, Victoria Fethke, Yi-An Hsieh
"""
import json
import pandas as pd
import os
import re
import pdb
import getopt, sys
import numpy as np
import operator
import geojson
import geohash as gh
from shapely.geometry import point, polygon, multipolygon, shape

def home_to_cbg(home_geohash, POLYGON_dict):
    #transform to point
    p_coords = gh.decode(home_geohash) #string >>> tuple (lat, lon)
    p = point.Point(p_coords[1], p_coords[0])
    home = ''
    for home_cbg, poly in POLYGON_dict.items():
        if p.within(poly):
            home = home_cbg
    return home[:12]

   
def main(path_json, path_output):

    all_files = sorted(os.listdir(path_json))
    mergedDict = {}
    for filename in all_files:
        with open(path_json + filename) as fp:
            visitorDict = json.load(fp)

        #merge dictionaries by adding
        for visitor, homes in visitorDict.items():
            if visitor in mergedDict.keys():
                for home, time in homes.items():
                    if home in mergedDict[visitor].keys():
                        mergedDict[visitor][home] +=  time
                    else:
                        mergedDict[visitor][home] = time
            else:
                mergedDict[visitor] = homes

    print("finished consolidating homes")
    #organize in table
    home_dict = {visitor:max(homes.items(), key=operator.itemgetter(1))[0] for
                 visitor, homes in mergedDict.items()}

    df = pd.DataFrame.from_dict(home_dict, orient = 'index')
    df.reset_index(drop=False, inplace=True)
    df.columns = ['visitor', 'home_geohash']
    
    with open('../../core_places/censusBlockGroups.geojson') as fp:
        geojson = json.load(fp)
    cb_polygon_dict = {}
    for i in range(len(geojson['features'])):
        if geojson['features'][i]['properties']['GEOID'][:5] in ['42101','42045','42091']:
            cb_polygon_dict[geojson['features'][i]['properties']['GEOID']] =  geojson['features'][i]['geometry']

    cb_polygon_dict = { k:shape(v) for k,v in cb_polygon_dict.items()}

    df['home_cbg'] = [home_to_cbg(home_geohash, cb_POLYGON_dict) for home_geohash in df['home_geohash']]

    pdb.set_trace()
    df.to_csv(path_output + 'user_homes_upd.csv', index = False)

if __name__ == '__main__':
    main(path_json = '../../stats/findHomeResults/',
=======
# -*- coding: utf-8 -*-
"""
Created on Fri May 15 09:42:11 2020
@author: Jorge Barreras, Abhinav Ramkumar, Victoria Fethke, Yi-An Hsieh
"""
import json
import pandas as pd
import os
import re
import pdb
import getopt, sys
import numpy as np
import operator
import geojson
import geohash as gh
from shapely.geometry import point, polygon, multipolygon

def home_to_cbg(home_geohash, POLYGON_dict):
    #transform to point
    p_coords = gh.decode(home_geohash) #string >>> tuple (lat, lon)
    p = point.Point(p_coords[0], p_coords[1])
    home = ''
    for home_cbg, poly in POLYGON_dict.items():
        if p.within(poly):
            home = home_cbg
    return home[:12]

def polygon_from_geojson(list_coords): #lon lat
    if  np.sum([not len(x) == 2 for x in list_coords]) > 0: #multipolygon
        pol_lists = multipolygon.MultiPolygon([polygon.Polygon([(y,x) for sub_poly in list_coords for x,y in sub_poly])])
        return(pol_lists)

    return polygon.Polygon([(y,x) for x,y in list_coords])
    
def main(path_json, path_output):

    all_files = sorted(os.listdir(path_json))
    mergedDict = {}
    for filename in all_files:
        with open(path_json + filename) as fp:
            visitorDict = json.load(fp)

        #merge dictionaries by adding
        for visitor, homes in visitorDict.items():
            if visitor in mergedDict.keys():
                for home, time in homes.items():
                    if home in mergedDict[visitor].keys():
                        mergedDict[visitor][home] +=  time
                    else:
                        mergedDict[visitor][home] = time
            else:
                mergedDict[visitor] = homes

    print("finished consolidating homes")
    #organize in table
    home_dict = {visitor:max(homes.items(), key=operator.itemgetter(1))[0] for
                 visitor, homes in mergedDict.items()}

    df = pd.DataFrame.from_dict(home_dict, orient = 'index')
    df.reset_index(drop=False, inplace=True)
    df.columns = ['visitor', 'home_geohash']
    
    with open('../../core_places/censusBlockGroups.geojson') as fp:
        geojson = json.load(fp)
    #obtain polygons?
#    cb_polygon_dict = { x['properties']['GEOID']:x['geometry'] for x in geojson['features']}
#    cb_POLYGON_dict = { k:polygon_from_geojson(v['coordinates'][0]) for k,v in cb_polygon_dict.items()}
    cb_polygon_dict = {}
    for i in range(len(geojson['features'])):
        if geojson['features'][i]['properties']['GEOID'][:5] in ['42101','42045','42091']:
            cb_polygon_dict[geojson['features'][i]['properties']['GEOID']] =  geojson['features'][i]['geometry']
    cb_POLYGON_dict = { k:polygon_from_geojson(v['coordinates'][0]) for k,v in cb_polygon_dict.items()}

    df['home_cbg'] = [home_to_cbg(home_geohash, cb_POLYGON_dict) for home_geohash in df['home_geohash']]

    pdb.set_trace()
    df.to_csv(path_output + 'user_homes_upd.csv', index = False)
#    df.to_csv('user_homes.csv', index = False)

if __name__ == '__main__':
    main(path_json = '../../stats/findHomeResults/',
>>>>>>> ff09cb0defbe2d98765ab4ca5d26b967020335af
         path_output = '../../stats/')