"""
Function to subset the patterns dataset to Philadelphia (in place)
"""
import numpy as np
import numpy.random as npr
import getopt, sys
import os
import pandas as pd
import gzip as gz
import subprocess
import time
import io
import pdb
import pyproj
from shapely.geometry import shape
from shapely.ops import transform
from functools import partial

def polygon_area(wkt): #square meters
    #parse polygon
    wkt = wkt[10:-2]
    coords = [[float(y) for y in str_.split(' ')] for str_ in wkt.split(', ')]

    geom = {'type': 'Polygon', 'coordinates': [ coords ]}

    s = shape(geom)
    proj = partial(pyproj.transform, pyproj.Proj(init='epsg:4326'),
               pyproj.Proj(init='epsg:3857'))
    s_new = transform(proj, s)
    return(s_new.area)

def main():
    core_path = "../../core_places/"
    geometry_path = "../../geometry/"
    county = "42101"

    #subset using the mapping from id to cbg
    places_cbg = pd.read_csv(core_path+'placeCountyCBG.csv', dtype={'CBGFIPS':str}, index_col='safegraph_place_id') #UPDATED FILE? New places are added weekly
    places_cbg.drop(['state', 'stateFIPS', 'countyFIPS', 'countyName'], axis= 1, inplace=True)
    places_cbg.columns = ['cbg']

    places_cbg = places_cbg.loc[ [isinstance(x, str) for x in places_cbg['cbg']]]
    places_cbg = places_cbg.loc[ [x[:5]==county for x in places_cbg['cbg']]]
    j=0

    #Obtain the area of places in Philadelphia (To Do)
    geometry = pd.read_csv(geometry_path+'19107.csv', index_col='safegraph_place_id')
    eg_wkt = geometry.loc['sg:22f615de0da2476881ee260a3fecb8fe']['polygon_wkt']
    aa = polygon_area(eg_wkt)
    print('Broad Street Ministry Area: {} m2'.format(aa))

    #loop through parts, subset and append
    phila_places = pd.DataFrame()
    file_list = [name for name in sorted(os.listdir(core_path)) if name[-3:]=='.gz']
    for file_name in file_list:
        print(file_name)
        data = pd.read_csv(core_path+file_name, index_col='safegraph_place_id')
        print('--Loaded file {}'.format(file_name))
        j = j+len(data)
        #subset to philadelphia
        data = data.loc[data.index.intersection(places_cbg.index)]
        phila_places = phila_places.append(data, ignore_index=False)
    #Print the number of observations
    print('Total places in core_places: {}'.format(j))
    print('Finished subsetting to: {}, and found {} places'.format(county, len(phila_places)))


    phila_places.to_csv(core_path+'phila_places.csv', index=True)
if __name__ == '__main__':
    main()
    