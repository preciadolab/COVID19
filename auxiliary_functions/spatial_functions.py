"""
Preparation of time series of compliance metrics
"""
import getopt, sys
import os
import sys
import pyproj    
import shapely
import shapely.ops as ops
from shapely.ops import transform
from shapely.geometry.polygon import Polygon
from shapely.geometry import shape
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

