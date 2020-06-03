"""
Preparation of time series of compliance metrics
"""
import getopt, sys
import os
import sys
import json
import pandas as pd
import pyproj    
import geojson
import pdb
import geohash as gh
import shapely
import shapely.ops as ops
from shapely.ops import transform, unary_union
from shapely.geometry.polygon import Polygon
from shapely.geometry import shape
from functools import partial

def point_to_circle(center,radius):
    #lon, lat = -122.431297, 37.773972 # lon lat for San Francisco
    #radius = 10000  # in m
    epsg3857_to_epsg4326 = partial(
        pyproj.transform, 
        pyproj.Proj(init='epsg:3857'),
        pyproj.Proj(init='epsg:4326')
    )
    epsg4326_to_epsg3857 = partial(
        pyproj.transform, 
        pyproj.Proj(init='epsg:4326'),
        pyproj.Proj(init='epsg:3857')
    )

    point_transformed = transform(epsg4326_to_epsg3857, center)
    buffer = point_transformed.buffer(radius)
    # Get the polygon with lat lon coordinates
    circle_poly = transform(epsg3857_to_epsg4326, buffer)
    return circle_poly

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



def polygons_to_geohash(polygon_list, precision = 5):
    if not type(polygon_list) == list:
        print ('input must be a list')
        sys.exit(2)
    #merge list of polygons super_polygon
    super_polygon = unary_union(polygon_list)
    #centroids are part of the polygon
    hashset = set([gh.encode(longitude = geom.centroid.coords[0][0],
                        latitude = geom.centroid.coords[0][1],
                        precision = precision) for geom in polygon_list])

    gh_to_check = [hash_neigh for hash_ in hashset for hash_neigh in gh.neighbors(hash_)]
    gh_to_check = set(gh_to_check) - hashset
    while len(gh_to_check)>0:
        hash_ = gh_to_check.pop()
        #check
        box = gh.bbox(hash_)
        box = {'type':'Polygon', 'coordinates':[[ [box['w'], box['n']],
                                                            [box['e'], box['n']],
                                                            [box['e'], box['s']],
                                                            [box['w'], box['s']] ]]}
        box = shape(box)
        if box.intersects(super_polygon):
            hashset.add(hash_)
            new_neighbors = [x for x in gh.neighbors(hash_) if x not in hashset and x not in gh_to_check]
            gh_to_check = gh_to_check.union(set(new_neighbors))
    return(hashset)
        #add neighbors
