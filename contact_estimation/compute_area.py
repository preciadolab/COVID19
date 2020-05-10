"""
Preparation of time series of compliance metrics
"""
import numpy as np
import numpy.random as npr
import getopt, sys
import os
import pandas as pd
import gzip as gz
import subprocess
import pdb
import time
import os
import sys
import pyproj    
import shapely
import shapely.ops as ops
from shapely.geometry.polygon import Polygon
from functools import partial


sys.path.insert(0, './descriptive_analysis/')
import descriptive_patterns as aux


def obtain_area(poly_str):

	geom = {'type': 'Polygon',
	        'coordinates': parse_polygon(poly_str)}

	s = shape(geom)
	proj = partial(pyproj.transform, pyproj.Proj(init='epsg:4326'),
	               pyproj.Proj(init='epsg:3857'))

	s_new = transform(proj, s)

	return(s_new.area)



def main():
    #Load data about geometry of places
    core_path = "../../geometry/PhiladelphiaCamdenWilmingtonPANJDEMDMSA-CORE_POI-GEOMETRY-2020_03-2020-04-27"
    pd.read_csv(core_path+'core_poi-geometry.csv', index_col='safegraph_place_id')
    return(0)

if __name__ == '__main__':
    main()