"""
Function to subset the patterns dataset to Philadelphia
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

def subset_patterns(core_path, pattern_path, county, backfill=False):
    global_path = "main-file/"
    county_path = 'main-file-'+ county +'/' #to be populated with subset

    
    if os.path.isdir(pattern_path+county_path) and not backfill:
        return("Directory {} exists already".format(pattern_path+county_path))
    #obtain indexes for places in county
    county_places = pd.read_csv(core_path+'places-'+ county +'.csv', index_col='safegraph_place_id')
    county_index =  county_places.index
    print('Subsetting for county {}'.format( county ))
    #loop through parts, subset and append
    file_list = sorted(os.listdir(pattern_path + global_path))
    os.makedirs(pattern_path+county_path, exist_ok=True)
    for file_name in file_list:
        print('--Subsetting file {}'.format(file_name))
        data_iterator = pd.read_csv(pattern_path + global_path + file_name, chunksize = 250000, index_col='safegraph_place_id')
        chunk_list = []  

        for i, chunk in enumerate(data_iterator):
            print('filtering chunk {}'.format(i))
            chunk = chunk.loc[ chunk.index.intersection(county_index) ]    
            chunk_list.append(chunk)
        data = pd.concat(chunk_list)

        print('Total places in patterns file: {}'.format(len(data)))
        data.to_csv(pattern_path + county_path + file_name, index=True)
    return(0)

    