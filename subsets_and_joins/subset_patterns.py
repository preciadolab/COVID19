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

def main():
    core_path = "../../core_places/"
    pattern_path = "../../weekly_patterns/"
    global_path = "main-file/"
    phl_path = "main-file-phl/" #to be populated with subset
    #obtain indexes for places in Philadelphia
    phila_places = pd.read_csv(core_path+'phila_places.csv', index_col='safegraph_place_id')
    phila_index =  phila_places.index
    min_visitors= 10
    print('Subsetting for >= {} visits'.format( min_visitors ))
    #loop through parts, subset and append
    file_list = sorted(os.listdir(pattern_path + global_path))
    for file_name in file_list:
        print('--Subsetting file {}'.format(file_name))
        data_iterator = pd.read_csv(pattern_path + global_path + file_name,
                                    chunksize = 250000, index_col='safegraph_place_id')
        chunk_list = []  

        for i, chunk in enumerate(data_iterator):
            print('filtering chunk {}'.format(i))
            chunk = chunk.loc[ chunk.index.intersection(phila_index)]
            chunk = chunk.loc[chunk['raw_visit_counts'] >= min_visitors]    
            chunk_list.append(chunk)
        data = pd.concat(chunk_list)

        print('Total places in subset: {}'.format(len(data)))
        data.to_csv(pattern_path + phl_path + file_name, index=True)

if __name__ == '__main__':
    main()
    