"""
Function to subset the patterns dataset to a given county
"""
import numpy as np
import numpy.random as npr
import getopt, sys
import os
import pandas as pd
import gzip as gz
import time
import io
import re
import pdb
sys.path.insert(0, '../auxiliary_functions/')
import spatial_functions as sss

def subset_core_places(core_path, county, backfill=False):

    if os.path.isfile(core_path+'places-'+ county + '.csv') and not backfill:
        return("File {} exists already".format(core_path+'places-'+ county + '.csv'))
    #subset using the mapping from id to cbg
    places_cbg = pd.read_csv(core_path+'placeCountyCBG.csv',
                             dtype={'CBGFIPS':str},
                             index_col='safegraph_place_id') #UPDATED FILE? New places are added weekly
    places_cbg.drop(['state', 'stateFIPS', 'countyFIPS', 'countyName'], axis= 1, inplace=True)
    places_cbg.columns = ['cbg']

    places_cbg = places_cbg.loc[ [isinstance(x, str) for x in places_cbg['cbg']]]
    places_county = places_cbg.loc[ [x[:5]==county for x in places_cbg['cbg']]]
    j=0

    #loop through parts, subset and append
    patterns_county = pd.DataFrame()
    file_list = [name for name in sorted(os.listdir(core_path)) if re.search(r'core_poi-', name) is not None] #FIX
    for file_name in file_list:
        print(file_name)
        data = pd.read_csv(core_path+file_name, index_col='safegraph_place_id')
        print('--Loaded file {}'.format(file_name))
        j = j+len(data)
        #subset to county
        data = data.loc[data.index.intersection(places_county.index)]
        patterns_county = patterns_county.append(data, ignore_index=False)
    #Print the number of observations
    print('Total places in core_places: {}'.format(j))
    print('Finished subsetting to: {}, and found {} places'.format(county, len(patterns_county)))

    patterns_county.to_csv(core_path+'places-'+ county + '.csv', index=True)
    return(0)    