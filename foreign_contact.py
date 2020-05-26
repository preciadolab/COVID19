import numpy as np
import numpy.random as npr
import getopt, sys
import os
import pandas as pd
import re
import pdb
sys.path.insert(0, 'auxiliary_functions/')
sys.path.insert(0, './')
import json_code as jjj
import contact_networks as ccc

def main(core_path, patterns_path, backfill = False):
    global_path = 'main-file/'
    foreign_path = 'main-file-foreign/'
    os.makedirs(patterns_path + foreign_path, exist_ok = True)
    county = '42101'
    #index of places in Philadelphia (exclude)
    county_places = pd.read_csv(core_path+'places-'+ county +'.csv', index_col='safegraph_place_id')
    county_index = county_places.index
    county_places = None

    #lazy load patterns
    pattern_dates = [x[5:10] for x in sorted(os.listdir(patterns_path+'main-file/'))]
    for patterns_date in pattern_dates:
        break
        print("--loading patterns file {}".format(patterns_date))
        data_iterator = pd.read_csv(patterns_path+global_path+'2020-{}-weekly-patterns.csv.gz'.format(patterns_date),
                chunksize = 300000,
                index_col='safegraph_place_id')
        chunk_list = []  

        for i, chunk in enumerate(data_iterator):
            print('filtering chunk {}'.format(i))
            chunk = chunk.loc[ chunk.index.difference(county_index) , ['visitor_home_cbgs']]
            mask = chunk.visitor_home_cbgs.apply(lambda x:  (re.search(r'"42101', x) is not None) ) 
            chunk = chunk[mask]

            chunk_list.append(chunk)
        data = pd.concat(chunk_list)
 
        print('Total places in patterns file: {}'.format(len(data)))
        data.to_csv(patterns_path + foreign_path + '2020-{}-weekly-patterns.csv.gz'.format(patterns_date), index=True)


    #### Compute contact networks
    place_cbg_map = pd.read_csv(core_path+'placeCountyCBG.csv',
                             dtype={'CBGFIPS':str, 'countyFIPS':str},
                             index_col='safegraph_place_id')
    for patterns_date in pattern_dates:
        print("--loading foreign patterns file {}".format(patterns_date))
        data = pd.read_csv(patterns_path + foreign_path + '2020-{}-weekly-patterns.csv.gz'.format(patterns_date),
             index_col = 'safegraph_place_id')

        data['top_category'] = -1
        prior_dict = {}
        norm_factor = np.array([1]*168)

        network = ccc.place_cbg_contacts_table(data, norm_factor,
                                           prior_dict,
                                           GEOID_type = 'CBG',
                                           plain_visits = True)
        mask = network.origin_census_block_group.apply(lambda x: x[:5] == '42101')
        network = network[mask]

        network = network.join(place_cbg_map[['countyFIPS','countyName','state']], how='inner')
        network.to_csv('../stats/time_series/networks/abroad_network_{}.csv'.format(patterns_date), index = True)
        #append county, county_name, state


if __name__ == '__main__':
    main(core_path = '../core_places/',
         patterns_path = '../weekly_patterns/')