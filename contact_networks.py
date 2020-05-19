"""
Preparation of contact networks based on places
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
import io
import json
sys.path.insert(0, 'auxiliary_functions/')
import json_code as jjj


def row_exploder(row, norm_factor):
    #return an nx3 array 
    dict_visitors = json.loads(row['visitor_home_cbgs'])
    place_id = [row['safegraph_place_id']] * len(dict_visitors)
    #count number of contacts without scaling (FIX with normalization!)
    visits_by_hour = np.array(json.loads(row['visits_by_each_hour']))
    visits_by_hour = visits_by_hour * norm_factor
    #contacts for a given device = # of other devices (possibly zero)
    contacts = np.array([max(0, x-1) for x in visits_by_hour])
    expected_contacts = np.sum((visits_by_hour/np.sum(visits_by_hour))*contacts) #scalar
    cbg_count = np.vstack([ [key, value] for key, value in dict_visitors.items()])
    #distribute unobserved visits of cbgs with 1 visit among observed cbgs
    cbg_expected_contacts = (cbg_count[:,1].astype(int)/np.sum(cbg_count[:,1].astype(int)))*np.sum(visits_by_hour)*expected_contacts
    return(np.hstack([np.array(place_id).reshape((len(place_id),1)),cbg_count, cbg_expected_contacts.reshape((len(place_id),1))]))

def place_cbg_contacts_table(pd_patterns, norm_factor):
    #vstack hstack of iterrows
    arr_ = np.vstack([row_exploder(row, norm_factor) for i, row in pd_patterns.iterrows() if len(row['visitor_home_cbgs']) > 2])
    #form into a dataframe and add double indexing
    df = pd.DataFrame(arr_, columns=['safegraph_place_id', 'origin_census_block_group', 'visits', 'expected_contacts'])
    df = df.astype({'visits': 'int32', 'expected_contacts': 'float64'}, copy = False)
    #set origin_census_block_group as index
    df.set_index('safegraph_place_id', drop= True, inplace = True)
    return(df)

def edge_creator(row, contact_weights):
    edges_df = pd.DataFrame(row['expected_contacts']*contact_weights).reset_index()
    edges_df.columns = ['destination_cbg', 'expected_contacts']
    edges_df['origin_cbg'] = row.name
    return(edges_df)

def inplace_contact_net(df):
    #For each CBG, distribute their expected contacts among the other CBGs
    df.reset_index(drop = True, inplace=True)
    df.set_index('origin_census_block_group', inplace=True)
    contact_weights = df['visits'].astype(int)/np.sum(df['visits'].astype(int))
    #create a pandas data frame with node1, node2, weight
    #return list of data.frames
    return([edge_creator(row, contact_weights) for i, row in df.iterrows()])


def main():
    #Load data about places (to get area) and patterns
    core_path = "../core_places/"
    county = '42101'
    county_places = pd.read_csv(core_path+'places-'+ county +'.csv', index_col='safegraph_place_id')   
    county_places = county_places[['location_name','latitude','longitude']].copy()


    #Create visitor table, weighing daily visits by active users in the panel 
    for patterns_date in [('03','01'),('03','08'),('03','15'),
                     ('03','22'),('03','29'),('04','05'),
                     ('04','12'),('04','19'),('04','26'), ('05','03')]:
        print("--loading patterns file {}-{}".format(patterns_date[0], patterns_date[1]))
        county_patterns = pd.read_csv('../weekly_patterns/main-file-'+ county +'/2020-{}-{}-weekly-patterns.csv.gz'.format(patterns_date[0], patterns_date[1]))
        #Load normalization factor
        normalization = pd.read_csv('../weekly_patterns/normalization_stats/2020-{}-{}-normalization.csv'.format(patterns_date[0], patterns_date[1]))
        norm_factor = normalization['normalization_factor'].values

        place_cbgs = place_cbg_contacts_table(county_patterns, norm_factor)
        place_cbgs = place_cbgs.loc[place_cbgs['expected_contacts']>0]

        place_cbgs.to_csv('stats/bipartite_network_{}-{}.csv'.format(patterns_date[0], patterns_date[1]),
                            index = True)
        #Now we construct non-bipartite network for the same week.
        #For each place we construct an edge list, then we aggregate with a double key
        temp_df= [ inplace_contact_net(place_cbgs.loc[[place_id]]) for place_id in set(place_cbgs.index)]
        temp_df = [j for i in temp_df for j in i]
        print('Finished constructing distributed lists of edges')
        large_df = pd.concat(temp_df, ignore_index=True)
        print('Finished merging lists of edges')
        contact_net = large_df.groupby(['origin_cbg','destination_cbg']).sum()
        contact_net.to_csv('stats/contact_network_{}-{}.csv'.format(patterns_date[0], patterns_date[1]), index=True)
        print('Finished exporting networks')

if __name__ == '__main__':
    main()