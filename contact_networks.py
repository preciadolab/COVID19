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
import math
import json
import subprocess

sys.path.insert(0, 'auxiliary_functions/')
import json_code as jjj
import dirichlet_mle as dirichlet

def row_exploder_plain(row):
    dict_visitors = json.loads(row['visitor_home_cbgs'])   
    m = len(dict_visitors) 

    stack = [ [key, int(value)] for key, value in dict_visitors.items()]
    cbg_count = np.vstack(stack)
    place_id = np.array([row.name]*m).reshape((m,1))

    return(np.hstack([place_id,cbg_count]))

def row_exploder(row, norm_factor, prior_dict, GEOID_type = 'CBG'):
    #return an nx3 array 
    dict_visitors = json.loads(row['visitor_home_cbgs'])
    #dict_visitors = {k:v for k,v in dict_visitors.items() if v > 4}

    if GEOID_type == 'CT': #aggregate to census tracts
        dict_visitors_ct = {}
        for cbg, count in dict_visitors.items():
            if cbg[:-1] in dict_visitors_ct.keys():
                dict_visitors_ct[cbg[:-1]]+= count
            else:
                dict_visitors_ct[cbg[:-1]] = count
        dict_visitors = dict_visitors_ct

    m = len(dict_visitors)
    place_id = np.array([row.name] * m).reshape((m,1))

    #count number of contacts scaling by norm_factor
    visits_by_hour = np.array(json.loads(row['visits_by_each_hour']))* norm_factor
    if isinstance(row['top_category'],str):
        prior = prior_dict[row['top_category']]
    else:
        prior = 0
    #Expectation of posterior Dirichlet distribution of multinomials i.e. 
    #probability of visiting each hour shrunk towards the mean
    if(np.sum(visits_by_hour + prior)>0):
        posterior = (visits_by_hour + prior)/np.sum(visits_by_hour + prior)
    else:
        posterior = np.array([0]*168).reshape(1,168)
    posterior_norm = np.sum(posterior*posterior)
    #how many of the visits correspond to the observable visits?
    #suppose that it is the same proportion as observable visitors to total visitors
    V = np.sum(visits_by_hour) *(np.sum([int(v) for k,v in  dict_visitors.items()])/row['raw_visitor_counts'])
      #expected contacts depends on proportion of visits from a given CBG
    observed_visitors = np.sum([value for key, value in dict_visitors.items()])
    stack = [ [key, V*int(value)/observed_visitors] for key, value in dict_visitors.items()]
    if len(stack) == 0:
        return(None)
    cbg_count = np.vstack(stack)
    if GEOID_type == 'CT' or plain_visits:
        return(np.hstack([place_id,cbg_count]))

    #distribute unobserved visits of cbgs with 1 visit among observed cbgs
    cbg_expected_contacts = cbg_count[:,1].astype(float)*(V - cbg_count[:,1].astype(float)/2 - 1/2)*posterior_norm
    posterior_norm = np.array([posterior_norm] * m).reshape((m,1))
    return(np.hstack([place_id,cbg_count, cbg_expected_contacts.reshape((m,1)), posterior_norm]))

def place_cbg_contacts_table(pd_patterns, norm_factor, prior_dict, GEOID_type = 'CBG', plain_visits = False):
    #vstack hstack of iterrows
    if plain_visits:
        stack = [row_exploder_plain(row) for i, row in pd_patterns.iterrows() if len(row['visitor_home_cbgs']) > 2]
    else:
        stack = [row_exploder(row, norm_factor, prior_dict, GEOID_type = 'CBG') for i, row in pd_patterns.iterrows() if len(row['visitor_home_cbgs']) > 2]
    
    stack = [x for x in stack if x is not None]
    arr_ = np.vstack(stack)
    #form into a dataframe and add double indexing
    if plain_visits:
        df = pd.DataFrame(arr_, columns=['safegraph_place_id', 'origin_census_block_group', 'estimated_visits'])
        df = df.astype({'estimated_visits': 'float64'}, copy = False)
    else:
        df = pd.DataFrame(arr_, columns=['safegraph_place_id', 'origin_census_block_group', 'estimated_visits', 'expected_contacts', 'posterior_norm'])
        df = df.astype({'estimated_visits': 'float64', 'expected_contacts': 'float64', 'posterior_norm': 'float64'}, copy = False)
    #set origin_census_block_group as index
    df.set_index('safegraph_place_id', drop= True, inplace = True)
    return(df)

def place_ct_contacts_table(pd_patterns, norm_factor, prior_dict):
    #vstack hstack of iterrows
    stack = [row_exploder(row, norm_factor, prior_dict, GEOID_type = 'CT') for i, row in pd_patterns.iterrows() if len(row['visitor_home_cbgs']) > 2]
    stack = [x for x in stack if x is not None]
    arr_ = np.vstack(stack)
    #form into a dataframe and add double indexing
    df = pd.DataFrame(arr_, columns=['safegraph_place_id', 'origin_census_tract', 'estimated_visits'])
    df = df.astype({'estimated_visits': 'float64'}, copy = False)
    #set origin_census_block_group as index
    df.set_index('safegraph_place_id', drop= True, inplace = True)
    return(df)


def edge_creator(row, contact_weights, self_index):
    contact_weights[self_index] = (contact_weights[self_index] -1)/2
    edges_df = pd.DataFrame(row['posterior_norm']*row['estimated_visits']*contact_weights).reset_index()
    edges_df.columns = ['destination_cbg', 'expected_contacts']
    edges_df['origin_cbg'] = row.name
    return(edges_df)

def inplace_contact_net(df):
    #For each CBG, compute expected contacts with other CBGs
    df.reset_index(drop = True, inplace=True)
    df.set_index('origin_census_block_group', inplace=True)
    #create a pandas data frame with node1, node2, weight
    #return list of data.frames
    return([edge_creator(row, df['estimated_visits'], i) for i, row in df.iterrows()])

def obtain_prior(county_patterns, norm_factor):
    print("hello world")

def contact_networks(county, core_path, patterns_path):
    #Load data about places (to get area) and patterns
    county = '42101'
    county_places = pd.read_csv(core_path+'places-'+ county +'.csv', index_col='safegraph_place_id')

    pattern_dates = [x[5:10] for x in sorted(os.listdir(patterns_path+'main-file-'+ county +'/'))]
    #Create visitor table, weighing daily visits by active users in the panel 

    for patterns_date in pattern_dates:
        print("--loading patterns file {}".format(patterns_date))
        county_patterns = pd.read_csv(patterns_path+'main-file-'+ county +'/2020-{}-weekly-patterns.csv.gz'.format(patterns_date),
                index_col='safegraph_place_id')
        #Load normalization factor
        normalization = pd.read_csv(patterns_path+'normalization_stats/2020-{}-normalization.csv'.format(patterns_date))
        norm_factor = normalization['normalization_factor'].values
        #COMPUTE PRIORS
        #join sub_category column to patterns, expand restaurants top category
        county_patterns= county_patterns.join(county_places[['top_category','sub_category']], how='inner')
        restaurants = county_patterns['top_category'] == 'Restaurants and Other Eating Places'
        county_patterns.loc[restaurants, 'top_category'] = county_patterns.loc[restaurants, 'sub_category']
        #for each top_category populate a dict of priors by fitting a Dirichlet
        prior_dict = {}
        for category in county_patterns.top_category.value_counts().index:
            places_in_cat = county_patterns['top_category'] == category
            dirich_samples = [np.array(json.loads(x))*norm_factor for x in county_patterns.loc[places_in_cat, 'visits_by_each_hour'] ] 
            prior_dict[category] = dirichlet.getInitAlphas(dirich_samples)

        place_cbgs = place_cbg_contacts_table(county_patterns, norm_factor, prior_dict)
        place_cbgs = place_cbgs.loc[place_cbgs['expected_contacts']>0]

        place_cbgs[['origin_census_block_group', 'estimated_visits', 'expected_contacts']].to_csv('../stats/time_series/networks/bipartite_network_{}.csv'.format(patterns_date),
                            index = True)
        #Now we construct non-bipartite network for the same week.
        #For each place we construct an edge list, then we aggregate with a double key
        temp_df= [ inplace_contact_net(place_cbgs.loc[[place_id]]) for place_id in set(place_cbgs.index)]
        temp_df = [j for i in temp_df for j in i]
        print('Finished constructing distributed lists of edges')
        large_df = pd.concat(temp_df, ignore_index=True)
        print('Finished merging lists of edges')
        contact_net = large_df.groupby(['origin_cbg','destination_cbg']).sum()
        contact_net.to_csv('../stats/time_series/networks/contact_network_{}.csv'.format(patterns_date), index=True)
        print('Finished exporting networks')

    cmd='aws s3 sync ../stats/time_series/ s3://edu-upenn-wattslab-covid'
    result = subprocess.run(cmd, shell=True, universal_newlines=True)
    result.check_returncode()   
    print('-- Finished synching time series to bucket')

if __name__ == '__main__':
    contact_networks(county = '42101',
                     core_path= "../core_places/",
                     patterns_path="../weekly_patterns/")