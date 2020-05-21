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
import io
import json
import math

sys.path.insert(0, 'auxiliary_functions/')
sys.path.insert(0, './')
import json_code as jjj
import contact_networks as ccc
import data_structuring as ddd #ratio_fun #merge_dicts #one_rower #ts_from_var #visit_parser
import dirichlet_mle as dirichlet

def get_metrics_dict(row, date, place_cbgs, county_places, type_ = 'cbg'):
    if row['origin_census_block_group'] in place_cbgs.index:
        total_visits = np.round(np.sum(place_cbgs.loc[row['origin_census_block_group']]['estimated_visits']), 0)
        expected_contacts = np.round(np.sum(place_cbgs.loc[row['origin_census_block_group']]['expected_contacts']), 0)
        places_dict = {row['safegraph_place_id']:ddd.visit_parser(row, county_places) for i, row in place_cbgs.loc[[row['origin_census_block_group']]].iterrows()}
    else:
        total_visits=0
        expected_contacts = 0
        places_dict = {}
    visited_cbgs_dict = json.loads(row['destination_cbgs'])
    if row['origin_census_block_group'] in visited_cbgs_dict.keys():
        visited_cbgs_dict[row['origin_census_block_group']] = visited_cbgs_dict[row['origin_census_block_group']] - row['completely_home_device_count']
    visited_cbgs_dict = {k:v for k,v in visited_cbgs_dict.items() if v >1}
    
    attribute_dict = {'type':type_ , 'low_device_count': {date:(row['within_cbg_count']/row['candidate_device_count'] < 0.15)}, 'covid':{
                 date: {
                          'pct_at_home': ddd.ratio_fun(row['completely_home_device_count'], row['within_cbg_count']),
                          'pct_within_neighborhood': ddd.ratio_fun(row['within_neighborhood']+row['completely_home_device_count'], row['within_cbg_count']),
                          'median_distance_traveled': 0 if math.isnan(row['distance_traveled_from_home']) else row['distance_traveled_from_home'],
                          'median_percentage_time_home': row['median_percentage_time_home'],
                          'total_visits_to_places': total_visits,
                          'normalized_visits_to_places': ddd.ratio_fun(total_visits, row['within_cbg_count']),
                          'total_expected_contacts': expected_contacts,
                          'places_visited': places_dict,
                          'cbgs_visited' : visited_cbgs_dict 
                         }
                 }}
    return(attribute_dict)

def collapse_low_device(metrics):
    for key, value in metrics.items():
        value['low_device_count'] = int(np.median([v1 for k1, v1 in value['low_device_count'].items()]))

def compliance_time_series(county, core_path , patterns_path, backfill = False):
    """
    Function assumes that there already exists a subset of core places for the given county
    as well as a subset of the social distancing metrics and weekly patterns
    """
    #Load data about places and patterns

    county_places = pd.read_csv(core_path+'places-'+ county +'.csv', index_col='safegraph_place_id')   

    month_names = ['January', 'February', 'March', 'April',
                   'May', 'June', 'July', 'August',
                   'September', 'October', 'November', 'December']
    
    pattern_dates = [x[5:10] for x in sorted(os.listdir(patterns_path+'main-file-'+ county +'/'))]
    w = 0
    next_date = pattern_dates[w]
    #Create visitor table
    place_cbgs = pd.DataFrame() #First two months don't have weekly patterns, use empty data.frame

    #List files for social distancing metrics in that county
    months_path = '../social_distancing/social_dist_'+ county +'/'
    month_list = sorted(os.listdir(months_path))
    os.makedirs( '../stats/time_series/', exist_ok = True)
    #metrics dictionary to be filled looping through every day
    if not os.path.isfile('../stats/time_series/metrics_{}.json'.format(county)) or backfill:
        metrics = {}
        existing_dates = []
    else:
        with open('../stats/time_series/metrics_{}.json'.format(county)) as fp:
            metrics = json.load(fp)
    #obtain the dates we already processed
        existing_dates = [date for date in 
                            metrics[next(iter(metrics))]['covid'].keys()]
    changed = False
    for month in month_list:
        #Loop through every day
        day_list = sorted(os.listdir(months_path + month))
        for day in day_list:
            date_name = month_names[int(month)-1] + '_' + day
            if date_name in existing_dates:
                break
            print(date_name)
            if month+'-'+day == next_date:
                print("--changing to next patterns file")
                county_patterns = pd.read_csv(patterns_path + 'main-file-{}/2020-{}-weekly-patterns.csv.gz'.format(county, next_date),
                    index_col='safegraph_place_id')
                normalization = pd.read_csv(patterns_path + 'normalization_stats-{}/2020-{}-normalization.csv'.format(county, next_date))
                norm_factor = normalization['normalization_factor'].values

                county_patterns= county_patterns.join(county_places[['top_category','sub_category']], how='inner')
                restaurants = county_patterns['top_category'] == 'Restaurants and Other Eating Places'
                county_patterns.loc[restaurants, 'top_category'] = county_patterns.loc[restaurants, 'sub_category']

                prior_dict = {}
                for category in county_patterns.top_category.value_counts().index:
                    places_in_cat = county_patterns['top_category'] == category
                    dirich_samples = [np.array(json.loads(x))*norm_factor for x in county_patterns.loc[places_in_cat, 'visits_by_each_hour'] ] 
                    prior_dict[category] = dirichlet.getInitAlphas(dirich_samples)

                place_cbgs = ccc.place_cbg_contacts_table(county_patterns, norm_factor, prior_dict)
                print('--computed bipartite contact network')
                place_cbgs = place_cbgs.loc[place_cbgs['expected_contacts']>1]
                w = w+1
                if w < len(pattern_dates):
                    next_date = pattern_dates[w]

            file_name = os.listdir(months_path+ '/' + month+'/'+day)[0]
            data_soc_dist = pd.read_csv(months_path+ '/' + month+'/'+day+'/'+file_name, dtype={'origin_census_block_group':str})

            #Create dictionary with global structure and deep merge
            temp_metrics = {row['origin_census_block_group']:get_metrics_dict(row, date_name, place_cbgs, county_places, 'cbg')
                            for i, row in data_soc_dist.iterrows()}
            ddd.merge_dicts(metrics, temp_metrics)
            changed = True

    if changed:
        collapse_low_device(metrics)
        with open('../stats/time_series/metrics_{}.json'.format(county), 'w+') as fp:
            json.dump(metrics, fp)
    return(0)

# if __name__ == '__main__':
#     compliance_time_series(county = "42101",
#                        core_path = '../core_places/',
#                        patterns_path = '../weekly_patterns/',
#                        backfill = True)