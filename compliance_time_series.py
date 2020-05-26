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
from time import time

sys.path.insert(0, 'auxiliary_functions/')
sys.path.insert(0, './')
import json_code as jjj
import contact_networks as ccc
import data_structuring as ddd #ratio_fun #merge_dicts #one_rower #ts_from_var #visit_parser
import dirichlet_mle as dirichlet

def update_metrics_columns(row, date_name, place_cbgs, county_places, columns):

    if row['origin_census_block_group'] in place_cbgs.index:
        total_visits = float(np.round(np.sum(place_cbgs.loc[row['origin_census_block_group']]['estimated_visits']), 0))
        expected_contacts = float(np.round(np.sum(place_cbgs.loc[row['origin_census_block_group']]['expected_contacts']), 2))
        places_dict = place_cbgs.loc[[row['origin_census_tract']]].set_index('safegraph_place_id').to_dict('index')
    else:
        total_visits=0
        expected_contacts = 0
        places_dict = {}
    visited_cbgs_dict = json.loads(row['destination_cbgs'])
    if row['origin_census_block_group'] in visited_cbgs_dict.keys():
        #subtract devices that stayed at home all day
        visited_cbgs_dict[row['origin_census_block_group']] = visited_cbgs_dict[row['origin_census_block_group']] - row['completely_home_device_count']
    visited_cbgs_dict = {k:v for k,v in visited_cbgs_dict.items() if v >1}
    
    #date and cbg will go as double keys
    columns['date']+= [date_name]
    columns['origin_census_block_group']+= [row['origin_census_block_group']]
    columns['low_device_count']+= [row['within_cbg_count']/row['candidate_device_count'] < 0.15]
    columns['pct_at_home']+= [ddd.ratio_fun(row['completely_home_device_count'], row['within_cbg_count'])]
    columns['pct_within_neighborhood']+= [ddd.ratio_fun(row['within_neighborhood'], row['within_cbg_count'])]
    columns['median_distance_traveled']+= [0 if math.isnan(row['distance_traveled_from_home']) else row['distance_traveled_from_home']]
    columns['median_percentage_time_home']+= [row['median_percentage_time_home']/100]
    columns['total_visits_to_places']+= [total_visits]
    columns['normalized_visits_to_places']+= [ddd.ratio_fun(total_visits, row['within_cbg_count'])]
    columns['total_expected_contacts']+= [expected_contacts]
    columns['places_visited']+= [json.dumps(places_dict)]
    columns['cbgs_visited']+= [json.dumps(visited_cbgs_dict)]    
    return(None)

def update_metrics_columns_CT(row, date_name, place_cts, county_places, columns):
    if row['origin_census_tract'] in place_cts.index:
        #sum of all estimated visits from that CT in the bipartite graph place_cbgs
        total_visits = float(np.round(np.sum(place_cts.loc[row['origin_census_tract']]['estimated_visits']), 0)) 
        places_dict = place_cts.loc[[row['origin_census_tract']]].set_index('safegraph_place_id').to_dict('index')
    else:
        total_visits=0
        places_dict = {}
    #date and cbg will go as double keys
    columns['date']+= [date_name]
    columns['origin_census_tract']+= [row['origin_census_tract']]
    columns['low_device_count']+= [row['within_cbg_count']/row['candidate_device_count'] < 0.15]
    columns['pct_at_home']+= [ddd.ratio_fun(row['completely_home_device_count'], row['within_cbg_count'])]
    columns['pct_within_neighborhood']+= [ddd.ratio_fun(row['within_neighborhood'], row['within_cbg_count'])]
    columns['median_distance_traveled']+= [row['median_distance_traveled']]
    columns['median_percentage_time_home']+= [row['median_percentage_time_home']/100]
    columns['total_visits_to_places']+= [total_visits]
    columns['normalized_visits_to_places']+= [ddd.ratio_fun(total_visits, row['within_cbg_count'])] # device count instead
    columns['places_visited']+= [json.dumps(places_dict)]
    return(None)


def collapse_low_device(metrics): #deprecated for csv version
    for key, value in metrics.items():
        value['low_device_count'] = int(np.median([v1 for k1, v1 in value['low_device_count'].items()]))

def compliance_time_series(county, core_path , patterns_path, backfill = False, GEOID_type = 'CBG'):
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
    place_cts = pd.DataFrame() #First two months don't have weekly patterns, use empty data.frame

    #List files for social distancing metrics in that county
    months_path = '../social_distancing/social_dist_'+ county +'/'
    month_list = sorted(os.listdir(months_path))
    os.makedirs( '../stats/time_series/', exist_ok = True)
    #metrics dictionary to be filled looping through every day
    if not os.path.isfile('../stats/time_series/metrics_{}_CT.csv'.format(county)) or backfill:
        metrics = pd.DataFrame()
        existing_dates = []
    else:
        metrics = pd.read_csv('../stats/time_series/metrics_{}_CT.csv'.format(county), dtype = {'origin_census_tract':str})
        #dates already processed
        existing_dates = metrics['date'].unique() #series of unique dates

    #Initialize columns of new data frame
    if GEOID_type == 'CBG':
        columns= {'date':[], 'origin_census_block_group':[], 'low_device_count':[], 'pct_at_home':[], 'pct_within_neighborhood':[], 'median_distance_traveled':[],
                  'median_percentage_time_home':[], 'total_visits_to_places':[], 'normalized_visits_to_places':[], 'total_expected_contacts':[], 'places_visited':[], 'cbgs_visited':[]}
    if GEOID_type == 'CT':
        columns= {'date':[], 'origin_census_tract':[], 'low_device_count':[], 'pct_at_home':[], 'pct_within_neighborhood':[], 'median_distance_traveled':[],
                  'median_percentage_time_home':[], 'total_visits_to_places':[], 'normalized_visits_to_places':[], 'places_visited':[]}        
    changed = False
    for month in month_list:
        #Loop through every day
        day_list = sorted(os.listdir(months_path + month))
        for day in day_list: 
            date_name = month_names[int(month)-1] + '_' + day
            print(date_name)
            if month+'-'+day == next_date:
                w = w+1
                if w < len(pattern_dates):
                    next_date = pattern_dates[w]
                if date_name in existing_dates:
                    continue
                print("--changing to next patterns file")
                county_patterns = pd.read_csv(patterns_path + 'main-file-{}/2020-{}-weekly-patterns.csv.gz'.format(county, next_date),
                    index_col='safegraph_place_id')
                normalization = pd.read_csv(patterns_path + 'normalization_stats-{}/2020-{}-normalization.csv'.format(county, next_date))
                norm_factor = normalization['normalization_factor'].values

                #Establish prior for hourly distribution of visits at the top_category level
                county_patterns= county_patterns.join(county_places[['top_category','sub_category']], how='inner')
                restaurants = county_patterns['top_category'] == 'Restaurants and Other Eating Places'
                county_patterns.loc[restaurants, 'top_category'] = county_patterns.loc[restaurants, 'sub_category']

                prior_dict = {}
                for category in county_patterns.top_category.value_counts().index:
                    places_in_cat = county_patterns['top_category'] == category
                    dirich_samples = [np.array(json.loads(x))*norm_factor for x in county_patterns.loc[places_in_cat, 'visits_by_each_hour'] ] 
                    prior_dict[category] = dirichlet.getInitAlphas(dirich_samples)
                if GEOID_type == 'CBG':
                    place_cbgs = ccc.place_cbg_contacts_table(county_patterns, norm_factor, prior_dict)
                    place_cbgs = place_cbgs.loc[place_cbgs['expected_contacts']>1]
                    place_cbgs = place_cbgs.join(county_places[['location_name','latitude','longitude']], how='inner')
                    place_cbgs.reset_index(inplace=True, drop=False)
                    place_cbgs.set_index('origin_census_block_group', inplace=True, drop=True)
                if GEOID_type == 'CT':
                    place_cts = ccc.place_ct_contacts_table(county_patterns, norm_factor, prior_dict)
                    place_cts = place_cts.join(county_places[['location_name','latitude','longitude']], how='inner')
                    place_cts.reset_index(inplace=True, drop=False)
                    place_cts.set_index('origin_census_tract', inplace=True, drop=True)
                print('--computed bipartite contact network')

            if date_name in existing_dates:
                continue


            file_name = os.listdir(months_path+ '/' + month+'/'+day)[0]
            data_soc_dist = pd.read_csv(months_path+ '/' + month+'/'+day+'/'+file_name, dtype={'origin_census_block_group':str})
            data_soc_dist['median_distance_traveled_from_home'] = [0 if math.isnan(x) else x for x in  data_soc_dist['distance_traveled_from_home']] #NOT NEEDED?
            
            if GEOID_type == 'CBG':
                [update_metrics_columns(row, date_name, place_cbgs, county_places, columns) for i, row in data_soc_dist.iterrows()]
            else:
                ####CREATE FUNCTION THAT AGGREGATES TO CENSUS TRACT
                data_soc_dist['origin_census_tract'] = [x[:-1] for x in data_soc_dist['origin_census_block_group']]
                ct_data_soc_dist = data_soc_dist.groupby('origin_census_tract').apply(ddd.aggregate_to_ct).reset_index()
                [update_metrics_columns_CT(row, date_name, place_cts, county_places, columns) for i, row in ct_data_soc_dist.iterrows()]
            changed = True

    print("--merging rows from new dates")
    new_metrics = pd.DataFrame.from_dict(columns, orient='index').transpose()

    metrics = pd.concat([metrics, new_metrics], ignore_index=True)
    if changed:
        if GEOID_type == 'CT':
            metrics.to_csv('../stats/time_series/metrics_{}_CT.csv'.format(county),index=False)
        else:
            metrics.to_csv('../stats/time_series/metrics_{}_CBG.csv'.format(county),index=False)
    print("--finished updating time series for {}".format(county))
    return(0)

if __name__ == '__main__':
    compliance_time_series(county = "42101",
                        core_path = '../core_places/',
                        patterns_path = '../weekly_patterns/',
                        backfill = True,
                        GEOID_type = 'CT')