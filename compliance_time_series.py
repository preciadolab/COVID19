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

sys.path.insert(0, './descriptive_analysis/')
import descriptive_patterns as aux


def ratio_fun(series1, series2):
    series2 = series2 + 0.001
    return(np.round(series1/series2, 2))

def merge_dicts(a, b, path=None):
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_dicts(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass # same leaf value
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a

def visit_parser(row, phila_places):
    visit_dict = phila_places.loc[row['safegraph_place_id']].to_dict()
    visit_dict['visits'] = row['visits']
    return(visit_dict)

def get_metrics_dict(row, date, place_cbgs, phila_places, type_ = 'cbg'):
    if row['origin_census_block_group'] in place_cbgs.index:
        total_visits = int(np.sum(place_cbgs.loc[row['origin_census_block_group']]['visits']))
        places_dict = {row['safegraph_place_id']:visit_parser(row, phila_places) for i, row in place_cbgs.loc[[row['origin_census_block_group']]].iterrows()}
    else:
        total_visits=0
        places_dict = {}

    attribute_dict = {'type':type_ , 'low_device_count': {date:(row['device_count']-row['permanently_away_device_count'] < 10)}, 'covid':{
                 date: {
                          'pct_at_home': row['pct_at_home'],
                          'pct_within_neighborhood': row['pct_within_neighborhood'],
                          'normalized_visits_to_places': ratio_fun(total_visits, row['device_count']-row['permanently_away_device_count']),
                          'ratio_day_night_device_count': row['ratio_day_night_device_count'],
                          'total_visits_to_places': total_visits,
                          #'permanently_away_device_count':row['permanently_away_device_count'],
                          #'device_count':row['device_count'],
                          #'less_than_10_active_devices':(row['device_count']-row['permanently_away_device_count'] < 10),
                          #'median_devices_home_day':row['median_devices_home_day'],
                          #'median_devices_home_night':row['median_devices_home_night'],
                          'places_visited': places_dict
                         }
                 }}
    return(attribute_dict)
    
def one_rower(key, value, var_name):
    one_row = pd.DataFrame([{k1: v1[var_name] for k1, v1 in value['covid'].items()}])
    one_row[ value['type'] ] = key
    return(one_row)

def ts_from_var(geoid_metrics, var_name):
    list_of_pd = [one_rower(key, value, var_name) for key, value in geoid_metrics.items()]
    return(pd.concat(list_of_pd, sort = False))

def row_exploder(row):
    #return an nx3 array 
    dict_ = json.loads(row['visitor_home_cbgs'])
    place_id = [row['safegraph_place_id']] * len(dict_)
    cbg_count = np.vstack([ [key, value] for key, value in dict_.items()])
    return(np.hstack([np.array(place_id).reshape((len(place_id),1)),cbg_count]))

def place_cbg_table(pd_patterns):
    #vstack hstack of iterrows
    arr_ = np.vstack([row_exploder(row) for i, row in pd_patterns.iterrows() if len(row['visitor_home_cbgs']) > 2])
    #form into a dataframe and add double indexing
    df = pd.DataFrame(arr_, columns=['safegraph_place_id', 'origin_census_block_group', 'visits'])
    df = df.astype({'visits': 'int32'}, copy = False)
    #set origin_census_block_group as index
    df.set_index('origin_census_block_group', drop= True, inplace = True)
    return(df)

def collapse_low_device(geoid_metrics):
    for key, value in geoid_metrics.items():
        value['low_device_count'] = int(np.median([v1 for k1, v1 in value['low_device_count'].items()]))

def main():
    #Load data about places and patterns
    core_path = "../core_places/"
    phila_places = pd.read_csv(core_path+'phila_places.csv', index_col='safegraph_place_id')   
    phila_places = phila_places[['location_name','latitude','longitude']].copy()


    #Create visitor table
    pattern_dates = [('03','01'),('03','08'),('03','15'),
                     ('03','22'),('03','29'),('04','05'),
                     ('04','12'),('100','100')]
    w = 0
    next_date = pattern_dates[w]
    place_cbgs = pd.DataFrame() #First two months don't have weekly patterns

    #List files in folder corresponding to month
    months_path = '../safegraph_social_dist_data/'
    month_list = sorted(os.listdir(months_path))

    geoid_metrics = {}
    geoid_counter = 1
    #Loop through every month
    day_counter = 0

    for month in month_list:
        #Loop through every day
        day_list = sorted(os.listdir(months_path + month))
        for day in day_list:
            if (month, day) == next_date:
                print("--changing to next patterns file")
                phl_patterns = pd.read_csv('../weekly_patterns/main-file-phl/2020-{}-{}-weekly-patterns.csv.gz'.format(next_date[0], next_date[1]))
                place_cbgs = place_cbg_table(phl_patterns)
                place_cbgs = place_cbgs.loc[place_cbgs['visits']>4]
                w = w+1
                next_date = pattern_dates[w]

            day_counter = day_counter+1
            file_name = os.listdir(months_path+ '/' + month+'/'+day)[0]
            data = pd.read_csv(months_path+ '/' + month+'/'+day+'/'+file_name,
             dtype={'origin_census_block_group':str})
            date_name = ['January', 'February', 'March', 'April'][int(month)-1] + '_' + day
            print(date_name)
            #Create dictionary with global structure and deep merge
            temp_metrics = {row['origin_census_block_group']:get_metrics_dict(row, date_name, place_cbgs, phila_places, 'cbg') for i, row in data.iterrows()}
            merge_dicts(geoid_metrics, temp_metrics)
            if day_counter == 10 or date_name == 'April_23':
                collapse_low_device(geoid_metrics)
                with open('stats/geoid_metrics_{}.json'.format(geoid_counter), 'w+') as fp:
                    json.dump(geoid_metrics, fp)
                geoid_metrics = {}
                day_counter = 0
                geoid_counter = geoid_counter + 1
    with open('stats/geoid_metrics.json', 'w+') as fp:
        json.dump(geoid_metrics, fp)

    #subset dict of dicts to obtain time series...
    var_list = ['total_visits_to_places', 'normalized_visits_to_places']
    for var_name in var_list:
        print(var_name)
        df = ts_from_var(geoid_metrics, var_name)
        df.to_csv('stats/'+var_name+'.csv', index=False)

    var_name = 'ratio_day_night_device_count'
    [ [v1['ratio_day_night_device_count'] for k1, v1 in v2['covid'].items()] for k2, v2 in geoid_metrics.items()]

    pdb.set_trace()


if __name__ == '__main__':
    main()