import json
import pandas as pd
import os
import re
import geohash
import pdb
import numpy as np
from datetime import datetime, timedelta


def expand_row_visits(id_,dict_visits, date, norm_factor):
    dict_visits.pop('visitors')
    dict_visits['visits'] = round(dict_visits['visits']*norm_factor)
    dict_visits['date']= date
    dict_visits['id'] = id_
    df = pd.DataFrame.from_dict(dict_visits, orient='index').transpose()
    df.columns = ['name', 'estimated_visits', 'date', 'place_id']
    return(df[['date','name', 'estimated_visits', 'place_id']])

def get_day_interval(date_start, num_days):
    date_start = date_start + '-2020'

    date_start = datetime.strptime(date_start, "%m-%d-%Y")
    day_list = [ date_start+timedelta(days=t) for t in range(num_days)]
    return([str(x.month).zfill(2)+'-'+str(x.day).zfill(2) for x in day_list])

def merge_visit_dict(glob_dict, new_dict, norm_factor = 1):
    #merge dictionaries in place
    #ignore name, lat, lon, add visits and append visitors
    for k, v in new_dict.items():
        p = geohash.decode(k) #CORRECT: it is findVisits responsibility
        v['latitude'] = p[0]
        v['longitude'] = p[1] 
        if k not in glob_dict.keys():
            glob_dict[k] = v
            glob_dict[k]['visitors'] = set(v['visitors']) 
            glob_dict[k]['visitor_count'] = round(len(glob_dict[k]['visitors'])*norm_factor)
        else:
            glob_dict[k]['visits'] += v['visits']*norm_factor
            glob_dict[k]['visitor_count'] = round(len(glob_dict[k]['visitors'])*norm_factor)
            glob_dict[k]['visitors'] = glob_dict[k]['visitors'].union(set(v['visitors']))
    return None

def main(json_path, patterns_path, output_path):
    #Load normalization factors
    normalization = pd.read_csv(patterns_path+'normalization_stats-42101/2020-daily.csv',
                                index_col = 'date')

    df_food_sites = []
    file_list = os.listdir(json_path)
    for file in file_list:
        with open(json_path + file) as fp:
            visits = json.load(fp)
        #date - site - visits - visitors

        date = re.match(r'food_visits_(.+)\.json', file).group(1)
        norm_factor = normalization.loc[date, 'normalization_factor']
        df_food_sites += [expand_row_visits(k,v, date, norm_factor) for k, v in visits.items()]
    df_food_sites = pd.concat(df_food_sites)
    df_food_sites.to_csv(output_path + 'food_sites_time_series.csv',index=False)


    #json for plotting id, name, lat, lon, total_visits, total_visitors, cbgs of origin
    #Time range
    day_interval = get_day_interval('05-18', 7)
    file_list = [file for file in os.listdir(json_path) if re.match(r'food_visits_(.+)\.json', file).group(1) in day_interval] 
    #Visitor homes
    home_cbg = pd.read_csv('../../stats/user_homes_upd.csv',
                           dtype = {'home_cbg':str},
                           index_col = 'visitor')
    food_js = {}
    file_list = os.listdir(json_path)
    print("merging visit dicts")
    for file in file_list:
        date = re.match(r'food_visits_(.+)\.json', file).group(1)
        norm_factor = normalization.loc[date, 'normalization_factor']
        with open(json_path + file) as fp:
            visits = json.load(fp)

        merge_visit_dict(food_js, visits, norm_factor)
    #reduce visitors to cbg counts
    for id_, dict_ in food_js.items():
        counts = home_cbg.loc[dict_['visitors'], 'home_cbg'].value_counts()
        dict_['visitor_cbg'] = counts[counts > 1].to_dict()
        dict_.pop('visitors', None)
    food_js = {k:v for k,v in food_js.items() if len(v['visitor_cbg'])>0}
    print("{} out of 148 food sites have visits".format(len(food_js)))
    #filter out sites with no visits
    with open(output_path + 'food_sites_visitors_05-18.json', 'w+') as fp:
        json.dump(food_js, fp)


    day_interval = get_day_interval('04-18', 7)
    file_list = [file for file in os.listdir(json_path) if re.match(r'food_visits_(.+)\.json', file).group(1) in day_interval] 
    #Visitor homes
    home_cbg = pd.read_csv('../../stats/user_homes_upd.csv',
                           dtype = {'home_cbg':str},
                           index_col = 'visitor')
    food_js = {}
    file_list = os.listdir(json_path)
    print("merging visit dicts")
    for file in file_list:
        date = re.match(r'food_visits_(.+)\.json', file).group(1)
        norm_factor = normalization.loc[date, 'normalization_factor']
        with open(json_path + file) as fp:
            visits = json.load(fp)

        merge_visit_dict(food_js, visits, norm_factor)
    #reduce visitors to cbg counts
    for id_, dict_ in food_js.items():
        counts = home_cbg.loc[dict_['visitors'], 'home_cbg'].value_counts()
        dict_['visitor_cbg'] = counts[counts > 1].to_dict()
        dict_.pop('visitors', None)
    #put breakpoint here
    food_js = {k:v for k,v in food_js.items() if len(v['visitor_cbg'])>0}
    print("{} out of 148 food sites have visits".format(len(food_js)))
    #filter out sites with no visits
    with open(output_path + 'food_sites_visitors_04-18.json', 'w+') as fp:
        json.dump(food_js, fp)
    #map visitor set to homes



if __name__ =='__main__':
    main(json_path = '../../stats/findVisitsResults/',
         patterns_path = '../../weekly_patterns/',
         output_path = '../../stats/food_sites_time_series/')