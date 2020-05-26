import numpy as np
import pandas as pd
import pdb
import os

def ratio_fun(x, y):
    if y == 0:
        return(10000)
    else:
        return(float(round(x/y, 2)))

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

def split_time_series(county, GEOID_type):
    #read in corresponding file from stats/time_series/
    df = pd.read_csv('../stats/time_series/metrics_{}_{}.csv'.format(county, GEOID_type))
    os.makedirs('../stats/time_series/'+ county +'/', exist_ok = True)
    for col_name in df.columns:
        if col_name in ['date', 'origin_census_tract', 'origin_census_block_group']: continue
        if GEOID_type == 'CBG':
            if col_name == 'places_visited':
                df_day = df[['date', 'origin_census_block_group', 'places_visited']]
                df_day.set_index('date', inplace=True, drop=False)
                for day in df_day['date'].unique():
                    df_day.loc[day].to_csv('../stats/time_series/'+ county +'/metrics_{}_{}-{}-{}.csv'.format(county, GEOID_type, col_name, day), index=False)
            else:
                df[['date', 'origin_census_block_group', col_name]].to_csv('../stats/time_series/'+ county +'/metrics_{}_{}-{}.csv'.format(county, GEOID_type, col_name), index=False)
        if GEOID_type == 'CT':
            if col_name == 'places_visited':
                df_day = df[['date', 'origin_census_tract', 'places_visited']]
                df_day.set_index('date', inplace=True, drop=False)
                for day in df_day['date'].unique():
                    df_day.loc[day].to_csv('../stats/time_series/'+ county +'/metrics_{}_{}-{}-{}.csv'.format(county, GEOID_type, col_name, day), index=False)
            else:
                df[['date', 'origin_census_tract', col_name]].to_csv('../stats/time_series/'+ county +'/metrics_{}_{}-{}.csv'.format(county, GEOID_type, col_name), index=False)
    return("Finished splitting time series for {}".format(county))

def weighted_median(data, weights):
    """
    Args:
      data (list or numpy.array): data
      weights (list or numpy.array): weights
    """
    if len(data) ==1: return(data[0])
    data, weights = np.array(data).squeeze(), np.array(weights).squeeze()
    s_data, s_weights = map(np.array, zip(*sorted(zip(data, weights))))
    midpoint = 0.5 * sum(s_weights)
    if any(weights > midpoint):
        w_median = (data[weights == np.max(weights)])[0]
    else:
        cs_weights = np.cumsum(s_weights)
        idx = np.where(cs_weights <= midpoint)[0][-1]
        if cs_weights[idx] == midpoint:
            w_median = np.mean(s_data[idx:idx+2])
        else:
            w_median = s_data[idx+1]
    return w_median

def aggregate_to_ct(data):
    df_dict={
    'within_cbg_count':data['within_cbg_count'].sum(),
    'candidate_device_count':data['candidate_device_count'].sum(),
    'completely_home_device_count':data['completely_home_device_count'].sum(),
    'within_neighborhood':data['within_neighborhood'].sum(),
    'median_distance_traveled':weighted_median(data['median_distance_traveled_from_home'].values,
                                               data['device_count'].values),
    'median_percentage_time_home':weighted_median(data['median_percentage_time_home'].values,
                                                  data['device_count'].values)
    }
    return(pd.DataFrame.from_dict(df_dict, orient='index').transpose())