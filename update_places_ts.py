import numpy as np
import getopt, sys
import os
import pandas as pd
import datetime
import re
import pdb
import subprocess

month_names = ['January', 'February', 'March', 'April',
               'May', 'June', 'July', 'August',
               'September', 'October', 'November', 'December']

def main(county, core_path, patterns_path, networks_path, output_path):
    county_places = pd.read_csv(
        core_path+'places-'+ county +'.csv',
        index_col='safegraph_place_id') 
    #create dataframe
    places_ts = pd.DataFrame(
        columns=['date', 'top_category', 'estimated_visits', 'expected_contacts'])
    #load all bipartite networks in order
    files = os.listdir(networks_path)
    files = [x for x in files if re.search(r'bipartite',x) is not None]
    for file in files:
        #extract date
        date = re.search(r'[a-z_]*(.*)\.csv', file).group(1)
        month, day = date.split('-')
        date = month_names[int(month)]+'-'+day
        #open network file

        df = pd.read_csv(
            networks_path+file)
        #aggregate by place
        df = df.groupby('safegraph_place_id').sum()
        df.drop('origin_census_block_group',inplace=True, axis=1)
        #join with core places to have category
        df=df.join(county_places[['top_category']], how='inner')
        df=df.groupby('top_category').sum()
        #round and divide contacts by 2
        df['estimated_visits'] = df['estimated_visits'].round()
        df['expected_contacts'] = (df['expected_contacts']/2).round()

        df.reset_index(inplace=True)
        df.insert(
            loc=0,
            column='date',
            value= [date]*len(df))
        places_ts = pd.concat([places_ts, df])
    places_ts.to_csv(
        output_path+'places_time_series-42101.csv',
        index=False)

    cmd='aws s3 sync ../stats/time_series/ s3://edu-upenn-wattslab-covid'
    result = subprocess.run(cmd, shell=True, universal_newlines=True)
    result.check_returncode()   
    print('-- Finished synching time series to bucket')    
    return(0)


if __name__ == '__main__':
    main(
        county = '42101',
        core_path= "../core_places/",
        patterns_path="../weekly_patterns/",
        networks_path='../stats/time_series/networks/',
        output_path='../stats/time_series/places_time_series/')