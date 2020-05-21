"""
Preparation of time series of compliance metrics
"""
import numpy as np
import pandas as pd
import io
import requests
import pdb
import math

def sync_caseloads(county):
    month_names = ['January', 'February', 'March', 'April',
                   'May', 'June', 'July', 'August',
                   'September', 'October', 'November', 'December']

    url = 'https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv'
    s= requests.get(url).content 
    df = pd.read_csv(io.StringIO(s.decode('utf-8')), dtype = {'fips':str}, parse_dates = ['date'])
    nyc_index = [x == 'New York City' for x in df['county']]
    df.loc[nyc_index, 'fips'] = '36061'

    df.set_index('fips', drop = True, inplace = True)
    #subset to county
    df_county = df.loc[county].copy()
    
    #transform date into month-dd format
    df_county.set_index('date', drop = False, inplace=True)

    df_county['date'] = [month_names[time.month - 1] + '-' +str(time.day).zfill(2)
     for time in  df_county['date']]

    #daily caseload
    daily_load = df_county['cases'].diff()
    daily_load[0] = 1
    #daily deaths
    daily_deaths = df_county['deaths'].diff()
    daily_deaths[0] = 0

    df_county['cases'] = daily_load
    df_county['deaths'] = daily_deaths

    df_county[['date', 'cases', 'deaths']].to_csv('../stats/time_series/caseload-{}.csv'.format(county) ,index=False)
    return('Outputed caseload for {}'.format(county))
    