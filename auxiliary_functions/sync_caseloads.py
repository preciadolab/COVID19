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
    daily_load = [int(x) if x>= 0 else 0 for x in df_county['cases'].diff()]
    daily_load[0] = df_county['cases'].values[0]
    #daily deaths
    daily_deaths = [int(x) if x>= 0 else 0 for x in df_county['deaths'].diff()]
    daily_deaths[0] = df_county['cases'].values[0]

    df_county['cases'] = daily_load
    df_county['deaths'] = daily_deaths

    daily_load_3davg = daily_load
    daily_deaths_3davg = daily_deaths
    for i in range(1, len(daily_load)-2):
        daily_load_3davg[i] = (daily_load[i-1] + daily_load[i] + daily_load[i+1])/3
        daily_deaths_3davg[i] = (daily_deaths[i-1] + daily_deaths[i] + daily_deaths[i+1])/3
    daily_load_3davg[0] = (daily_load[0] + daily_load[1])/2
    daily_load_3davg[len(daily_load)-1] = (daily_load[-1] + daily_load[-2])/2

    daily_deaths_3davg[0] = (daily_deaths[0] + daily_deaths[1])/2
    daily_deaths_3davg[len(daily_deaths)-1] = (daily_deaths[-1] + daily_deaths[-2])/2

    daily_load_3davg = np.round(daily_load_3davg, 2)
    daily_deaths_3davg = np.round(daily_deaths_3davg, 2)
    df_county['cases_3d'] = daily_load_3davg
    df_county['deaths_3d'] = daily_deaths_3davg

    df_county[['date', 'cases', 'deaths', 'cases_3d', 'deaths_3d']].to_csv('../stats/time_series/caseload-{}.csv'.format(county) ,index=False)
    return('Outputed caseload for {}'.format(county))

