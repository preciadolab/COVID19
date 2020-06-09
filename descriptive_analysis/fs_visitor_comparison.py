"""
Functions to compare visit measurements against food distribution sites
"""
import numpy as np
import numpy.random as npr
import getopt, sys
import os
import pandas as pd
import pdb
import time
import math
import shapefile
from datetime import datetime
sys.path.insert(0, '..')
from auxiliary_functions.data_structuring import merge_dicts
from auxiliary_functions.plotting import make_histograms
from food_distribution_sites.findVisits import shp_to_dict

def main():
    #data for meals-served is split between two disjoint files
    meals_path = '../../food_sites/meals_served/'
    
    meal_sites = shp_to_dict(meals_path + 'COVID19_Free_Meal_Sites_PUBLICVIEW_ALLRECORDS')
    meal_sites = [pd.DataFrame.from_dict(v['features'], orient='index').transpose()
                  for k,v in meal_sites.items()]
    meal_sites = pd.concat(meal_sites, ignore_index=True)
    meal_sites = meal_sites[['site_name', 'units_serv',
                               'individ_se', 'report_dat', 'category_t']]
    meal_sites.columns = ['name', 'total_meals',
                           'visitor_count', 'date', 'category']
    meal_sites.date = [str(d.month).zfill(2)+'-'+str(d.day).zfill(2) for
                 d in meal_sites.date]
    
    #We still don't have meals served for Youth meal sites ?
    youth_sites = pd.read_csv(
        meals_path + 'Youth_Meal_Sites_manager_All.csv')
    youth_sites = youth_sites[['SCHOOL_NAME', 'Total Meals',
                               'Students Served', 'Report Date']]
    youth_sites.columns = ['name', 'total_meals',
                           'visitor_count', 'date']
    youth_sites['category'] = 'Youth Meal Site'
    youth_sites = youth_sites.loc[[isinstance(x, str) for
                                   x in youth_sites.date]]    
    new_dates = [datetime.strptime(date, "%m/%d/%Y, %I:%M %p") for
                 date in youth_sites.date] #change date to mm-dd 
    youth_sites.date = [str(d.month).zfill(2)+'-'+str(d.day).zfill(2)
                               for d in new_dates]
    
    all_sites_meals = pd.concat([meal_sites, youth_sites], ignore_index=True)

    #compare with computed visits from veraset
    visits_path = '../../stats/food_sites_time_series/'
    all_sites_veraset = pd.read_csv(visits_path+'food_sites_time_series.csv')

    meals_visits_df = all_sites_veraset.merge(
        all_sites_meals,
        how= 'inner',
        on = ['name', 'date'])

    #Histograms by category_t (indiv_served, subset)
    df = meals_visits_df.copy()
    df = df.loc[df.visitor_count > 0]
    for cat in  df.category.unique():
        x = df.loc[df.category == cat]['estimated_visits']
        y = df.loc[df.category == cat]['visitor_count']
        make_histograms(
            pd_series=[x,y],
            title='Comparison of visitor counts for {}'.format(cat),
            labels=['Veraset gh7', 'Individuals Served'],
            bins=20,
            range = [0, 300],
            file_name='../plots/food_sites_plots/hists_{}.png'.format(cat))
    #Scatterplots with correlation by category_t
    # for cat in  df.category.unique():
    #     x = df.loc[df.category == cat]['estimated_visits']
    #     y = df.loc[df.category == cat]['visitor_count']
    #     make_scatter(
    #         pd_series=[x,y],
    #         title='Comparison of visitor counts for {}'.format(cat),
    #         xlabel= 'Veraset gh7',
    #         ylabel = 'Individuals Served',
    #         file_name='../plots/food_sites_plots/hists_{}.png'.format(cat))

if __name__ == '__main__':
    main()