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
import shapefile


def main():
    #data for meals-served is split between two disjoint files
    meals_path = '../../food/Meals_distributed/'
    meal_sites =pd.read_csv(
        meals_path +
        'COVID19_Free_Meal_Sites_PUBLICVIEW_ALLRECORDS.csv')
    meal_sites = meal_sites[['Site Name', 'GlobalID',
                             'individ_served','Report Date']]

    youth_sites = pd.read_csv(
        meals_path + 'Youth_Meal_Sites_manager_All.csv')
    youth_sites = youth_sites[['SCHOOL_NAME', 'GlobalID',
                               'Students Served', 'Report Date']]

    meal_sites.columns  = ['site_name', 'global_id',
                           'visitor_count', 'report_date']
    youth_sites.columns = ['site_name', 'global_id',
                           'visitor_count', 'report_date']

    all_sites_meals = pd.concat([meal_sites, youth_sites])

    visits_path = '../../stats/food_sites_time_series/'
    all_sites_veraset = pd.read_csv(visits_path+'food_sites_time_series.csv')

    mask = [x in all_sites_meals.site_name.unique() for x in all_sites_veraset.name.unique()]
    pdb.set_trace()
if __name__ == '__main__':
    main()