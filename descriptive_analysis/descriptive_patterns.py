"""
Function to analyze patterns in Philadelphia
"""
import numpy as np
import numpy.random as npr
import getopt, sys
import os
import pandas as pd
import gzip as gz
import subprocess
from array import array
import time
import io
import pdb
from textwrap import wrap
import matplotlib.pyplot as plt
from matplotlib.pyplot import figure
import matplotlib
matplotlib.use('Agg')
def make_boxplot(pd_series, title, file_name = 'box.png'):
    width = 0.6
    x = np.arange(len(pd_series))
    plt.bar(x, pd_series, width)
    plt.title(title)
    #Make sure xticks are readable and fall inside
    plt.xticks(rotation=0, wrap=True, fontsize = 'small')
    plt.tick_params(axis= 'x', pad=0)
    plt.subplots_adjust(left=0.07, right=0.95, bottom=0.15)
    labels = ['\n'.join(wrap(l,14)) for l in pd_series.index]
    plt.xticks(x, labels)
    plt.ylabel('Frequency')

    fig = plt.gcf()
    fig.set_size_inches(12, 5)
    fig.savefig(file_name, dpi=150)
    fig.clf()

def make_histograms(pd_series, title, labels, file_name):
    bins = 100

    plt.hist(pd_series[0], bins, alpha=0.5, label=labels[0], range=(30, 750))
    plt.hist(pd_series[1], bins, alpha=0.5, label=labels[1], range=(30, 750))
    plt.legend(loc='upper right')

    plt.title(title)
    plt.xlim(30, 750)


    fig = plt.gcf()
    fig.set_size_inches(10, 5)
    fig.savefig(file_name, dpi=150)
    fig.clf()

def make_histogram(pd_series, title, file_name, range = None):
    plt.hist(pd_series, bins = 50, range = range)
    plt.title(title)
    fig = plt.gcf()
    fig.set_size_inches(10, 5)
    fig.savefig(file_name, dpi=300)
    fig.clf()

def avg_top_10(json_int):
    x = json_int.replace('[','')
    x = x.replace(']','')

    counts = [int(y) for y in x.split(',')]
    score = np.mean(sorted(counts)[-10:])
    return(score)

def array_cbg_visits(json_dict):
    x = json_dict.replace('{','')
    x = x.replace('}','')
    x = x.replace('"','')
    if len(x) == 0:
        return(None)
    entries = x.split(',')
    return(np.vstack([[y.split(':')[0], int(y.split(':')[1])] for y in entries]))

def main():
    #bar plot of place categories
    core_places = pd.read_csv( '../core_places/phila_places.csv', index_col='safegraph_place_id')
    core_index =  core_places.index
    core_places['sub_category'].value_counts().to_csv('subcategories.csv')

    top_places = core_places['top_category'].value_counts()[:10]

    make_boxplot(top_places,
                 'Places in Philadelphia\n (Top 10 place types)', #more than 10 devices
                 file_name = 'places_philly.png')

    #Histogram of raw_visit_count before
    phl_patterns = pd.read_csv('../weekly_patterns/main-file-phl/2020-03-01-weekly-patterns.csv.gz')
    raw_counts_m = phl_patterns['raw_visit_counts']

    #Histogram of raw_visit_count after
    phl_patterns = pd.read_csv('../weekly_patterns/main-file-phl/2020-04-12-weekly-patterns.csv.gz')
    raw_counts_a = phl_patterns['raw_visit_counts']

    make_histograms([raw_counts_m, raw_counts_a],
                   'weekly visit count (devices >10)',
                   ['2020-03-01', '2020-04-12'],
                   file_name = 'hists_compared.png')

    #Table with relevant variables
    phl_patterns = pd.read_csv('../weekly_patterns/main-file-phl/2020-04-12-weekly-patterns.csv.gz', index_col='safegraph_place_id')
    #raw_visit_counts 
    #visit_counts*median_dwell_time
    phl_patterns['weighted_visit_counts'] = phl_patterns['raw_visit_counts']*phl_patterns['median_dwell']
    #distance_from_home 
    #count_home_cbgs : (parse JSON? https://www.kaggle.com/safegraph/manipulating-exploding-json-columns?scriptVersionId=14951442)
    phl_patterns['count_home_cbgs'] = [len(x.split(',')) for x in phl_patterns['visitor_home_cbgs']]
    #visits_top_10_hours
    phl_patterns['visits_top_10_hours'] = [avg_top_10(x) for x in phl_patterns['visits_by_each_hour']]
    #latitude
    phl_patterns['latitude'] = (core_places.loc[phl_patterns.index])['latitude']
    phl_patterns['longitude'] = (core_places.loc[phl_patterns.index])['longitude']

    metrics = phl_patterns[['latitude', 'longitude', 'location_name', 'street_address',
                           'raw_visit_counts', 'distance_from_home', 'weighted_visit_counts',
                           'count_home_cbgs', 'visits_top_10_hours']].copy()

    metrics.to_csv('stats/phl_metrics.csv', index=True)
    #Plot histograms for two new variables
    make_histogram([x for x in phl_patterns['count_home_cbgs'] if x>2],
     'Distribution of count of \n different CBGs of origin. \n (> 2 CBGs)',
     'count_home.png',
     (3,120))

    make_histogram([x for x in phl_patterns['visits_top_10_hours'] if x>2],
     'Distribution of avg. visits \n during top 10 hours.\n (> 2 devices)',
     'top_10.png',
      (3, 20))
    #Obtain rankings (safegraphID of all the top metrics)
    metrics.sort_values(["raw_visit_counts"], axis=0, ascending=False, inplace=True)

    raw_visit_counts = metrics['raw_visit_counts'][:50].index

    metrics.sort_values(["weighted_visit_counts"], axis=0, 
                 ascending=False, inplace=True)
    weighted_visit_counts = metrics['weighted_visit_counts'][:50].index

    metrics.sort_values(["distance_from_home"], axis=0, 
                 ascending=False, inplace=True)
    distance_from_home = metrics['distance_from_home'][:50].index

    metrics.sort_values(["count_home_cbgs"], axis=0, 
                 ascending=False, inplace=True)
    count_home_cbgs = metrics['count_home_cbgs'][:50].index    

    metrics.sort_values(["visits_top_10_hours"], axis=0, 
                 ascending=False, inplace=True)
    visits_top_10_hours = metrics['visits_top_10_hours'][:50].index   

    #Put together table with rankings
    rankings = pd.DataFrame(columns = ['raw_visit_counts',
                                 'weighted_visit_counts',
                                 'count_home_cbgs',
                                 'distance_from_home',
                                 'visits_top_10_hours'])

    #Append names and coordinates for each variable
    #turn safegraph ID into index (key) of metrics

    #generate variables
    rankings['raw_visit_counts'] = metrics.loc[raw_visit_counts, 'location_name'].reset_index(drop=True)
    rankings['weighted_visit_counts'] = metrics.loc[weighted_visit_counts.values, 'location_name'].reset_index(drop=True)
    rankings['count_home_cbgs'] = metrics.loc[count_home_cbgs.values, 'location_name'].reset_index(drop=True)
    rankings['distance_from_home'] = metrics.loc[distance_from_home.values, 'location_name'].reset_index(drop=True)
    rankings['visits_top_10_hours'] = metrics.loc[visits_top_10_hours.values, 'location_name'].reset_index(drop=True) 

    #sort rankings by column name
    rankings = rankings[sorted(rankings.columns)]
    rankings.to_csv('stats/phl_rankings.csv', index=False)
    #print some of the repeated values
    #concatenate every name
    location_list = []
    for (columnName, columnData) in rankings.iteritems():
        location_list.append(np.vstack(columnData.values))
    
    location_list = np.vstack(location_list)
    location_counts = pd.DataFrame(location_list)[0].value_counts()
    print(location_counts[:10].index)
    
if __name__ == '__main__':
    main()
    