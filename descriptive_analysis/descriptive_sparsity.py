"""
Function to obtain descriptive statistics of the sparsity of measurements
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


sys.path.insert(0, '../auxiliary_functions/')
import plotting as ppp
import json_code as jjj

def main():
    argument_list = sys.argv[1:]
    short_options = "m:"
    long_options = ["month"]
    months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dic']
    try:
        arguments, values = getopt.getopt(argument_list, short_options, long_options)
    except getopt.error as err:
        # Output error, and return with an error code
        print (str(err))
        sys.exit(2)

    for current_argument, current_value in arguments:
        if current_argument in ("-m", "--month"):
            print ("Subsetting for month: " + current_value)
            #REJECT IF NOT IN STANDARD FORMAT Mmm
            if current_value not in months:
                print("Error: Month must be in Mmm format")
                sys.exit(2)
            month = current_value

    #List files in folder corresponding to month
    file_path = '../../safegraph_social_dist_data/'+ str(months.index(month) + 1).zfill(2) +'/'
    file_list = os.listdir(file_path)
    file_list = sorted([day for day in file_list])
    #Iterate through days and obtain metrics
    pct_active = []
    pct_in_cbg = []
    for day in file_list:
        file_name = os.listdir(file_path + day + '/')[0]
        data = pd.read_csv( file_path + day + '/' + file_name,
                            dtype = {'origin_census_block_group':str})
        data.set_index('origin_census_block_group', inplace=True)

        pct_active = pct_active + [np.round(np.sum(data['device_count'] / np.sum(data['candidate_device_count'])), 2)]
        pct_in_cbg = pct_in_cbg + [np.round(np.sum(data['within_cbg_count'] / np.sum(data['candidate_device_count'])), 2)]
    #Plot two lines with percentage of relevant phones
    ppp.make_lines(x_axis= file_list,
                   y_axes = [pct_active, pct_in_cbg],
                   title = 'Percentage of active devices',
                   labels = ['Active', 'Active in cbg'],
                   xlabel = month,
                   ylabel = '%',
                   ylim = [0.20, 0.45],
                   file_name= '../plots/lines.png')

    #Histograms for day 15
    day = '15'
    file_name = os.listdir(file_path + day + '/')[0]
    data = pd.read_csv( file_path + day + '/' + file_name,
                        dtype = {'origin_census_block_group':str})
    data.set_index('origin_census_block_group', inplace=True)

    pct_active = np.round(data['device_count']/data['candidate_device_count'], 2)
    pct_in_cbg = np.round(data['within_cbg_count']/data['candidate_device_count'], 2)

    ppp.make_histograms(pd_series = [pct_active, pct_in_cbg],
                        title = 'Percentage of active devices in CBG',
                        labels = ['Active', 'Active in cbg'],
                        file_name = '../plots/actives_hist.png',
                        bins = 27,
                        xlabel = month + ' ' + day,
                        ylabel = 'Frequency')

    #Avg time with confidence intervals
    total_hours_person = []
    for day in file_list:
        file_name = os.listdir(file_path + day + '/')[0]
        data = pd.read_csv( file_path + day + '/' + file_name,
                            dtype = {'origin_census_block_group':str})
        data.set_index('origin_census_block_group', inplace=True)

        #Time away
        times_home = [jjj.parse_total_hours_at_home(string) for string in data['bucketed_home_dwell_time']]
        times_home = np.vstack(times_home)
        #Time away
        times_away = [jjj.parse_total_hours_away(string) for string in data['bucketed_away_from_home_time']]
        times_away = np.vstack(times_away)

        agg_times_home = np.round(np.sum(times_home, axis=0)/(60*np.sum(data['device_count'])),2)
        agg_times_away = np.round(np.sum(times_away, axis=0)/(60*np.sum(data['device_count'])),2)

        total_hours_person = total_hours_person + [agg_times_home + agg_times_away]
    
    total_hours_person = np.vstack(total_hours_person)

    ppp.make_line_ci(x_axis= file_list,
                     y_axes = [total_hours_person[:,0], total_hours_person[:,1], total_hours_person[:,2]],
                     title = 'Hours measured per active device',
                     xlabel = month,
                     ylabel = 'Hours',
                     ylim = [0, 24],
                     file_name= '../plots/line_ci.png')

    #Histogram of average times for Apr 15
    day = '15'
    file_name = os.listdir(file_path + day + '/')[0]
    data = pd.read_csv( file_path + day + '/' + file_name,
                        dtype = {'origin_census_block_group':str})
    data.set_index('origin_census_block_group', inplace=True)

    #Time away
    times_home = [jjj.parse_total_hours_at_home(string) for string in data['bucketed_home_dwell_time']]
    times_home = np.vstack(times_home)[:,1]
    #Time away
    times_away = [jjj.parse_total_hours_away(string) for string in data['bucketed_away_from_home_time']]
    times_away = np.vstack(times_away)[:,1]

    #Total time per cbg distribution
    time_dist = (times_home + times_away)/(60*data['device_count'])
    ppp.make_histogram(pd_series = time_dist,
                       title = 'Distribution of measured time\n (hours/user) over CBGs',
                       xlabel = month + ' ' + day,
                       range= [5, 22],
                       bins = 17,
                       ylabel = 'Number of CBGs',
                       file_name = '../plots/times_hist.png')

if __name__ == '__main__':
    main()