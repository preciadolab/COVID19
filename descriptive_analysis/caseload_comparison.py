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
from datetime import datetime
sys.path.insert(0, '..')
from auxiliary_functions.plotting import make_lines

def main():
    #load simulated trajectory
    simulated = pd.read_csv(
        '../../stats/trajectories/example.csv',
        dtype={'origin_cbg':str})
    #aggregate at the city level (simple col sums)
    city_trajectory = simulated.sum(axis=0)
    #plot a single line 

    pdb.set_trace()
    make_lines(
        x_axis=[x for x in range(len(city_trajectory))],
        y_axes=[city_trajectory, city_trajectory],
        title= 'Simulated trajectory [REMOVED]\n beta=0.28, initial_cases=50',
        labels = ['simulation', 'real'],
        file_name='../plots/simulated_trajectory.png')

if __name__ == '__main__':
    main()