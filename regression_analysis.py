"""
Lasso regression to explain non-compliance of users at the census block level
"""
import numpy as np
import numpy.random as npr
import getopt, sys
import os
import pandas as pd
import gzip as gz
import time
import pdb
import matplotlib.pyplot
from sklearn.model_selection import cross_val_score
from sklearn.linear_model import LinearRegression

def scatter_plot(feature, target):
    plt.figure(figsize=(16,8))
    plt.scatter(
        data[feature],
        data[target],
        c='black')
    plt.xlabel(feature)
    plt.ylabel(target)
    plt.show

def main():
    #Load data frame    
    data = pd.read_csv('stats/joint_acs.csv')
    #Remove NA columns
    data = data.dropna(axis=1, how= 'any')
    #Remove id column
    data.drop('origin_census_block_group', axis=1, inplace=True)
    #Add some new variables?
    data['household_size'] = data['B00001_001']/data['B00002_001']
    #LinearRegression 
    Xs = data.drop('pct_home', axis=1)
    y  = data['pct_home'].values.reshape(-1,1)

    lin_reg = LinearRegression()
    MSEs = cross_val_score(lin_reg, Xs, y, scoring = 'neg_mean_squared_error', cv=5)
    mean_MSE = np.mean(MSEs)

    return 0

if __name__ == '__main__':
    main()