"""
Auxiliary functions for plotting 
"""
import getopt, sys
import os
import pandas as pd
import gzip as gz
import subprocess
from array import array
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

def make_histograms(pd_series, title, labels, file_name, **kwargs):
    # Receives a list of two pandas series and produces two overlaid histograms
    bins = None
    if 'bins' in kwargs.keys():
        bins = kwargs['bins']

    plt.hist(pd_series[0], alpha=0.5, label=labels[0], bins = bins)
    plt.hist(pd_series[1], alpha=0.5, label=labels[1], bins = bins)
    plt.legend(loc='upper right')
    plt.title(title, fontsize=18)

    for key, value in kwargs.items():
        if key == 'xlabel':
            plt.xlabel(value, fontsize=16)
        if key == 'ylabel':
            plt.ylabel(value, fontsize=16)
        if key == 'ylim':
            plt.ylim(value)

    fig = plt.gcf()
    fig.set_size_inches(10, 5)
    fig.savefig(file_name, dpi=300)
    fig.clf()

def make_histogram(pd_series, title, file_name, **kwargs):
    bins = None
    range_ = None
    if 'bins' in kwargs.keys():
        bins = kwargs['bins']
    if 'range' in kwargs.keys():
        range_ = kwargs['range']

    plt.hist(pd_series, range = range_, bins = bins, color = 'skyblue', alpha = 0.9)
    plt.title(title)

    for key, value in kwargs.items():
        if key == 'xlabel':
            plt.xlabel(value, fontsize=16)
        if key == 'ylabel':
            plt.ylabel(value, fontsize=16)
        if key == 'ylim':
            plt.ylim(value)

    fig = plt.gcf()
    fig.set_size_inches(10, 5)
    fig.savefig(file_name, dpi=300)
    fig.clf()

def make_lines(x_axis, y_axes, title, labels, file_name, **kwargs):
    # Receives a list of two pandas series and produces two overlaid histograms
    plt.plot( x_axis, y_axes[0], marker='', color='olive', linewidth=2, label = labels[0])
    plt.plot( x_axis, y_axes[1], marker='', color='skyblue', linewidth=2, label = labels[1])
    plt.legend(loc='upper right')
    plt.title(title, fontsize=18)
    plt.grid()
    for key, value in kwargs.items():
        if key == 'xlabel':
            plt.xlabel(value, fontsize=16)
        if key == 'ylabel':
            plt.ylabel(value, fontsize=16)
        if key == 'ylim':
            plt.ylim(value)

    fig = plt.gcf()
    fig.set_size_inches(10, 5)
    fig.savefig(file_name, dpi=150)
    fig.clf()

def make_line_ci(x_axis, y_axes, title, file_name, **kwargs):
    # Receives a list of two pandas series and produces two overlaid histograms
    plt.plot( x_axis, y_axes[1], marker='', color='skyblue', linewidth=3)
    plt.fill_between(x_axis, y_axes[0], y_axes[2], color='b', alpha=.1)

    plt.title(title, fontsize=18)
    plt.grid()
    for key, value in kwargs.items():
        if key == 'xlabel':
            plt.xlabel(value, fontsize=16)
        if key == 'ylabel':
            plt.ylabel(value, fontsize=16)
        if key == 'ylim':
            plt.ylim(value)

    fig = plt.gcf()
    fig.set_size_inches(10, 5)
    fig.savefig(file_name, dpi=150)
    fig.clf()