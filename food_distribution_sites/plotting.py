# -*- coding: utf-8 -*-
"""
Created on Tue May 19 12:09:02 2020

@author: Victoria Fethke
"""

# define function
subset = []
geo = []
for item in siteLocationTimes.geo_hash.tolist():
    if visitorDict[item]['visits'] != 0:
        subset.append(visitorDict[item]['visits'])
        geo.append(item)
plt.figure(figsize=(15,10))
p1 = plt.bar(geo, subset, 0.5)
plt.ylabel('Number of Visits')
plt.xlabel('Food Distribution Sites')
plt.title('Visits to food distribution site by day')
plt.xticks(np.arange(len(geo)), (geo), fontsize=14)