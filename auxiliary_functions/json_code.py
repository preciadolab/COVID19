import json
import math
import pandas as pd

def parse_time_away_json(x):
    if not isinstance(x, str):
        return(0)
    y = json.loads(x)
    if '<60' in y.keys():
        return(y['<60'])
    else:
        return(0)

def parse_bucketed_distance_json(x):
    if not isinstance(x, str):
        return(0)
    y = json.loads(x)
    if '1-1000' in y.keys():
        return(y['1-1000'])
    else:
        return(0)

def parse_within_cbg(x, own_cbg):
	if not isinstance(x, str):
		return(0)
	y = json.loads(x)
	if own_cbg in y.keys():
		return(int(y[own_cbg]))
	return(0)

def parse_total_hours_at_home(x):
    if not isinstance(x, str):
        return(0)
    y = json.loads(x)

    [low, avg, upp] = [0, 0, 0]
    for key, value in y.items():
        if key == '<60':
            low = low + 0*value
            avg = avg + 30*value
            upp = upp + 60*value
        elif key == '>1080':
            low = low + 1080*value
            avg = avg + 1260*value
            upp = upp + 1440*value
        else:
            times = key.split("-")

            low = low + int(times[0])*value
            avg = avg + (int(times[0])+int(times[1]))*value/2
            upp = upp + int(times[1])*value
    
    return( [low, avg, upp] )

def parse_total_hours_away(x):
    if not isinstance(x, str):
        return(0)
    y = json.loads(x)

    [low, avg, upp] = [0, 0, 0]
    for key, value in y.items():
        if key == '<20':
            low = low + 0*value
            avg = avg + 10*value
            upp = upp + 20*value
        else:
            times = key.split("-")
            low = low + int(times[0])*value
            avg = avg + (int(times[0])+int(times[1]))*value/2
            upp = upp + int(times[1])*value
    
    return( [low, avg, upp] )