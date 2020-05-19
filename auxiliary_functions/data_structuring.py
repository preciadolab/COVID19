import numpy as np

def ratio_fun(x, y):
    if y == 0:
        return(10000)
    else:
        return(np.round(x/y, 2))

def merge_dicts(a, b, path=None):
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge_dicts(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass # same leaf value
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a

def one_rower(key, value, var_name):
    one_row = pd.DataFrame([{k1: v1[var_name] for k1, v1 in value['covid'].items()}])
    one_row[ value['type'] ] = key
    return(one_row)

def ts_from_var(geoid_metrics, var_name):
    list_of_pd = [one_rower(key, value, var_name) for key, value in geoid_metrics.items()]
    return(pd.concat(list_of_pd, sort = False))

def visit_parser(row, county_places):
    visit_dict = county_places.loc[row['safegraph_place_id']].to_dict()
    visit_dict['visits'] = row['visits']
    return(visit_dict)
    
def row_exploder(row):
    #return an nx3 array 
    dict_ = json.loads(row['visitor_home_cbgs'])
    place_id = [row['safegraph_place_id']] * len(dict_)
    cbg_count = np.vstack([ [key, value] for key, value in dict_.items()])
    return(np.hstack([np.array(place_id).reshape((len(place_id),1)),cbg_count]))