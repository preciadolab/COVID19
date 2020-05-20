import pdb 
import numpy as np 
import json 
import re
import math

def main():
    county = "42101"
    with open('stats/time_series/metrics_{}.json'.format(county)) as fp:
        metrics = json.load(fp)

    #sum all the fields for low_device_count   
    low_dev = [v['low_device_count'] for k,v in metrics.items()]
    if np.sum([x for x in low_dev if x not in [0,1]]) == 0.0:
        print('low_device_count ok')

    #Check day by day
    cbgs = list(metrics.keys())

    for cbg in cbgs:
        #obtain all values of pct at home
        for date, attributes in metrics[cbg]['covid'].items():
            # if cbg == '421010158002' and date == 'April_04':
            #     pdb.set_trace()

            num_values = [attr_value for attr_name, attr_value in attributes.items()
              if re.search('_visited',attr_name) is None]
            visit_values = [attr_value for attr_name, attr_value in attributes.items()
              if re.search('_visited',attr_name) is not None]
            visit_values = [v for x in visit_values for k,v in x.items()]
            if np.sum([math.isnan(x) for x in num_values])>0:
                print("NaN value in num_values of CBG {} for date {}".format(cbg,date))
            if np.sum([math.isnan(x) for x in visit_values])>0:
                print("NaN value in visit_values of CBG {} for date {}".format(cbg,date))
    print("Finished")

if __name__ == '__main__':
    main()