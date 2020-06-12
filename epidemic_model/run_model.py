"""
Sets of functions that initialize and run an epidemiologic model for county 42101
based on contacts in places. 
"""
import pandas as pd
import pdb
import numpy as np 
import numpy.random as npr
from datetime import datetime, timedelta
import math

def get_day_interval(date_start, num_days):
    """
    date in format mm-dd
    """
    date_start = date_start + '-2020'

    date_start = datetime.strptime(date_start, "%m-%d-%Y")
    day_list = [ date_start+timedelta(days=t) for t in range(num_days)]
    return([str(x.month).zfill(2)+'-'+str(x.day).zfill(2) for x in day_list])

def initialize_states(cbg_pops, n=50):
    """
    Produce a vector of states as initial conditions for the model,
    for now, spreads number of cases evenly across census block groups.

    params:
        n int
        number of exposed individuals to begin simulation
        cbg_pops dict
        population of the different cbgs, key is census block group
    """
    k = 12
    dct_states = {}
    dct_states['N'] = cbg_pops #total population
    dct_states['S'] = cbg_pops

    zeros = pd.Series(0, index= cbg_pops.index)
    
    dct_states['E'] = zeros.copy()
    dct_states['I_s'] = zeros.copy()
    dct_states['I_a'] = zeros.copy()
    dct_states['R'] = zeros.copy()

    #We choose k CBGs to distribute the initial cases
    initial_cbgs = npr.choice(
        dct_states['E'].index,
        size= k,
        replace = False)
    #distribute infecteds amongst those
    sample = npr.multinomial(n, pvals = [1/k]*k)
    sample = np.array([min(x,y) for x,y in
                       zip(dct_states['S'][initial_cbgs], sample)])
    
    dct_states['S'][initial_cbgs] += - sample
    dct_states['E'][initial_cbgs] +=  sample
    return(dct_states)

def compute_rate(df, states, alpha, beta):
    cbg = df.origin_cbg.iloc[0]
    frac_s = states['S'][cbg]/states['N'][cbg]
    d_cbg = df.destination_cbg
    df.set_index('destination_cbg', inplace=True)

    c_a = sum((states['I_a'][d_cbg]/states['N'][d_cbg])*df.expected_contacts)
    c_s = sum((states['I_s'][d_cbg]/states['N'][d_cbg])*df.expected_contacts)
    
    df = pd.DataFrame(
        {'rate':[frac_s*beta*(c_s + alpha*c_a)]})
    return(df)

def update_states(states, alpha, beta, gamma, kappa, rho, ctc_network):
    """
    Takes the states of the model in a given day and simulates the 
    compartmental transitions. The only non-static parameter, is the 
    contact network. 
    params:
        states: dict
            current compartment counts for every subpopulation.
        alpha: float
            discount factor for contact with an asymptomatic infective.
        beta: float
            rate of infection the probability of a susceptible becoming 
            infected from exposure to one infective/sq_foot. 
        gamma: float
            rate at which infectives recover.
        kappa: float
            rate at which an exposed becomes infective.
        rho: float
            probability of exposed becoming symptomatic.
        ctc_network: pandas DataFrame
            network estimating the number of contacts with other CBGs
            in a given day. 
    """
    #compute effective infection rates
    #beta * Si/Nj * Ij/Nj * contacts + alpha*beta Si/Nj * Ij/Nj * contacts
    effective_rates = ctc_network.groupby('origin_cbg').apply(
        compute_rate,
        states=states,
        alpha=alpha,
        beta=beta).reset_index(level=1, drop=True)

    print("max effective rate: {}".format(max(effective_rates.rate)))
    if math.isinf(max(effective_rates.rate)):
        pdb.set_trace()
    S_to_E = pd.Series(
        npr.poisson(effective_rates.rate),
        index=effective_rates.index)
    S_to_E = pd.DataFrame([S_to_E, states['S']]).min() #prevent negatives

    E_to_I = pd.Series(
        npr.poisson(kappa*states['E']),
        index=states['E'].index)
    E_to_I = pd.DataFrame([E_to_I, states['E']]).min() #prevent negatives

    Is_to_R = pd.Series(
        npr.poisson(gamma*states['I_s']),
        index=states['I_s'].index)
    Is_to_R = pd.DataFrame([Is_to_R, states['I_s']]).min()

    Ia_to_R = pd.Series(
        npr.poisson(gamma*states['I_a']),
        index=states['I_a'].index)
    Ia_to_R = pd.DataFrame([Ia_to_R, states['I_a']]).min()

    #update states
    states['S'] += -S_to_E
    states['E'] += S_to_E - E_to_I 
    states['I_s'] += (rho*E_to_I).round() - Is_to_R
    states['I_a'] += E_to_I - (rho*E_to_I).round() - Ia_to_R #sends many to 'I_a'
    states['R'] += Is_to_R + Ia_to_R
    return(states)

def main():
    #read file with populations per cbg
    networks_path = '../../stats/time_series/networks/' 
    acs_path = '../../acs_vars/'
    df_pops = pd.read_csv(
        acs_path + 'aggregated_acs_cbg_subset.csv',
        dtype={'GEOID':str})
    df_pops.set_index('GEOID', inplace=True, drop=True)
    mask = [idx[:5] == '42101' for idx in df_pops.index]
    df_pops = df_pops.loc[mask, ['B01003_001_Population']]
    df_pops = df_pops.loc[df_pops.B01003_001_Population > 0]
    cbg_pops = df_pops.B01003_001_Population
    cbg_pops.rename_axis('origin_cbg', inplace=True)


    #load networks from before lockdown and after lockdown
    nets = {}
    nets['03-09'] = pd.read_csv(
        networks_path + 'contact_network_03-09.csv',
        dtype= {'destination_cbg':str, 'origin_cbg':str})
    nets['03-16'] = pd.read_csv(
        networks_path + 'contact_network_03-16.csv',
        dtype= {'destination_cbg':str, 'origin_cbg':str})

    #Remove cbgs not in net (unlikely, but for safety)
    #Filter out foreign CBGs
    print("--Using contact network {}".format('03-09'))
    net = nets['03-09']
    mask = [x in cbg_pops.keys() and y in cbg_pops.keys() 
            for x, y  in zip(net.origin_cbg, net.destination_cbg)]
    net = net.loc[mask]
    #Keep only 400000 edges from non diagonal
    keep = 200000
    s = keep/len(net)
    mask1 = net.expected_contacts > np.quantile(net.expected_contacts, 1-s)
    mask2 = (net.origin_cbg == net.destination_cbg)
    net = net.loc[[x or y for x,y in zip(mask1, mask2)]].copy()
    #Make expected contacts daily
    net['expected_contacts'] = net['expected_contacts']/7
    cbg_pops = cbg_pops[net.origin_cbg.unique()]
    #Begin modeling
    states = initialize_states(cbg_pops, n=50)
    init_date = '03-01'
    date_interval = get_day_interval(init_date, 40)
    I_df = states['R'].to_frame(0)
    I_df.columns = [init_date]

    for date in date_interval[1:]:
        print(date)
        if date == '03-16':
            print("--Changing contact network")
            net = nets['03-16']

            mask = [x in cbg_pops.keys() and y in cbg_pops.keys() 
                    for x, y  in zip(net.origin_cbg, net.destination_cbg)]
            net = net.loc[mask]
            #Keep only 400000 edges from non diagonal
            keep = 200000
            s = keep/len(net)
            mask1 = net.expected_contacts > np.quantile(net.expected_contacts, 1-s)
            mask2 = (net.origin_cbg == net.destination_cbg)
            net = net.loc[[x or y for x,y in zip(mask1, mask2)]].copy()

        states = update_states(
            states=states,
            alpha=0.55,
            beta=0.08,
            gamma=0.45,
            kappa=0.3,
            rho=0.14,
            ctc_network=net)
        #apend states['I'] to dataframe
        I_df[date] = states['R']
    I_df.to_csv('../../stats/trajectories/example.csv')


if __name__ == '__main__':
    main()