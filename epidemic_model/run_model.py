"""
Sets of functions that initialize and run an epidemiologic model for county 42101
based on contacts in places. 
"""
import pandas as pd
import pdb
import numpy as np 
import numpy.random as npr

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
    dct_states['S'] = pd.Series(cbg_pops)

    zeros = pd.Series(0, index= cbg_pops.keys())
    
    dct_states['E'] = zeros
    dct_states['I_s'] = zeros
    dct_states['I_a'] = zeros
    dct_states['R'] = zeros

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
    #compute effective infection rate


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
    cbg_pops = df_pops.B01003_001_Population.to_dict()

    states = initialize_states(cbg_pops, n=50)
    #load networks from before lockdown and after lockdown
    nets = {}
    nets['03-09'] = pd.read_csv(networks_path + 'contact_network_03-08.csv')
    nets['03-16'] = pd.read_csv(networks_path + 'contact_network_03-15.csv')

    pdb.set_trace()

if __name__ == '__main__':
    main()