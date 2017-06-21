import numpy as np
import pandas as pd


def migrating_pop(population, migration_rates):

    return population.join(migration_rates, how='left', sort=False)


def non_migrating_pop(non_mig_pop):

    non_mig_pop['mig_Dout'] = (non_mig_pop['persons'] * non_mig_pop['DOUT']).round()
    non_mig_pop['mig_Fout'] = (non_mig_pop['persons'] * non_mig_pop['FOUT']).round()
    non_mig_pop['non_mig_pop'] = (non_mig_pop['persons'] - non_mig_pop['mig_Dout'] - non_mig_pop['mig_Fout']).round()
    return non_mig_pop


def rates_for_yr(rates_all_years, sim_year):
    """
    Filter specific rates for a given year

    Parameters
    ----------
    rates_all_years : pandas DataFrame
        rates, to be filtered by year
    sim_year : int
        year being simulated

    Returns
    -------
    pop_w_rates : pandas DataFrame
        rates for a given year

    """
    rates_yr = rates_all_years[rates_all_years['yr'] == sim_year]
    return rates_yr
